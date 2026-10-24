-- | Weak-reference controls for callback ownership tests. Keeping the Weak
-- value does not retain its key. Callers must create captured values in a
-- separate NOINLINE function so their own stack cannot retain the key.
module Kafka.CallbackTestSupport (awaitCollected, assertRetained, runChildProbe) where

import Control.Concurrent (threadDelay)
import System.Directory (createDirectoryIfMissing)
import System.Environment (getEnvironment, getExecutablePath, lookupEnv)
import System.Exit (ExitCode (ExitSuccess))
import System.Mem (performMajorGC)
import System.Mem.Weak (Weak, deRefWeak)
import System.Process (CreateProcess (env), proc, readCreateProcessWithExitCode)
import System.Random (randomIO)
import System.Timeout (timeout)
import Test.Tasty.HUnit (assertFailure)

-- Isolate foreign-boundary probes and runtime exception handlers. Keep child
-- coverage separate and let the collector merge each successful child.
runChildProbe :: String -> [String] -> IO ()
runChildProbe prefix arguments = do
  executable <- getExecutablePath
  environment <- getEnvironment
  directory <- maybe "dist-callback-probes" id <$> lookupEnv "RDKAFKA_TEST_CHILD_HPC_DIR"
  createDirectoryIfMissing True directory
  nonce <- randomIO :: IO Word
  let output = directory <> "/" <> prefix <> "-" <> show nonce <> ".tix"
      process =
        (proc executable (arguments <> ["+RTS", "--read-tix-file=no", "-RTS"]))
          { env = Just (("HPCTIXFILE", output) : filter ((/= "HPCTIXFILE") . fst) environment)
          }
  result <- timeout 15000000 (readCreateProcessWithExitCode process "")
  case result of
    Just (ExitSuccess, _, _) -> pure ()
    other -> assertFailure ("native callback child failed: " <> show other)

awaitCollected :: Weak a -> IO ()
awaitCollected weak = go (100 :: Int)
  where
    go 0 = assertFailure "callback capture remained reachable after release"
    go retries = do
      performMajorGC
      dead <- isDead weak
      if dead then pure () else threadDelay 10000 >> go (retries - 1)

assertRetained :: Weak a -> IO ()
assertRetained weak = do
  performMajorGC
  dead <- isDead weak
  if dead then assertFailure "callback capture collected while still needed" else pure ()

{-# NOINLINE isDead #-}
isDead :: Weak a -> IO Bool
isDead weak =
  deRefWeak weak >>= \case
    Nothing -> pure True
    Just _ -> pure False
