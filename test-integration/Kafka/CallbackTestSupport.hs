-- | Weak-reference controls for callback ownership tests. Keeping the Weak
-- value does not retain its key. Callers must create captured values in a
-- separate NOINLINE function so their own stack cannot retain the key.
module Kafka.CallbackTestSupport (awaitCollected, assertRetained) where

import Control.Concurrent (threadDelay)
import System.Mem (performMajorGC)
import System.Mem.Weak (Weak, deRefWeak)
import Test.Tasty.HUnit (assertFailure)

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
