-- Include unlinked library modules as explicit zero-count coverage. The MIX
-- file supplies the exact instrumentation hash and tick count; never invent
-- counts or mark an unlinked module as inapplicable.
module Main (main) where

import System.Environment (getArgs)
import Trace.Hpc.Mix (Mix (..))
import Trace.Hpc.Tix (Tix (..), TixModule (..), writeTix)

main :: IO ()
main = do
  args <- getArgs
  case args of
    output : inputs -> mapM zero (pairs inputs) >>= writeTix output . Tix
    _ -> fail "usage: coverage-zero.hs OUTPUT NAME MIX [NAME MIX ...]"
  where
    pairs [] = []
    pairs (name : path : rest) = (name, path) : pairs rest
    pairs _ = error "coverage-zero: unmatched name/path"
    zero (name, path) = do
      Mix _ _ hash _ entries <- read <$> readFile path
      pure (TixModule name hash (length entries) (replicate (length entries) 0))
