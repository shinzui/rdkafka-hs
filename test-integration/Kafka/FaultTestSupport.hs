-- | Observed thread-stage barriers, with bounded waits. A caller signals
-- arrival at the stage and waits until the controlling test releases it.
module Kafka.FaultTestSupport (Barrier, newBarrier, reachBarrier, awaitBarrier, releaseBarrier) where

import Control.Concurrent.MVar (MVar, newEmptyMVar, putMVar, readMVar)
import System.Timeout (timeout)
import Test.Tasty.HUnit (assertFailure)

data Barrier = Barrier (MVar ()) (MVar ())

newBarrier :: IO Barrier
newBarrier = Barrier <$> newEmptyMVar <*> newEmptyMVar

reachBarrier :: Barrier -> IO ()
reachBarrier (Barrier reached resume) = putMVar reached () >> bounded (readMVar resume)

awaitBarrier :: Barrier -> IO ()
awaitBarrier (Barrier reached _) = bounded (readMVar reached)

releaseBarrier :: Barrier -> IO ()
releaseBarrier (Barrier _ resume) = putMVar resume ()

bounded :: IO a -> IO a
bounded action = timeout 5000000 action >>= maybe (assertFailure "thread stage barrier timed out") pure
