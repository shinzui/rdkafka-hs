-- | Observed thread-stage barriers, with bounded waits. A caller signals
-- arrival at the stage and waits until the controlling test releases it.
module Kafka.FaultTestSupport (Barrier, newBarrier, reachBarrier, awaitBarrier, releaseBarrier, withUnresponsiveBroker) where

import Control.Concurrent (forkIO, killThread)
import Control.Concurrent.MVar (MVar, modifyMVar_, newEmptyMVar, newMVar, putMVar, readMVar, tryPutMVar)
import Control.Exception (bracket, mask_)
import Control.Monad (forever, void)
import Data.Text qualified as Text
import Kafka.Types (BrokerAddress (..))
import Network.Socket qualified as N
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

-- | A run-owned loopback endpoint that accepts connections but never replies
-- to Kafka requests. The listener and every accepted socket close on exit.
withUnresponsiveBroker :: (BrokerAddress -> IO () -> IO a) -> IO a
withUnresponsiveBroker action =
  bracket (N.socket N.AF_INET N.Stream N.defaultProtocol) N.close $ \listener -> do
    N.bind listener (N.SockAddrInet 0 (N.tupleToHostAddress (127, 0, 0, 1)))
    N.listen listener 16
    port <- N.socketPort listener
    connections <- newMVar []
    connected <- newEmptyMVar
    let acceptLoop = forever $ mask_ $ do
          (socket, _) <- N.accept listener
          modifyMVar_ connections (pure . (socket :))
          void (tryPutMVar connected ())
        stop thread = do
          killThread thread
          modifyMVar_ connections (\sockets -> mapM_ N.close sockets >> pure [])
    bracket (forkIO acceptLoop) stop $ \_ ->
      action (BrokerAddress ("127.0.0.1:" <> Text.pack (show port))) (bounded (readMVar connected))
