-- | 'destroyConsumer' tears the client down synchronously (improvement
-- request IR-2, AC-3), in both callback poll modes, and the close/destroy
-- transitions are idempotent.
module Kafka.Integration.DestroyTest
  ( tests,
  )
where

import Control.Concurrent (forkIO, killThread, threadDelay)
import Control.Concurrent.MVar (MVar, modifyMVar_, newEmptyMVar, newMVar, putMVar, readMVar, tryPutMVar)
import Control.Exception (SomeException, bracket, finally, try)
import Control.Monad (forM_, forever, replicateM, void)
import Data.ByteString qualified as BS
import Data.Text qualified as Text
import GHC.Clock (getMonotonicTime)
import Kafka.Consumer
import Kafka.TestEnv
import Network.Socket qualified as N
import Network.Socket.ByteString qualified as NB
import System.Timeout (timeout)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (Assertion, assertBool, assertFailure, testCase, testCaseInfo, (@?=))

tests :: TestEnv -> TestTree
tests env =
  testGroup
    "destroy"
    [ testGroup label (modeCases env label mode)
    | (label, mode) <- [("async", CallbackPollModeAsync), ("sync", CallbackPollModeSync)]
    ]

modeCases :: TestEnv -> String -> CallbackPollMode -> [TestTree]
modeCases env label mode =
  [ testCaseInfo "closes broker connections within one second" $
      withSilentBroker $ \broker -> do
        consumer <- silentConsumer env mode (label <> "-sockets") broker
        connections <- awaitConnections broker
        start <- getMonotonicTime
        destroyConsumer consumer
        returned <- getMonotonicTime
        allClosedBy broker connections (start + 1)
        closed <- getMonotonicTime
        pure $
          "destroyConsumer returned after "
            <> showMillis (returned - start)
            <> "; all "
            <> show (length connections)
            <> " connection(s) closed after "
            <> showMillis (closed - start),
    testCase "is safe to repeat" $
      withSilentBroker $ \broker -> do
        consumer <- silentConsumer env mode (label <> "-repeat") broker
        connections <- awaitConnections broker
        start <- getMonotonicTime
        destroyConsumer consumer
        destroyConsumer consumer
        allClosedBy broker connections (start + 1)
        closeConsumer consumer >>= (@?= Nothing),
    testCase "concurrent destroys both return after cleanup" $
      withSilentBroker $ \broker -> do
        consumer <- silentConsumer env mode (label <> "-concurrent") broker
        connections <- awaitConnections broker
        start <- getMonotonicTime
        done <- replicateM 2 newEmptyMVar
        forM_ done $ \var -> forkIO (try (destroyConsumer consumer) >>= putMVar var)
        forM_ done $ \var -> do
          result <- timeout 2000000 (readMVar var)
          case result of
            Just (Right ()) -> do
              -- Neither caller returns before the sockets are gone.
              closed <- countClosed broker connections
              closed @?= length connections
            Just (Left err) -> assertFailure ("destroyConsumer threw " <> show (err :: SomeException))
            Nothing -> assertFailure "destroyConsumer did not return within 2 seconds"
        allClosedBy broker connections (start + 1),
    testCase "completes after an interrupted destroy" $
      withSilentBroker $ \broker -> do
        consumer <- silentConsumer env mode (label <> "-interrupted") broker
        connections <- awaitConnections broker
        interrupted <- forkIO (destroyConsumer consumer)
        killThread interrupted
        start <- getMonotonicTime
        destroyConsumer consumer
        allClosedBy broker connections (start + 1),
    testCase "close then destroy, destroy then close" $ do
      let props suffix =
            consumerProps env
              <> groupId (makeGroupId env (Text.pack ("destroy-" <> label <> "-" <> suffix)))
              <> noAutoCommit
              <> callbackPollMode mode
      closing <- newAssignedConsumer (props "close-first") >>= either (assertFailure . show) pure
      closeConsumer closing >>= (@?= Nothing)
      closeConsumer closing >>= (@?= Nothing)
      destroyConsumer closing
      destroyConsumer closing

      destroying <- newAssignedConsumer (props "destroy-first") >>= either (assertFailure . show) pure
      destroyConsumer destroying
      closeConsumer destroying >>= (@?= Nothing)
  ]

-- | A consumer whose only bootstrap broker is the silent listener.
silentConsumer :: TestEnv -> CallbackPollMode -> String -> SilentBroker -> IO KafkaConsumer
silentConsumer env mode label broker =
  newAssignedConsumer
    ( brokersList [BrokerAddress ("127.0.0.1:" <> Text.pack (show (sbPort broker)))]
        <> groupId (makeGroupId env (Text.pack ("destroy-" <> label)))
        <> noAutoCommit
        -- Last: the ConsumerProperties semigroup takes the right operand's
        -- poll mode.
        <> callbackPollMode mode
    )
    >>= either (assertFailure . ("newAssignedConsumer: " <>) . show) pure

-- | A TCP listener on loopback that accepts connections and reads from them
-- but never writes, so a Kafka client connected to it waits forever.
data SilentBroker = SilentBroker
  { sbPort :: N.PortNumber,
    -- | One variable per accepted connection, filled when the peer closes it.
    sbConnections :: MVar [MVar ()]
  }

showMillis :: Double -> String
showMillis seconds = show (round (seconds * 1000) :: Int) <> " ms"

withSilentBroker :: (SilentBroker -> IO a) -> IO a
withSilentBroker body =
  bracket open close $ \(_, _, broker) -> body broker
  where
    open = do
      sock <- N.socket N.AF_INET N.Stream N.defaultProtocol
      N.setSocketOption sock N.ReuseAddr 1
      N.bind sock (N.SockAddrInet 0 (N.tupleToHostAddress (127, 0, 0, 1)))
      N.listen sock 16
      port <- N.socketPort sock
      connections <- newMVar []
      acceptor <- forkIO . forever $ do
        (conn, _) <- N.accept sock
        closed <- newEmptyMVar
        modifyMVar_ connections (pure . (closed :))
        void . forkIO $ readUntilClosed conn closed `finally` N.close conn
      pure (sock, acceptor, SilentBroker port connections)
    close (sock, acceptor, _) = killThread acceptor >> N.close sock
    readUntilClosed conn closed =
      (try (NB.recv conn 4096) :: IO (Either SomeException BS.ByteString)) >>= \case
        Right bytes | not (BS.null bytes) -> readUntilClosed conn closed
        _ -> void (tryPutMVar closed ())

-- | Wait (five seconds at most) until the client has connected, and return
-- the connections accepted so far.
awaitConnections :: SilentBroker -> IO [MVar ()]
awaitConnections broker = do
  deadline <- (+ 5) <$> getMonotonicTime
  let go = do
        connections <- readMVar (sbConnections broker)
        now <- getMonotonicTime
        if not (null connections)
          then pure connections
          else
            if now > deadline
              then assertFailure "the consumer did not connect within 5 seconds"
              else threadDelay 20000 >> go
  go

-- | Require every connection accepted so far to be closed by the deadline
-- (monotonic seconds).
allClosedBy :: SilentBroker -> [MVar ()] -> Double -> Assertion
allClosedBy broker earlier deadline = do
  connections <- readMVar (sbConnections broker)
  assertBool "connections accepted before destroy are tracked" (length connections >= length earlier)
  forM_ connections $ \closed -> do
    now <- getMonotonicTime
    let remaining = max 0 (deadline - now)
    result <- timeout (round (remaining * 1000000)) (readMVar closed)
    case result of
      Just () -> pure ()
      Nothing -> assertFailure "a broker connection was still open one second after destroyConsumer started"

-- | How many of the given connections the client has closed, allowing 200 ms
-- for the end-of-file to reach the listener.
countClosed :: SilentBroker -> [MVar ()] -> IO Int
countClosed _ connections =
  length . filter id
    <$> mapM (\closed -> maybe False (const True) <$> timeout 200000 (readMVar closed)) connections
