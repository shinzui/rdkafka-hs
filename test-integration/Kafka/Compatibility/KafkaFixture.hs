-- | Disposable Apache processes owned by this test run. The explicit
-- qualification entry point requires a released Kafka distribution and Java.
module Kafka.Compatibility.KafkaFixture
  ( KafkaFixture,
    createKafkaFixture,
    SecureExtension (..),
    createSecureKafkaFixture,
    destroyKafkaFixture,
    fixtureEnv,
    fixtureRoot,
    fixtureCommand,
    ownedCommand,
    stopBroker,
    startBroker,
    awaitBrokers,
    fixtureAllStopped,
    withBrokersStopped,
  )
where

import Control.Concurrent (threadDelay)
import Control.Concurrent.MVar
import Control.Exception (SomeException, bracket, bracketOnError, bracket_, mask, mask_, onException, throwIO, try)
import Control.Monad (forM, forM_, unless, void)
import Data.IORef
import Data.List (intercalate, sort)
import Data.Text qualified as T
import Kafka.Metadata qualified as D
import Kafka.Producer qualified as P
import Kafka.TestEnv
import Kafka.Types
import Network.Socket qualified as N
import System.Directory
import System.Environment (getEnvironment, lookupEnv)
import System.Exit (ExitCode (..))
import System.IO
import System.Posix.Files (setFileMode)
import System.Posix.Signals (sigKILL, signalProcess)
import System.Process
import System.Timeout (timeout)
import Test.Tasty.HUnit (assertFailure)

data Node = Node Int Int FilePath (MVar (Maybe ProcessHandle))

data KafkaFixture = KafkaFixture
  { fixtureEnv :: TestEnv,
    fixtureRoot :: FilePath,
    kafkaHome :: FilePath,
    fixtureNodes :: [Node],
    healthEnv :: TestEnv,
    secureExtension :: Maybe SecureExtension
  }

-- Security owns credentials and policy; this runner owns every process and port.
data SecureExtension = SecureExtension
  { securePrepare :: FilePath -> IO (),
    secureConfiguration :: FilePath -> Int -> Int -> Int -> IO [String],
    secureEnvironment :: FilePath -> [(String, String)],
    secureCleanup :: FilePath -> IO ()
  }

bounded :: Int -> String -> IO a -> IO a
bounded seconds label action = timeout (seconds * 1000000) action >>= maybe (assertFailure label) pure

-- This function can terminate only a ProcessHandle returned by createProcess.
-- It never resolves a machine-global broker or reads an externally supplied PID.
stopProcess :: ProcessHandle -> IO ()
stopProcess process =
  getProcessExitCode process >>= \case
    Just _ -> pure ()
    Nothing -> do
      terminateProcess process
      timeout 15000000 (waitForProcess process) >>= \case
        Just _ -> pure ()
        Nothing -> do
          getPid process >>= mapM_ (signalProcess sigKILL)
          void (bounded 5 "owned Kafka process did not exit after SIGKILL" (waitForProcess process))

fixtureCommand :: KafkaFixture -> String -> [String] -> IO String
fixtureCommand fixture command = ownedCommand (fixtureRoot fixture) (kafkaHome fixture <> "/bin/" <> command <> ".sh")

ownedCommand :: FilePath -> FilePath -> [String] -> IO String
ownedCommand root command args = mask $ \restore -> do
  (outputPath, outputHandle) <- openTempFile root "command-out-"
  (_, errorHandle) <- openTempFile root "command-err-" `onException` hClose outputHandle
  -- createProcess closes the parent's UseHandle copies after duplicating them.
  (_, _, _, process) <-
    createProcess
      (proc command args)
        { std_in = NoStream,
          std_out = UseHandle outputHandle,
          std_err = UseHandle errorHandle
        }
      `onException` (hClose outputHandle >> hClose errorHandle)
  code <- restore (bounded 45 (command <> " timed out") (waitForProcess process)) `onException` stopProcess process
  output <- readFile outputPath
  case code of
    ExitSuccess -> pure output
    _ -> assertFailure ("owned fixture command failed: " <> show (command, code) <> "; private output retained for sanitized inspection")

createKafkaFixture :: IO KafkaFixture
createKafkaFixture = createFixture 3 Nothing

createSecureKafkaFixture :: SecureExtension -> IO KafkaFixture
createSecureKafkaFixture = createFixture 1 . Just

createFixture :: Int -> Maybe SecureExtension -> IO KafkaFixture
createFixture count extension = do
  home <- lookupEnv "RDKAFKA_TEST_KAFKA_HOME" >>= maybe (assertFailure "set RDKAFKA_TEST_KAFKA_HOME to a released Apache Kafka distribution") pure
  forM_ ["kafka-storage", "kafka-server-start", "kafka-topics", "kafka-configs"] $ \name ->
    doesFileExist (home <> "/bin/" <> name <> ".sh") >>= \found -> unless found (assertFailure ("missing Kafka fixture command: " <> name))
  createDirectoryIfMissing True "dist-compatibility/kafka-fixtures"
  (root, handle) <- openTempFile "dist-compatibility/kafka-fixtures" "owned-"
  hClose handle
  removeFile root
  createDirectory root
  setFileMode root 0o700
  writeFile (root <> "/owned-by-rdkafka-hs") "run-owned Apache qualification fixture\n"
  -- Bind all ports while preparing the files. The default primary and
  -- global brokers cannot be selected: occupied ports never bind successfully.
  bracket (reserveAll (3 * count) >>= newIORef) releasePorts $ \reservations -> do
    reserved <- readIORef reservations
    let ports = map snd reserved
        brokerPorts = take count ports
        controllerPorts = take count (drop count ports)
        internalPorts = drop (2 * count) ports
        hostname = maybe "127.0.0.1:" (const "localhost:") extension
        addresses = [BrokerAddress (hostname <> T.pack (show port)) | port <- brokerPorts]
    environment <- newTestEnv addresses
    health <- maybe (pure environment) (const (newTestEnv [BrokerAddress ("127.0.0.1:" <> T.pack (show port)) | port <- internalPorts])) extension
    nodes <- forM (zip [1 ..] brokerPorts) $ \(node, port) -> Node node port (root <> "/node-" <> show node <> ".properties") <$> newMVar Nothing
    let fixture = KafkaFixture environment root home nodes health extension
        voters = intercalate "," [show node <> "@127.0.0.1:" <> show port | (node, port) <- zip [1 .. count] controllerPorts]
    flip onException (destroyKafkaFixture fixture) $ do
      mapM_ (\security -> securePrepare security root) extension
      cluster <- filter (`notElem` ['\r', '\n']) <$> fixtureCommand fixture "kafka-storage" ["random-uuid"]
      forM_ (zip3 nodes controllerPorts internalPorts) $ \(Node node port config _, controller, internal) -> do
        additional <- maybe (pure []) (\security -> secureConfiguration security root port controller internal) extension
        writeFile config $
          unlines $
            [ "process.roles=broker,controller",
              "node.id=" <> show node,
              "controller.quorum.voters=" <> voters,
              "listeners=PLAINTEXT://127.0.0.1:" <> show port <> ",CONTROLLER://127.0.0.1:" <> show controller,
              "advertised.listeners=PLAINTEXT://127.0.0.1:" <> show port <> ",CONTROLLER://127.0.0.1:" <> show controller,
              "inter.broker.listener.name=PLAINTEXT",
              "controller.listener.names=CONTROLLER",
              "listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
              "log.dirs=" <> root <> "/node-" <> show node <> "-data",
              "default.replication.factor=" <> show count,
              "min.insync.replicas=1",
              "num.partitions=1",
              "offsets.topic.replication.factor=" <> show count,
              "offsets.topic.num.partitions=3",
              "transaction.state.log.replication.factor=" <> show count,
              "transaction.state.log.min.isr=" <> show (min 2 count),
              "transaction.state.log.num.partitions=3",
              "group.initial.rebalance.delay.ms=0",
              "auto.create.topics.enable=false",
              "num.network.threads=2",
              "num.io.threads=2",
              "num.replica.fetchers=1"
            ]
              <> additional
        setFileMode config 0o600
        void (fixtureCommand fixture "kafka-storage" ["format", "-t", cluster, "-c", config])
      -- Transfer the sockets out of the cleanup reference exactly once before
      -- Java binds. Partial acquisition and cancellation retain cleanup.
      releasePorts reservations
      mapM_ (startNode fixture) nodes
      awaitBrokers fixture
      version <- fixtureCommand fixture "kafka-topics" ["--version"]
      writeFile (root <> "/version.txt") version
      putStrLn ("[owned-kafka] root=" <> root <> " brokers=" <> show addresses <> " version=" <> version)
      pure fixture
  where
    releasePorts reference = atomicModifyIORef' reference (\sockets -> ([], sockets)) >>= mapM_ (N.close . fst)
    reserveAll 0 = pure []
    reserveAll remaining = bracketOnError reservePort (N.close . fst) $ \socket -> (socket :) <$> reserveAll (remaining - 1)
    reservePort = do
      socket <- N.socket N.AF_INET N.Stream N.defaultProtocol
      flip onException (N.close socket) $ do
        N.bind socket (N.SockAddrInet 0 (N.tupleToHostAddress (127, 0, 0, 1)))
        port <- fromIntegral <$> N.socketPort socket
        whenForbidden port
        pure (socket, port)
    whenForbidden port = unless (port `notElem` [9092, 39092, 39644]) (assertFailure "reserved a protected broker port")

startNode :: KafkaFixture -> Node -> IO ()
startNode fixture (Node node _ config state) = mask_ $ modifyMVar_ state $ \case
  Just _ -> assertFailure "owned broker is already running"
  Nothing -> do
    variables <- getEnvironment
    let extra = [("KAFKA_HEAP_OPTS", "-Xms256m -Xmx384m"), ("LOG_DIR", fixtureRoot fixture <> "/node-" <> show node <> "-logs")] <> maybe [] (\security -> secureEnvironment security (fixtureRoot fixture)) (secureExtension fixture)
    logHandle <- openFile (fixtureRoot fixture <> "/node-" <> show node <> ".stdout.log") AppendMode
    (_, _, _, process) <-
      createProcess
        (proc (kafkaHome fixture <> "/bin/kafka-server-start.sh") [config])
          { env = Just (extra <> filter (\(key, _) -> key `notElem` map fst extra) variables),
            std_in = NoStream,
            std_out = UseHandle logHandle,
            std_err = UseHandle logHandle
          }
        `onException` hClose logHandle
    pure (Just process)

stopNode :: Node -> IO ()
stopNode (Node _ _ _ state) = modifyMVar_ state $ \process -> mapM_ stopProcess process >> pure Nothing

findNode :: KafkaFixture -> BrokerId -> IO Node
findNode fixture (BrokerId wanted) = case [node | node@(Node identifier _ _ _) <- fixtureNodes fixture, identifier == wanted] of
  [node] -> pure node
  _ -> assertFailure "broker ID is outside this run-owned topology"

stopBroker :: KafkaFixture -> BrokerId -> IO ()
stopBroker fixture broker = findNode fixture broker >>= stopNode

startBroker :: KafkaFixture -> BrokerId -> IO ()
startBroker fixture broker = findNode fixture broker >>= startNode fixture

awaitBrokers :: KafkaFixture -> IO ()
awaitBrokers fixture = bracket (P.newProducer (P.brokersList (teBrokers (healthEnv fixture)) <> P.extraProp "log_level" "0") >>= either throwIO pure) releaseProducer $ \producer ->
  bounded 45 "owned Apache brokers did not become reachable" $
    let await =
          D.allTopicsMetadata producer (Timeout 1000) >>= \case
            Right metadata | sort (map D.bmBrokerId (D.kmBrokers metadata)) == [BrokerId node | Node node _ _ _ <- fixtureNodes fixture] -> pure ()
            _ -> threadDelay 100000 >> await
     in await

destroyKafkaFixture :: KafkaFixture -> IO ()
destroyKafkaFixture fixture = do
  -- Cleanup is installed before any process starts, including partial setup.
  results <- mapM (try @SomeException . stopNode) (fixtureNodes fixture)
  case [exception | Left exception <- results] of
    exception : _ -> throwIO exception
    [] -> pure ()
  mapM_ (\security -> secureCleanup security (fixtureRoot fixture)) (secureExtension fixture)
  forM_ (fixtureNodes fixture) $ \(Node node _ _ _) -> do
    let dataDirectory = fixtureRoot fixture <> "/node-" <> show node <> "-data"
    exists <- doesDirectoryExist dataDirectory
    if exists then removeDirectoryRecursive dataDirectory else pure ()
  writeFile (fixtureRoot fixture <> "/cleanup.txt") "All owned process handles exited; owned broker data removed.\n"

fixtureAllStopped :: KafkaFixture -> IO Bool
fixtureAllStopped fixture = and <$> mapM stopped (fixtureNodes fixture)
  where
    stopped (Node _ _ _ state) = readMVar state >>= maybe (pure True) (fmap (/= Nothing) . getProcessExitCode)

-- Install restoration for each successfully stopped process before stopping
-- the next. Start all controllers before the caller waits for quorum health.
withBrokersStopped :: KafkaFixture -> IO a -> IO a
withBrokersStopped fixture action = foldr (\node next -> bracket_ (stopNode node) (startNode fixture node) next) action (fixtureNodes fixture)
