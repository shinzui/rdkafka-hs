module Kafka.Compatibility.SecurityFixture
  ( SecurityFixture,
    createSecurityFixture,
    destroySecurityFixture,
    securityKafka,
    securitySettings,
    securityTopic,
    withSecurityCredentials,
    assertGroupAuthorizationDenied,
  )
where

import Control.Exception (onException)
import Control.Monad (forM_, unless, void, when)
import Data.List (isInfixOf, isPrefixOf)
import Data.Map.Strict qualified as M
import Data.Text qualified as T
import Data.Text.IO qualified as T
import Kafka.Compatibility.KafkaFixture
import Kafka.TestEnv
import Kafka.Types
import System.Directory
import System.Posix.Files (setFileMode)
import Test.Tasty.HUnit (assertFailure)

-- No Show instance: passwords must never appear in assertion diagnostics.
data SecurityFixture = SecurityFixture
  { securityKafka :: KafkaFixture,
    identityPasswords :: M.Map String String
  }

labels :: [String]
labels = ["admin", "writer", "reader", "denied", "writer2", "reader2"]

username :: String -> String
username label = "fixture_" <> label

secret :: FilePath -> FilePath -> FilePath
secret root name = root <> "/secret/" <> name

readPassword :: FilePath -> String -> IO String
readPassword root label = T.unpack . T.strip <$> T.readFile (secret root (label <> ".password"))

prepare :: FilePath -> IO ()
prepare root = do
  createDirectory (root <> "/secret")
  setFileMode (root <> "/secret") 0o700
  forM_ ("tls" : labels) $ \label -> do
    void (ownedCommand root "openssl" ["rand", "-hex", "-out", secret root (label <> ".password"), "24"])
    setFileMode (secret root (label <> ".password")) 0o600
  let ssl = void . ownedCommand root "openssl"
  ssl ["req", "-x509", "-newkey", "rsa:2048", "-noenc", "-keyout", secret root "ca.key", "-out", secret root "ca.pem", "-days", "1", "-subj", "/CN=rdkafka-hs-fixture-ca"]
  ssl ["req", "-new", "-newkey", "rsa:2048", "-noenc", "-keyout", secret root "server.key", "-out", secret root "server.csr", "-subj", "/CN=localhost"]
  writeFile (secret root "server.ext") "subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth\n"
  ssl ["x509", "-req", "-in", secret root "server.csr", "-CA", secret root "ca.pem", "-CAkey", secret root "ca.key", "-CAcreateserial", "-out", secret root "server.pem", "-days", "1", "-extfile", secret root "server.ext"]
  ssl ["pkcs12", "-export", "-in", secret root "server.pem", "-inkey", secret root "server.key", "-certfile", secret root "ca.pem", "-out", secret root "server.p12", "-passout", "file:" <> secret root "tls.password"]
  listDirectory (root <> "/secret") >>= mapM_ (\name -> setFileMode (secret root name) 0o600)
  writeFile (root <> "/log4j2.xml") "<Configuration status=\"WARN\"><Appenders><Console name=\"Console\" target=\"SYSTEM_OUT\"><PatternLayout pattern=\"%d %p %c %m%n\"/></Console></Appenders><Loggers><Logger name=\"kafka.authorizer.logger\" level=\"DEBUG\" additivity=\"false\"><AppenderRef ref=\"Console\"/></Logger><Root level=\"WARN\"><AppenderRef ref=\"Console\"/></Root></Loggers></Configuration>"

configuration :: FilePath -> Int -> Int -> Int -> IO [String]
configuration root port controller internal = do
  passwords <- mapM (readPassword root) labels
  adminPassword <- readPassword root "admin"
  tlsPassword <- readPassword root "tls"
  let login = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" <> username "admin" <> "\" password=\"" <> adminPassword <> "\" " <> unwords ["user_" <> username label <> "=\"" <> password <> "\"" | (label, password) <- zip labels passwords] <> ";"
  writeFile (secret root "admin.properties") $
    unlines
      [ "security.protocol=SASL_SSL",
        "sasl.mechanism=PLAIN",
        "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" <> username "admin" <> "\" password=\"" <> adminPassword <> "\";",
        "ssl.truststore.type=PEM",
        "ssl.truststore.location=" <> secret root "ca.pem",
        "ssl.endpoint.identification.algorithm=https"
      ]
  setFileMode (secret root "admin.properties") 0o600
  pure
    [ "listeners=SASL_SSL://127.0.0.1:" <> show port <> ",INTERNAL://127.0.0.1:" <> show internal <> ",CONTROLLER://127.0.0.1:" <> show controller,
      "advertised.listeners=SASL_SSL://localhost:" <> show port <> ",INTERNAL://127.0.0.1:" <> show internal <> ",CONTROLLER://127.0.0.1:" <> show controller,
      "listener.security.protocol.map=CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,SASL_SSL:SASL_SSL",
      "inter.broker.listener.name=INTERNAL",
      "sasl.enabled.mechanisms=PLAIN",
      "listener.name.sasl_ssl.plain.sasl.jaas.config=" <> login,
      "ssl.keystore.type=PKCS12",
      "ssl.keystore.location=" <> secret root "server.p12",
      "ssl.keystore.password=" <> tlsPassword,
      "ssl.key.password=" <> tlsPassword,
      "ssl.client.auth=none",
      "authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer",
      "super.users=User:ANONYMOUS;User:" <> username "admin",
      "allow.everyone.if.no.acl.found=false"
    ]

-- Stop all Java processes first, remove credentials/configuration, then inspect
-- retained logs. Even a failed setup enters this cleanup through the runner.
cleanup :: FilePath -> IO ()
cleanup root = do
  present <- doesDirectoryExist (root <> "/secret")
  passwords <- if present then mapM (\label -> doesFileExist (secret root (label <> ".password")) >>= \exists -> if exists then readPassword root label else pure "") ("tls" : labels) else pure []
  names <- listDirectory root
  forM_ names $ \name -> when ("node-" `isPrefixOf` name && ".properties" `isInfixOf` name || "command-" `isPrefixOf` name) (removeFile (root <> "/" <> name))
  when present (removeDirectoryRecursive (root <> "/secret"))
  -- No binary broker data is inspected; only owned, textual log files.
  let inspect path = do
        text <- T.unpack <$> T.readFile path
        let leaked = any (\password -> not (null password) && password `isInfixOf` text) passwords
        when leaked (writeFile path "Credential-bearing diagnostic removed by fixture cleanup.\n")
        pure leaked
      logs directory =
        listDirectory directory
          >>= fmap concat
            . mapM
              ( \name -> do
                  let path = directory <> "/" <> name
                  dir <- doesDirectoryExist path
                  if dir then if "-data" `isInfixOf` name then pure [] else logs path else if ".log" `isInfixOf` name then pure [path] else pure []
              )
  leaked <- logs root >>= mapM inspect
  writeFile (root <> "/security-cleanup.txt") "Owned credential files and JAAS configurations removed; owned log files checked for ephemeral passwords.\n"
  unless (not (or leaked)) (assertFailure "ephemeral credential appeared in an owned log; diagnostic removed")

createSecurityFixture :: IO SecurityFixture
createSecurityFixture = do
  kafka <- createSecureKafkaFixture (SecureExtension prepare configuration (\root -> [("KAFKA_LOG4J_OPTS", "-Dlog4j2.configurationFile=" <> root <> "/log4j2.xml")]) cleanup)
  flip onException (destroyKafkaFixture kafka) $ do
    passwords <- M.fromList <$> mapM (\label -> (label,) <$> readPassword (fixtureRoot kafka) label) labels
    let fixture = SecurityFixture kafka passwords
        environment = fixtureEnv kafka
        common = ["--bootstrap-server", T.unpack (T.intercalate "," (map unBrokerAddress (teBrokers environment))), "--command-config", secret (fixtureRoot kafka) "admin.properties"]
        cli command args = void (fixtureCommand kafka command (common <> args))
    forM_ ["primary", "replacement"] $ \suffix -> do
      let TopicName topic = securityTopic fixture suffix
      cli "kafka-topics" ["--create", "--topic", T.unpack topic, "--partitions", "1", "--replication-factor", "1"]
      forM_ [(if suffix == "primary" then "writer" else "writer2", ["Write", "Describe"]), (if suffix == "primary" then "reader" else "reader2", ["Read", "Describe"]), ("denied", ["Describe"])] $ \(label, operations) ->
        cli "kafka-acls" (["--add", "--allow-principal", "User:" <> username label, "--topic", T.unpack topic] <> concatMap (\operation -> ["--operation", operation]) operations)
    forM_ ["reader", "reader2", "denied"] $ \label ->
      cli "kafka-acls" ["--add", "--allow-principal", "User:" <> username label, "--operation", "Read", "--group", T.unpack (prefixedName environment "security-"), "--resource-pattern-type", "prefixed"]
    pure fixture

destroySecurityFixture :: SecurityFixture -> IO ()
destroySecurityFixture = destroyKafkaFixture . securityKafka

securitySettings :: SecurityFixture -> String -> M.Map T.Text T.Text
securitySettings fixture label =
  M.fromList
    [ ("security.protocol", "SASL_SSL"),
      ("sasl.mechanisms", "PLAIN"),
      ("sasl.username", T.pack (username label)),
      ("sasl.password", T.pack (identityPasswords fixture M.! label)),
      ("ssl.ca.location", T.pack (secret (fixtureRoot (securityKafka fixture)) "ca.pem")),
      ("ssl.endpoint.identification.algorithm", "https"),
      ("log_level", "0")
    ]

securityTopic :: SecurityFixture -> T.Text -> TopicName
securityTopic fixture suffix = TopicName (prefixedName (fixtureEnv (securityKafka fixture)) ("security-" <> suffix))

withSecurityCredentials :: SecurityFixture -> String -> (T.Text -> T.Text -> IO a) -> IO a
withSecurityCredentials fixture label action = action (T.pack (username label)) (T.pack (identityPasswords fixture M.! label))

assertGroupAuthorizationDenied :: SecurityFixture -> String -> T.Text -> IO ()
assertGroupAuthorizationDenied fixture label group = do
  output <- T.readFile (fixtureRoot (securityKafka fixture) <> "/node-1.stdout.log")
  let denied line = ("Principal = User:" <> T.pack (username label) <> " is Denied operation = DESCRIBE") `T.isInfixOf` line && ("on resource = Group:LITERAL:" <> group <> " for request = FindCoordinator") `T.isInfixOf` line
  unless (any denied (T.lines output)) (assertFailure "broker did not audit the exact denied group-coordinator request")
