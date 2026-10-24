#!/usr/bin/env cabal
{-# LANGUAGE GHC2024 #-}
{- cabal:
build-depends: base >=4.21 && <5, aeson, bytestring, directory, filepath, process, text, time
default-language: GHC2024
ghc-options: -Wall
-}
{-# LANGUAGE OverloadedStrings #-}

-- One runner for the repair plans' isolated, compiled semantic mutations.
-- Ordinary tests remain Cabal test suites; this tool only proves their sensitivity.
import Control.Exception (evaluate)
import Control.Monad (forM, forM_, unless, when)
import Data.Aeson (encode, object, (.=))
import Data.ByteString qualified as B
import Data.ByteString.Char8 qualified as BC
import Data.ByteString.Lazy qualified as BL
import Data.Char (isSpace)
import Data.List (findIndex, isInfixOf, isPrefixOf, isSuffixOf, nub, sortOn, stripPrefix, tails)
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Data.Time (defaultTimeLocale, formatTime, getCurrentTime)
import System.Directory (createDirectory, createDirectoryIfMissing, doesDirectoryExist, doesFileExist, getCurrentDirectory, listDirectory, removeFile)
import System.Environment (getArgs, getEnvironment)
import System.Exit (ExitCode (..), die)
import System.FilePath (makeRelative, (</>))
import System.IO (IOMode (WriteMode), hClose, hFlush, openTempFile, stdout, withFile)
import System.Process (CreateProcess (..), StdStream (..), proc, readCreateProcessWithExitCode, waitForProcess, withCreateProcess)
import System.Timeout (timeout)

data Mutation = Mutation
  { name :: String,
    suite :: String,
    patternText :: String,
    expected :: Maybe String,
    edits :: [(FilePath, String -> String)],
    replay :: Bool,
    safety :: Maybe String
  }

integration, unit, producer, callbacks, conversion, consumer, consumerConversion, topic, properties :: String
integration = "rdkafka-hs-integration"
unit = "rdkafka-hs-test"
producer = "src/Kafka/Producer.hs"
callbacks = "src/Kafka/Producer/Callbacks.hs"
conversion = "src/Kafka/Producer/Convert.hs"
consumer = "src/Kafka/Consumer.hs"
consumerConversion = "src/Kafka/Consumer/Convert.hs"
topic = "src/Kafka/Topic.hs"
properties = "src/Kafka/Producer/ProducerProperties.hs"

control :: String -> String -> String -> Mutation
control label component group = Mutation label component ("/" <> group <> "/") Nothing [] False Nothing

mutation :: String -> String -> FilePath -> (String -> String) -> String -> Mutation
mutation group label file change test = Mutation label integration ("/" <> group <> "/&&/" <> test <> "/") (Just test) [(file, change)] False Nothing

families :: String -> [Mutation]
families "producer" =
  control "unchanged-control" integration "producer-regressions"
    : [ p "topic-settings-dropped" producer (replace setting "") "explicit topic timeout expires an accepted message",
        p "rejection-root-retained" producer (replace release "") "queue-full rejection releases callback capture without invoking it",
        unsafe $ p "accepted-root-freed" producer (replace "then pure (Right ())" "then freeStablePtr callbackPtr >> pure (Right ())") "accepted callback retains its capture until a purged report completes",
        unsafe $ p "enqueue-mask-removed" producer (replace "\\topicName -> mask_ $ do" "\\topicName -> do") "cancellation after native acceptance retains callback until its report",
        p "global-exception-skips-message" callbacks (replace "outcome <- try @SomeException (restore (callback rep))" "outcome <- (Right <$> restore (callback rep)) :: IO (Either SomeException ())") "throwing global callback preserves per-message delivery and releases capture",
        p "delivery-root-retained" callbacks (replace "bracket (pure $ castPtrToStablePtr cbPtr) freeStablePtr" "bracket (pure $ castPtrToStablePtr cbPtr) (const (pure ()))") "accepted callback retains its capture until a purged report completes",
        p "empty-bytes-become-absent" producer (replace "withBS (Just bs) f | BS.null bs = allocaBytes 1 $ \\p -> f p 0\n" "") "byte-exact round trip with NoCompression",
        p "input-validation-dropped" producer (replace "case recordError msg of" "case Nothing of") "rejects before native enqueue: NUL topic name",
        p "global-overrides-topic" producer (replace setting (setting <> "\n    setAllKafkaConfValues kc (KafkaProps (ppKafkaProps pps))")) "actual client configuration: explicit topic wins duplicate",
        p "headers-dropped" producer (replace "rdKafkaMessageProduceVa k (hdrs ++ opts)" "rdKafkaMessageProduceVa k opts") "byte-exact round trip with NoCompression",
        p "rejection-release-after-decoding" producer (replace "                  res <- handleProduceErrT code\n" ("                  res <- handleProduceErrT code\n" <> release) . replace release "") "native error decoding failure releases rejected callback capture",
        p "nonnull-success-code-accepted" producer (replace "pure $ Left (ImmediateError (maybe KafkaInvalidReturnValue id res))" "pure $ maybe (Right ()) (Left . ImmediateError) res") "non-null native success-code error rejects and releases capture",
        p "unknown-error-replaced-by-errno" conversion (replace "handleProduceErrT RdKafkaRespErrNoError" "handleProduceErrT RdKafkaRespErrUnknown = Just . kafkaRespErr <$> getErrno\nhandleProduceErrT RdKafkaRespErrNoError") "unknown native error code is preserved instead of reading errno"
      ]
  where
    p = mutation "producer-regressions"
    setting = "    setAllKafkaConfValues kc (KafkaProps (ppTopicProps pps))"
    release = "                  freeStablePtr callbackPtr\n"
    unsafe m = m {safety = Just ("producer safety probe: native enqueue accepted (" <> name m <> ")")}
families "seek" =
  control "unchanged-control" integration "seek-partitions"
    : [ s "member-errors-dropped" consumer (replace selection "pure Nothing") "unassigned member reports native error",
        s "error-order-reversed" consumer (replace selection "pure $ KafkaResponseError <$> find substantive (reverse errors)") "mixed errors follow actual request order",
        s "pending-always-normalized" consumer (replace pending "code /= RdKafkaRespErrInProgress") "nonzero-timeout preserves residual pending",
        s "pending-never-normalized" consumer (replace pending "True") "zero-timeout all pending promises initiation only",
        s "message-commit-forgets-next-offset" consumerConversion (replace "PartitionOffset $ moff + 1" "PartitionOffset moff") "rewind message commit resumes after committed message",
        s "rewind-decrements-committed-offset" consumer (replace "let ps' = map checkOffsets ps" "let ps' = map (\\p -> case tpOffset p of { PartitionOffset n -> p {tpOffset = PartitionOffset (n - 1)}; _ -> p }) (map checkOffsets ps)") "rewind uses committed next offset exactly",
        s "rewind-discards-seek-error" consumer (replace "              seekPartitions c ps' to" "              seekPartitions c ps' to >> pure Nothing") "rewind propagates assignment change after committed lookup"
      ]
  where
    s = mutation "seek-partitions"
    selection = "pure $ KafkaResponseError <$> find substantive errors"
    pending = "(timeout /= 0 || code /= RdKafkaRespErrInProgress)"
families "property" =
  map
    stress
    [ control "pure-control" unit "Producer properties generated",
      control "callback-control" integration "producer-properties",
      pureLaw "global-bias-reversed" "M.union k2 k1" "M.union k1 k2",
      pureLaw "topic-bias-reversed" "M.union t2 t1" "M.union t1 t2",
      pureLaw "log-level-bias-reversed" "ll2 `mplus` ll1" "ll1 `mplus` ll2",
      mutation "producer-properties" "callback-order-reversed" properties (replace "cb1 `mplus` cb2" "cb2 `mplus` cb1") "callback composition preserves order and identity"
    ]
  where
    stress m = m {replay = True}
    pureLaw label old new = (mutation "Producer properties generated" label properties (replace old new) "program follows last-write semantics") {suite = unit}
families "topic" =
  control "unchanged-control" integration "topic-configuration"
    : [ t "configuration-removed" (section "withNewTopic t action =" "withOldTopic ::" (section "configured <-" "\n    case configured of" (const "configured <- pure (Right ())"))) "singleton configuration reaches the broker",
        t "old-acquisition-lifetime" oldAcquisition "setter rejection releases allocated request before submission",
        t "queue-finalization-removed" (replace "bracket (newRdKafkaQueue kafka) finalizeForeignPtr" "bracket (newRdKafkaQueue kafka) (const (pure ()))") "setter rejection releases allocated request before submission",
        t "options-finalization-removed" (replace "bracket (newRdKAdminOptions kafka RdKafkaAdminOpAny) finalizeForeignPtr" "bracket (newRdKAdminOptions kafka RdKafkaAdminOpAny) (const (pure ()))") "setter rejection releases allocated request before submission",
        t "delete-request-finalization-removed" (section "withOldTopic tName transform =" "oldTopicPtr ::" (replace "(mapM_ finalizeForeignPtr)" "(const (pure ()))")) "missing topic deletion preserves its error and releases the request",
        t "event-finalization-removed" (section "waitForResponse topic fromEvent" "singletonList ::" (replace "(mapM_ finalizeForeignPtr)" "(const (pure ()))")) "broker-rejected values release requests and response events repeatedly",
        t "native-request-error-ignored" (replace "requestError <- rdKafkaEventError event" "requestError <- pure RdKafkaRespErrNoError") "unresponsive broker preserves the native timeout error",
        t "topic-identity-ignored" (replace "[Right name] | TopicName (T.pack name) == topic -> Right topic" "[Right _] -> Right topic") "wrong topic response cannot report success"
      ]
  where
    t label change = mutation "topic-configuration" label topic change
    oldAcquisition =
      section "mkNewTopicUnsafe topic =" "-- Keep the client alive" (const oldSetup)
        . section "withNewTopic t action =" "withOldTopic ::" (const "withNewTopic t = withUnsafeOne t mkNewTopicUnsafe rdKafkaNewTopicDestroy\n\n")
    oldSetup =
      unlines
        [ "mkNewTopicUnsafe topic = do",
          "  allocated <- newRdKafkaNewTopicUnsafe (T.unpack $ unTopicName $ topicName topic) (unPartitionCount $ topicPartitionCount topic) (unReplicationFactor $ topicReplicationFactor topic)",
          "  case first (KafkaError . T.pack) allocated of",
          "    Left err -> pure (Left err)",
          "    Right native -> do",
          "      configured <- whileRight (uncurry $ rdKafkaNewTopicSetConfig native) (M.toList $ topicConfig topic)",
          "      pure $ case configured of",
          "        Left err -> Left (KafkaResponseError err)",
          "        Right () -> Right native",
          ""
        ]
families "callback" =
  [ control "integration-control" integration "callback-lifetime",
    control "registry-control" unit "Callback owner",
    c "registrations-never-freed" owner (replace "newCallbackOwner = newCallbackOwnerWith freeHaskellFunPtr" "newCallbackOwner = newCallbackOwnerWith (const (pure ()))") "async error callback releases after destroy with retained wrapper",
    early {safety = Just ("callback safety probe: native destructor entered (" <> name early <> ")")},
    c "native-handoff-mask-removed" native (replace "newRdKafkaT owner kafkaType confPtr = mask_ $" "newRdKafkaT owner kafkaType confPtr =") "producer constructor cancellation at native acceptance releases its owner",
    c "lifecycle-published-before-teardown" consumer (replace "      finalizeForeignPtr k" "      void $ forkIO (finalizeForeignPtr k)") "async concurrent destruction joins at native entry",
    (mutation "Callback owner" "registration-allocation-unmasked" owner (replace "allocate install = mask_ $ do" "allocate install = do") "bridge allocation executes masked before ownership is recorded") {suite = unit}
  ]
  where
    owner = "src/Kafka/Internal/CallbackOwner.hs"
    native = "src/Kafka/Internal/RdKafka.chs"
    c = mutation "callback-lifetime"
    finalizer = section "Concurrent.addForeignPtrFinalizer ret $" "\n{#fun rd_kafka_brokers_add"
    early =
      c
        "registrations-freed-before-native-shutdown"
        native
        (finalizer (replace "                  {# call rd_kafka_destroy_flags #} realPtr 0x8" "                  releaseCallbackOwner owner\n                  {# call rd_kafka_destroy_flags #} realPtr 0x8" . replace "                  releaseCallbackOwner owner\n" ""))
        "async concurrent destruction joins at native entry"
families other = error ("Unknown family: " <> other <> "; choose topic, producer, property, seek or callback")

replace :: String -> String -> String -> String
replace old new source = case [i | (i, rest) <- zip [0 ..] (tails source), old `isPrefixOf` rest] of
  [i] -> take i source <> new <> drop (i + length old) source
  _ -> error ("Mutation target must occur exactly once: " <> old)

section :: String -> String -> (String -> String) -> String -> String
section start end change source = case findIndex (isPrefixOf start) (tails source) of
  Nothing -> error ("Missing section: " <> start)
  Just first -> case findIndex (isPrefixOf end) (tails (drop first source)) of
    Nothing -> error ("Missing section end: " <> end)
    Just size -> take first source <> change (take size (drop first source)) <> drop (first + size) source

main :: IO ()
main =
  getArgs >>= \case
    ["--fingerprint"] -> getCurrentDirectory >>= fingerprint >>= putStrLn
    ["--check", family] -> do
      root <- getCurrentDirectory
      forM_ (families family) $ \m -> forM_ (edits m) $ \(path, change) -> do
        old <- readUtf8 (root </> path)
        let new = change old
        _ <- evaluate (length new)
        when (old == new) (die ("Mutation is unchanged: " <> name m))
      putStrLn "All mutation targets found."
    [family] -> runFamily family
    _ -> die "Usage: cabal run scripts/test-mutations.hs -- FAMILY | --check FAMILY | --fingerprint"

runFamily :: String -> IO ()
runFamily family = do
  root <- getCurrentDirectory
  before <- fingerprint root
  revision <- command root "git" ["rev-parse", "HEAD"]
  now <- getCurrentTime
  let base = root </> "dist-mutations"
  createDirectoryIfMissing True base
  (output, handle) <- openTempFile base (formatTime defaultTimeLocale "%Y%m%dT%H%M%SZ-" now <> family <> "-")
  hClose handle
  removeFile output
  createDirectory output
  _ <- command root "cabal" ["sdist", "--output-directory=" <> output]
  archives <- filter (isSuffixOf ".tar.gz") <$> listDirectory output
  archive <- case archives of [file] -> pure (output </> file); _ -> die "Expected one source archive"
  let package = output </> "source"
  createDirectory package
  _ <- command root "tar" ["-xzf", archive, "--strip-components=1", "-C", package]
  writeFile (package </> "cabal.project") "packages: .\n"
  let variants = families family
  originals <- forM (nub [path | m <- variants, (path, _) <- edits m]) $ \path -> (path,) <$> readUtf8 (package </> path)
  results <- forM variants $ \m -> do
    forM_ originals $ \(path, source) -> writeUtf8 (package </> path) source
    forM_ (edits m) $ \(path, change) -> case lookup path originals of
      Nothing -> die ("Missing original: " <> path)
      Just source -> do
        let changed = change source
        when (source == changed) (die ("Unchanged mutation: " <> name m))
        writeUtf8 (package </> path) changed
    let buildLog = output </> name m <> ".build.log"
        testLog = output </> name m <> ".test.log"
    withFile buildLog WriteMode $ \logHandle ->
      withCreateProcess
        (proc "cabal" ["build", suite m, "-f", "integration"])
          { cwd = Just package,
            std_out = UseHandle logHandle,
            std_err = UseHandle logHandle
          }
        $ \_ _ _ child ->
          waitForProcess child >>= \code -> unless (code == ExitSuccess) (die ("Build failed: " <> buildLog))
    binary <- command package "cabal" ["list-bin", suite m, "-f", "integration", "-v0"]
    environment <- getEnvironment
    let options = ["-p", patternText m] <> ["--quickcheck-replay=20261024" | replay m] <> ["--quickcheck-tests=1000" | replay m] <> ["+RTS", "-N2", "-RTS"]
        key = if maybe False (isPrefixOf "callback safety probe") (safety m) then "RDKAFKA_TEST_CALLBACK_SAFETY_PROBE" else "RDKAFKA_TEST_PRODUCER_SAFETY_PROBE"
        extra = [(key, name m) | safety m /= Nothing]
        process args = (proc binary args) {cwd = Just package, env = Just (extra <> environment)}
    (code, out, err) <- bounded (readCreateProcessWithExitCode (process options) "")
    writeFile testLog (out <> err)
    let intended = maybe False (`namedFailure` out) (expected m)
        invalidAccess = case (code, safety m) of (ExitFailure n, Just marker) -> n < 0 && marker `isInfixOf` err; _ -> False
    case expected m of
      Nothing -> unless (code == ExitSuccess && not ("All 0 tests" `isInfixOf` out)) (die ("Control failed: " <> testLog))
      Just _ -> unless ((code == ExitFailure 1 && intended) || invalidAccess) (die ("Intended regression did not fail: " <> testLog))
    token <-
      if replay m && expected m /= Nothing
        then do
          unless ("Falsified" `isInfixOf` out) (die ("Missing generated counterexample: " <> testLog))
          let marker = "Use --quickcheck-replay=\""
          value <- case findIndex (isPrefixOf marker) (tails out) of
            Nothing -> die ("Missing replay token: " <> testLog)
            Just i -> pure (takeWhile (/= '"') (drop (i + length marker) out))
          let replayOptions = [if "--quickcheck-replay=" `isPrefixOf` arg then "--quickcheck-replay=" <> value else arg | arg <- options]
          (again, replayOut, replayErr) <- bounded (readCreateProcessWithExitCode (process replayOptions) "")
          writeFile (output </> name m <> ".replay.log") (replayOut <> replayErr)
          unless (again == ExitFailure 1 && maybe False (`namedFailure` replayOut) (expected m) && "Falsified" `isInfixOf` replayOut) (die "Counterexample did not replay")
          pure (Just value)
        else pure Nothing
    putStrLn (name m <> ": " <> if expected m == Nothing then "control passed" else "intended regression failed")
    hFlush stdout
    pure $
      object
        [ "mutation" .= name m,
          "compiled" .= True,
          "exit" .= exitNumber code,
          "test" .= expected m,
          "command" .= (binary : options),
          "build_log" .= makeRelative root buildLog,
          "test_log" .= makeRelative root testLog,
          "replay_token" .= token
        ]
  forM_ originals $ \(path, source) -> writeUtf8 (package </> path) source
  after <- fingerprint root
  unless (before == after) (die "Candidate changed during mutations")
  archiveHash <- B.readFile archive >>= digest
  native <- command root "pkg-config" ["--modversion", "rdkafka"]
  BL.writeFile
    (output </> "results.json")
    ( encode
        ( object
            [ "revision" .= revision,
              "fingerprint" .= before,
              "family" .= family,
              "native" .= native,
              "archive_sha256" .= archiveHash,
              "results" .= results
            ]
        )
    )
  putStrLn ("Mutation evidence: " <> output </> "results.json")

namedFailure :: String -> String -> Bool
namedFailure label output = any check (lines output)
  where
    check line = maybe False (isPrefixOf "FAIL" . dropWhile isSpace) (stripPrefix (label <> ":") (dropWhile isSpace line))

exitNumber :: ExitCode -> Int
exitNumber ExitSuccess = 0
exitNumber (ExitFailure n) = n

bounded :: IO a -> IO a
bounded action = timeout 120000000 action >>= maybe (die "Mutation process exceeded 120 seconds") pure

command :: FilePath -> FilePath -> [String] -> IO String
command directory executable arguments = do
  (code, output, errors) <- readCreateProcessWithExitCode (proc executable arguments) {cwd = Just directory} ""
  unless (code == ExitSuccess) (die (unwords (executable : arguments) <> " failed: " <> errors <> output))
  pure (reverse (dropWhile isSpace (reverse output)))

readUtf8 :: FilePath -> IO String
readUtf8 path = T.unpack . TE.decodeUtf8 <$> B.readFile path

writeUtf8 :: FilePath -> String -> IO ()
writeUtf8 path = B.writeFile path . TE.encodeUtf8 . T.pack

fingerprint :: FilePath -> IO String
fingerprint root = do
  files <- concat <$> mapM (walk root) ["src", "test", "test-integration", "test-support", "scripts", "nix"]
  let optional = ["rdkafka-hs.cabal", "cabal.project", "cabal.project.local", "cabal.project.freeze", ".envrc", ".envrc.local", "flake.nix", "flake.lock", "flake.module.nix", "justfile"]
  present <- filterMFile root optional
  -- Match coverage_common.py's pathlib ordering, which compares path components.
  content <- forM (sortOn (T.splitOn "/" . T.pack) (nub (files <> present))) $ \path -> do
    bytes <- B.readFile (root </> path)
    pure (TE.encodeUtf8 (T.pack path) <> B.singleton 0 <> bytes <> B.singleton 0)
  digest (B.concat content)

walk :: FilePath -> FilePath -> IO [FilePath]
walk root relative = do
  exists <- doesDirectoryExist (root </> relative)
  if not exists
    then pure []
    else do
      entries <- filter (/= "__pycache__") <$> listDirectory (root </> relative)
      concat
        <$> forM
          entries
          ( \entry -> do
              let path = relative </> entry
              directory <- doesDirectoryExist (root </> path)
              if directory then walk root path else filterMFile root [path]
          )

filterMFile :: FilePath -> [FilePath] -> IO [FilePath]
filterMFile root files = concat <$> forM files (\file -> doesFileExist (root </> file) >>= \exists -> pure [file | exists])

digest :: B.ByteString -> IO String
digest bytes = withCreateProcess (proc "sha256sum" []) {std_in = CreatePipe, std_out = CreatePipe} $ \input output _ child ->
  case (input, output) of
    (Just writer, Just reader) -> do
      B.hPut writer bytes
      hClose writer
      result <- B.hGetContents reader
      code <- waitForProcess child
      unless (code == ExitSuccess) (die "sha256sum failed")
      pure (take 64 (BC.unpack result))
    _ -> die "Missing hash pipes"
