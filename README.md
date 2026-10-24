# rdkafka-hs

Kafka bindings for Haskell backed by
[librdkafka](https://github.com/confluentinc/librdkafka), the C client for
Apache Kafka.

rdkafka-hs is the maintained successor of
[hw-kafka-client](https://github.com/haskell-works/hw-kafka-client), whose last
Hackage release, 5.3.0, dates from 2023. It contains the full upstream history,
the upstream commits that were never released, and fixes dependents needed,
most importantly that a consumer fatal error is reported from every poll
instead of being silently swallowed in the default callback poll mode. See
[CHANGELOG.md](CHANGELOG.md) for everything that differs from hw-kafka-client
5.3.0.

## Installation

rdkafka-hs is not on Hackage; depend on a release tag. In `cabal.project`:

```cabal
source-repository-package
  type: git
  location: https://github.com/shinzui/rdkafka-hs.git
  tag: v1.0.0.1
```

and in each component's `build-depends`:

```cabal
build-depends: rdkafka-hs >=1.0 && <1.1
```

The modules are `Kafka.Consumer`, `Kafka.Producer`, `Kafka.Metadata`,
`Kafka.Topic`, `Kafka.Transaction` and `Kafka.Version`.

## Migrating from hw-kafka-client

Replace any `hw-kafka-client` pin in `cabal.project` with the stanza above.
Then, in every component that depends on it (libraries, executables, test
suites and benchmarks), replace the dependency and its bounds; keeping
`>=5.3 && <6` would exclude rdkafka-hs's restarted version line:

```diff
-    hw-kafka-client >=5.3 && <6,
+    rdkafka-hs >=1.0 && <1.1,
```

Imports do not change: the `Kafka.*` module namespace, type names and record
field names are the same. GHC 9.12 and librdkafka 2.15 are required (see
Requirements). Behaviour worth knowing when you switch:

- A consumer fatal error (for example a static group member fenced by another
  consumer with the same `group.instance.id`) is now reported by every poll as
  `Left (KafkaResponseError RdKafkaRespErrFatal)`, in both callback poll modes;
  `consumerFatalError` returns the cause. In `CallbackPollModeAsync` it used to
  be unobservable. A fatal consumer must be recreated.
- `callbackPollMode` is no longer reset by properties composed after it, so a
  consumer configured as `callbackPollMode CallbackPollModeSync <> groupId g`
  is now actually synchronous; under hw-kafka-client it ran in async mode. Code
  that reads `cpCallbackPollMode` gets a `Maybe`; use
  `effectiveCallbackPollMode`.
- `flushProducer` uses `rd_kafka_flush`; it still blocks until the queue is
  empty. `flushProducerTimeout` bounds the wait.
- In `CallbackPollModeAsync`, partitions given to `assign` now deliver their
  records to `pollMessage`.
- `closeConsumer` leaves the group gracefully but never frees the client,
  which waits for its garbage-collector finalizer; it can block against an
  unresponsive broker. `destroyConsumer` frees the client immediately, within a
  bound. Close first if you need a graceful departure.

## Requirements

- GHC 9.12 or newer.
- librdkafka 2.15 or newer, with its headers. The build stops with
  `rdkafka-hs requires librdkafka 2.15 or newer` on an older header.
  `Kafka.Version.librdkafkaVersion` reports the library a program is actually
  linked against.
- [c2hs](https://hackage.haskell.org/package/c2hs), used to generate the FFI
  bindings at build time. Cabal builds it automatically through
  `build-tool-depends`.

## Consumer

A high-level consumer joins a consumer group and has partitions assigned to it
by the group; when consumers join or leave, the group rebalances the
partitions among them. A working example is in
[ConsumerExample.hs](example/ConsumerExample.hs). The examples connect to a
broker at `localhost:9092`; build and run them with:

```bash
cabal run kafka-client-example -f examples
```

To select a broker, topic and consumer group, pass all three arguments. The
program reads three input lines, produces four records and then consumes them:

```bash
cabal run kafka-client-example -f examples -- 127.0.0.1:39092 my-example-topic my-example-group
```

```haskell
import Control.Exception (bracket)
import Control.Monad (replicateM_)
import Kafka.Consumer

-- Global consumer properties
consumerProps :: ConsumerProperties
consumerProps = brokersList ["localhost:9092"]
             <> groupId "consumer_example_group"
             <> noAutoCommit
             <> logLevel KafkaLogInfo

-- Subscription to topics
consumerSub :: Subscription
consumerSub = topics ["kafka-client-example-topic"]
           <> offsetReset Earliest

-- Running an example
runConsumerExample :: IO ()
runConsumerExample = do
    res <- bracket mkConsumer clConsumer runHandler
    print res
    where
      mkConsumer = newConsumer consumerProps consumerSub
      clConsumer (Left err) = return (Left err)
      clConsumer (Right kc) = maybe (Right ()) Left <$> closeConsumer kc
      runHandler (Left err) = return (Left err)
      runHandler (Right kc) = processMessages kc

-------------------------------------------------------------------
processMessages :: KafkaConsumer -> IO (Either KafkaError ())
processMessages kafka = do
    replicateM_ 10 $ do
      msg <- pollMessage kafka (Timeout 1000)
      putStrLn $ "Message: " <> show msg
      err <- commitAllOffsets OffsetCommit kafka
      putStrLn $ "Offsets: " <> maybe "Committed." show err
    return $ Right ()
```

Consumer topic names and metadata group queries reject embedded NUL. Partition
IDs outside the native C integer range return `KafkaBadSpecification` through
the operation’s existing error result before changing state. Other invalid
inputs retain librdkafka’s validation and error conventions.

The examples in the sections below assume `import Kafka.Consumer` (or
`import Kafka.Producer`) and the `LambdaCase` and `OverloadedStrings`
extensions.

### Callback poll modes

The default async mode requires an executable linked with `-threaded`.
Without it, consumer construction returns `KafkaBadSpecification`; select
`callbackPollMode CallbackPollModeSync` to use a nonthreaded executable.

librdkafka reports rebalances, offset commits, errors, logs and statistics
through callbacks, which run only while something polls the client. A consumer
has one of two callback poll modes, set with `callbackPollMode`:

- `CallbackPollModeAsync` (the default): a background thread polls every
  100 ms to serve callbacks, and records are redirected to a separate queue
  that `pollMessage` reads. Callbacks keep running while the application is
  busy, so librdkafka's `max.poll.interval.ms` cannot detect an application
  that has stopped polling.
- `CallbackPollModeSync`: callbacks run inside the application's own
  `pollMessage` calls, and nothing runs between them. `pollMessageBatch` is not
  available in this mode.

```haskell
syncProps :: ConsumerProperties
syncProps = brokersList ["localhost:9092"]
         <> groupId "my-group"
         <> callbackPollMode CallbackPollModeSync
```

### Fatal errors

Some errors are permanent: once librdkafka raises a fatal error on a consumer,
it stops all group activity, and the only remedy is a new consumer. Every poll
then returns `Left (KafkaResponseError RdKafkaRespErrFatal)`, in both poll modes,
and `consumerFatalError` returns the underlying cause:

```haskell
consumeLoop :: KafkaConsumer -> IO ()
consumeLoop consumer =
  pollMessage consumer (Timeout 1000) >>= \case
    Right record -> print (crValue record) >> consumeLoop consumer
    Left (KafkaResponseError RdKafkaRespErrTimedOut) -> consumeLoop consumer
    Left (KafkaResponseError RdKafkaRespErrFatal) -> do
      cause <- consumerFatalError consumer
      putStrLn ("consumer is permanently broken: " <> show cause)
      -- close or destroy it, then create a new consumer
    Left err -> print err >> consumeLoop consumer
```

### Waiting for records without polling

`setConsumerQueueIOEvent` makes librdkafka write a payload to a non-blocking
file descriptor when records or events reach the queue `pollMessage` reads, so
an application can wait with `threadWaitRead` instead of polling on a timer.
The notification is edge-triggered: one wake-up can stand for several records,
or for an event that is not a record. Discard pending wake-ups, poll the queue
empty, then wait, and bound the wait so a fatal error is still noticed:

```haskell
import Control.Concurrent (threadWaitRead)
import Control.Exception (bracket, try)
import Control.Monad (when)
import Data.ByteString (ByteString)
import Foreign.Marshal.Alloc (allocaBytes)
import System.Posix.IO (FdOption (..), closeFd, createPipe, fdReadBuf, setFdOption)
import System.Posix.Types (ByteCount, Fd)
import System.Timeout (timeout)

-- | Handle records as librdkafka announces them, waking at least once a second.
-- Returns when the consumer turns fatal.
waitAndConsume :: KafkaConsumer -> (ConsumerRecord (Maybe ByteString) (Maybe ByteString) -> IO ()) -> IO ()
waitAndConsume consumer handle =
  bracket createPipe release $ \(readEnd, writeEnd) -> do
    -- NonBlockingRead sets O_NONBLOCK, which librdkafka's writes need too.
    setFdOption readEnd NonBlockingRead True
    setFdOption writeEnd NonBlockingRead True
    setConsumerQueueIOEvent consumer (Just (writeEnd, "1"))
    let drain =
          pollMessage consumer (Timeout 0) >>= \case
            Right record -> handle record >> drain
            Left (KafkaResponseError RdKafkaRespErrTimedOut) -> pure True -- queue empty
            Left (KafkaResponseError RdKafkaRespErrFatal) -> pure False
            Left _ -> drain -- a non-record event
        loop = do
          discardWakeups readEnd -- before draining, so no wake-up is lost
          alive <- drain
          when alive $ do
            _ <- timeout 1000000 (threadWaitRead readEnd)
            loop
    loop
  where
    release (readEnd, writeEnd) = do
      setConsumerQueueIOEvent consumer Nothing -- before closing the descriptor
      closeFd readEnd
      closeFd writeEnd

discardWakeups :: Fd -> IO ()
discardWakeups fd =
  allocaBytes 64 $ \buf ->
    (try (fdReadBuf fd buf 64) :: IO (Either IOError ByteCount)) >>= \case
      Right n | n > 0 -> discardWakeups fd
      _ -> pure ()
```

### Reading partitions without joining a group

`newAssignedConsumer` creates a consumer that never subscribes, so it never
joins its consumer group: it reads only the partitions given to `assign`, from
the offsets given there. With `enable.partition.eof` set, librdkafka reports when
a partition's end is reached, and `pollMessageAttributed` says which partition
and offset each such error belongs to. Together they make a read-only scanner:

```haskell
-- | Print partition 0 of "events" from its first offset to its current end.
scanPartition :: IO ()
scanPartition =
  bracket (newAssignedConsumer props) (either (const (pure ())) destroyConsumer) $ \case
    Left err -> print err
    Right consumer -> do
      _ <- assign consumer [TopicPartition "events" (PartitionId 0) PartitionOffsetBeginning]
      let loop =
            pollMessageAttributed consumer (Timeout 1000) >>= \case
              Right record -> print (crOffset record, crValue record) >> loop
              Left ConsumerMessageError {cmeError = KafkaResponseError RdKafkaRespErrPartitionEof, cmeOffset} ->
                putStrLn ("reached the end at " <> show cmeOffset)
              Left ConsumerMessageError {cmeError = KafkaResponseError RdKafkaRespErrTimedOut} -> loop
              Left err -> print err
      loop
  where
    props = brokersList ["localhost:9092"]
         <> groupId "events-observer" -- required by librdkafka; never joined
         <> noAutoCommit
         <> extraProp "enable.partition.eof" "true"
```

### Shutting down a consumer

`closeConsumer` is the graceful close: it leaves the consumer group (letting the
group rebalance at once) and commits offsets if auto-commit is enabled. It does
not free the librdkafka client, which otherwise waits for a garbage collection,
and it can block against an unresponsive broker. `destroyConsumer` stops the
consumer's threads and closes its broker connections before it returns; every
other operation on the consumer is invalid afterwards. Closing and destroying
are each safe to repeat.

```haskell
shutdownConsumer :: KafkaConsumer -> IO ()
shutdownConsumer consumer = do
  _ <- closeConsumer consumer -- leave the group; commits if auto-commit is on
  destroyConsumer consumer    -- free the client and close its connections now
```

To bound shutdown when the broker may not answer, call only `destroyConsumer`;
the group then notices the departure when the consumer's session times out.

## Producer

The producer sends messages to any number of topics; the target topic is part
of each `ProducerRecord` given to `produceMessage`. A working example is in
[ProducerExample.hs](example/ProducerExample.hs).

```haskell
{-# LANGUAGE OverloadedStrings #-}
import Control.Exception (bracket)
import Control.Monad (forM_)
import Data.ByteString (ByteString)
import Kafka.Producer

-- Global producer properties
producerProps :: ProducerProperties
producerProps = brokersList ["localhost:9092"]
             <> logLevel KafkaLogDebug

-- Topic to send messages to
targetTopic :: TopicName
targetTopic = "kafka-client-example-topic"

-- Run an example
runProducerExample :: IO ()
runProducerExample =
    bracket mkProducer clProducer runHandler >>= print
    where
      mkProducer = newProducer producerProps
      clProducer (Left _)     = return ()
      clProducer (Right prod) = closeProducer prod
      runHandler (Left err)   = return $ Left err
      runHandler (Right prod) = sendMessages prod

sendMessages :: KafkaProducer -> IO (Either KafkaError ())
sendMessages prod = do
  err1 <- produceMessage prod (mkMessage Nothing (Just "test from producer") )
  forM_ err1 print

  err2 <- produceMessage prod (mkMessage (Just "key") (Just "test from producer (with key)"))
  forM_ err2 print

  return $ Right ()

mkMessage :: Maybe ByteString -> Maybe ByteString -> ProducerRecord
mkMessage k v = ProducerRecord
                  { prTopic = targetTopic
                  , prPartition = UnassignedPartition
                  , prKey = k
                  , prValue = v
                  , prHeaders = mempty
                  }
```

### Delivery reports

The producer keeps its own queue of outgoing messages. `produceMessage`
returning means the message is queued, not that it has reached Kafka. A message
that cannot be sent within `message.timeout.ms` (see librdkafka's
[configuration](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md))
is dropped from the queue and reported as failed. `sendTimeout (Timeout 0)`
makes the timeout infinite, so messages are never dropped:

```haskell
producerProps :: ProducerProperties
producerProps = brokersList ["localhost:9092"]
             <> sendTimeout (Timeout 0) -- for librdkafka "0" means "infinite"
```

`deliveryCallback` receives a `DeliveryReport` for every message: delivered at an
offset, failed with an error, or an error librdkafka could not attach to a
message. Callbacks run while the producer is polled, for example by
`flushProducer`:

```haskell
producerProps :: ProducerProperties
producerProps = brokersList ["localhost:9092"]
             <> setCallback (deliveryCallback report)
  where
    report = \case
      DeliverySuccess record offset -> putStrLn ("delivered to " <> show (prTopic record) <> " at " <> show offset)
      DeliveryFailure _ err -> putStrLn ("not delivered: " <> show err)
      NoMessageError err -> putStrLn ("delivery error: " <> show err)
```

### Synchronous sending of messages

librdkafka has no synchronous produce call, but `produceMessage'` takes a
callback for the message's own delivery report, which an `MVar` turns into a
synchronous send:

```haskell
{-# LANGUAGE LambdaCase #-}
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Kafka.Consumer (Offset)
import Kafka.Producer

sendMessageSync :: MonadIO m => KafkaProducer -> ProducerRecord -> m (Either KafkaError Offset)
sendMessageSync producer record = liftIO $ do
  var <- newEmptyMVar
  res <- produceMessage' producer record (putMVar var)
  case res of
    Left (ImmediateError err) -> pure (Left err)
    Right () -> do
      -- Flush so the delivery report is served rather than waited for forever.
      flushProducer producer
      takeMVar var >>= \case
        DeliverySuccess _ offset -> pure (Right offset)
        DeliveryFailure _ err -> pure (Left err)
        NoMessageError err -> pure (Left err)
```

`flushProducer` waits until the queue is empty, which against an unreachable
broker can take as long as `message.timeout.ms`.

### Bounded shutdown

`closeProducer` flushes the queue without a time limit. To bound shutdown, flush
with `flushProducerTimeout` and purge whatever is left; purged messages are
reported to the delivery callback as failed
(`RdKafkaRespErrPurgeQueue` or `RdKafkaRespErrPurgeInflight`):

```haskell
shutdownProducer :: KafkaProducer -> IO ()
shutdownProducer producer =
  flushProducerTimeout producer (Timeout 5000) >>= \case
    Nothing -> pure ()
    Just _ -> do
      _ <- purgeProducer producer [PurgeQueue, PurgeInFlight]
      _ <- flushProducerTimeout producer (Timeout 1000) -- serves the purged reports
      pure ()
```

## Development

The Nix flake provides GHC, HLS, cabal, c2hs and librdkafka. Enter the shell
with `nix develop` (or let direnv load it), then:

```bash
just build            # cabal build all
just test             # cabal test rdkafka-hs-test (unit tests, no broker)
just test-it          # integration tests against the private broker (below)
just build-examples   # build the example executable
just fmt              # nix fmt: fourmolu, cabal-gild and nixpkgs-fmt
just check            # sandboxed build and unit tests, formatting, docs and bindings checks
just docs-check       # validate docs/adr and docs/improvement-requests
just bindings-check   # verify docs/bindings.md against the binding module
```

`nix build` builds the package and runs its unit tests in the Nix sandbox.

[docs/bindings.md](docs/bindings.md) lists every function librdkafka's public
header exports and which of them this package binds. It is generated from the
header of the librdkafka release that pkg-config finds (cloned once into
`~/.cache/rdkafka-hs/`, or named by `RDKAFKA_SOURCE_HEADER`) and from
`src/Kafka/Internal/RdKafka.chs`; regenerate it with `just bindings-inventory`
after changing the bindings.

Design decisions are recorded in [docs/adr/](docs/adr/index.md).

## Integration tests

The integration suite `rdkafka-hs-integration` needs a running Kafka broker and
is disabled unless the manual `integration` flag is set, so `cabal build all`,
`cabal test` and `nix build` never need one.

The suite runs against a private Redpanda broker owned by this project. It never
uses the machine-wide broker at `127.0.0.1:9092`, and refuses to start if
`RDKAFKA_TEST_BROKER` names it, because the tests create and delete topics.

Requirements: an Apple Silicon Mac on macOS 26 or newer with
[Apple Container](https://github.com/apple/container) installed and its service
running (`container system start`).

```bash
just test-it          # start the broker if needed, then run the suite
just redpanda-up      # start the broker (idempotent)
just redpanda-status  # exits non-zero unless the broker is serving
just redpanda-logs    # tail the broker logs
just redpanda-down    # stop the broker, keeping its data
just redpanda-purge   # delete its containers, network and volume
```

Arguments after `just test-it` go to cabal; to run one group, for example:

```bash
just test-it --test-option=-p '--test-option=/queue event/'
```

`process-compose up` also starts the broker. The dev shell exports
`RDKAFKA_TEST_BROKER=127.0.0.1:39092` unless it is already set. Every topic and
consumer group a run creates carries a random per-run prefix, so runs do not
interfere with each other.

| Service          | Host port | Container            |
|------------------|-----------|----------------------|
| Kafka            | 39092     | `rdkafka-hs-rp-0`    |
| Admin API        | 39644     | `rdkafka-hs-rp-0`    |
| Schema Registry  | 38081     | `rdkafka-hs-rp-0`    |
| HTTP Proxy       | 38082     | `rdkafka-hs-rp-0`    |

The ports are the `redpanda.*-port` variables in `.seihou/manifest.json`,
rendered into `nix/redpanda.nix` by `seihou run nix-haskell-flake`; changing one
means changing it there, in `flake.module.nix` and in
`test-integration/Kafka/TestEnv.hs`.

## Credits

rdkafka-hs is a fork of [hw-kafka-client](https://github.com/haskell-works/hw-kafka-client)
by Alexey Raga and its contributors, which was itself inspired by
[Haskakafka](https://github.com/cosbynator/haskakafka).
