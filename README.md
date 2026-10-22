# rdkafka-hs

Kafka bindings for Haskell backed by
[librdkafka](https://github.com/confluentinc/librdkafka), the C client for
Apache Kafka.

rdkafka-hs is the maintained successor of
[hw-kafka-client](https://github.com/haskell-works/hw-kafka-client). Upstream
has not been released since 5.3.0 (2023) and has not been pushed to since
October 2025, while dependents need fixes it does not carry, most importantly
that a consumer fatal error is now reported from every poll instead of being
silently swallowed in the default callback poll mode. This repository contains
the full upstream history, the unreleased upstream commits and those fixes.
See [CHANGELOG.md](CHANGELOG.md) for everything that differs from
hw-kafka-client 5.3.0.

The `Kafka.*` module namespace, type names and record fields are unchanged, so
migrating from hw-kafka-client means replacing the dependency name and its
version bounds in `build-depends`:

```cabal
build-depends: rdkafka-hs >=1.0 && <1.1
```

## Requirements

- GHC 9.12 or newer.
- librdkafka 2.15 or newer, with its headers and pkg-config file. Cabal checks
  the version through pkg-config and refuses an older library at configure
  time. `Kafka.Version.librdkafkaVersion` reports the library a program is
  actually linked against.
- [c2hs](https://hackage.haskell.org/package/c2hs), used to generate the FFI
  bindings at build time.

## Development

The Nix flake provides GHC, HLS, cabal, c2hs and librdkafka. Enter the shell
with `nix develop` (or let direnv load it), then:

```bash
just build            # cabal build all
just test             # cabal test rdkafka-hs-test (unit tests, no broker)
just build-examples   # build the example executable
just fmt              # nix fmt: fourmolu, cabal-gild and nixpkgs-fmt
just check            # sandboxed build and unit tests, formatting, docs-check
just docs-check       # validate docs/adr and docs/improvement-requests
```

`nix build` builds the package and runs its unit tests in the Nix sandbox.

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

## Consumer

High level consumers are supported by `librdkafka` starting from version 0.9.
High-level consumers provide an abstraction for consuming messages from multiple
partitions and topics. They also address scalability (up to a number of partitions)
by providing automatic rebalancing functionality. When a new consumer joins a consumer
group the set of consumers attempts to "rebalance" the load to assign partitions to each consumer.

### Consumer example

```bash
cabal build --flag examples
```

or

```bash
cabal run kafka-client-example --flag examples
```

A working consumer example can be found here: [ConsumerExample.hs](example/ConsumerExample.hs)</br>
To run an example please compile with the `examples` flag.

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

## Producer

The producer supports sending messages to multiple topics.
Target topic name is a part of each message that is to be sent by `produceMessage`.

A working producer example can be found here: [ProducerExample.hs](example/ProducerExample.hs)

### Delivery reports

Kafka Producer maintains its own internal queue for outgoing messages. Calling `produceMessage`
does not mean that the message is actually written to Kafka, it only means that the message is put
to that outgoing queue and that the producer will (eventually) push it to Kafka.

However, it is not always possible for the producer to send messages to Kafka. Network problems
or Kafka cluster being offline can prevent the producer from doing it.

When a message cannot be sent to Kafka for some time (see `message.timeout.ms` [configuration](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md) option),
the message is *dropped from the outgoing queue* and the *delivery report* indicating an error is raised.

It is possible to configure the producer with an infinite message timeout so the message is
never dropped from the queue:

```haskell
producerProps :: ProducerProperties
producerProps = brokersList ["localhost:9092"]
             <> sendTimeout (Timeout 0)           -- for librdkafka "0" means "infinite" (see https://github.com/edenhill/librdkafka/issues/2015)
```

*Delivery reports* provide the way to detect when producer experiences problems sending messages
to Kafka.

Currently the producer only supports delivery error callbacks:

```haskell
producerProps :: ProducerProperties
producerProps = brokersList ["localhost:9092"]
             <> setCallback (deliveryCallback print)
```

In the example above when the producer cannot deliver the message to Kafka,
the error will be printed (and the message will be dropped).

### Producer example

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

### Synchronous sending of messages

Because of the asynchronous nature of librdkafka, there is no API to provide
synchronous production of messages. It is, however, possible to combine the
delivery reports feature with that of callbacks. This can be done using the
`Kafka.Producer.produceMessage'` function.

```haskell
produceMessage' :: MonadIO m
                => KafkaProducer
                -> ProducerRecord
                -> (DeliveryReport -> IO ())
                -> m (Either ImmediateError ())
```

Using this function, you can provide a callback which will be invoked upon the
produced message's delivery report. With a little help of `MVar`s or similar,
you can in fact, create a synchronous-like interface.

```haskell
sendMessageSync :: MonadIO m
                => KafkaProducer
                -> ProducerRecord
                -> m (Either KafkaError Offset)
sendMessageSync producer record = liftIO $ do
  -- Create an empty MVar:
  var <- newEmptyMVar

  -- Produce the message and use the callback to put the delivery report in the
  -- MVar:
  res <- produceMessage' producer record (putMVar var)

  case res of
    Left (ImmediateError err) ->
      pure (Left err)
    Right () -> do
      -- Flush producer queue to make sure you don't get stuck waiting for the
      -- message to send:
      flushProducer producer

      -- Wait for the message's delivery report and map accordingly:
      takeMVar var >>= return . \case
        DeliverySuccess _ offset -> Right offset
        DeliveryFailure _ err    -> Left err
        NoMessageError err       -> Left err
```

_Note:_ this is a semi-naive solution as this waits forever (or until
librdkafka times out). You should make sure that your configuration reflects
the behavior you want out of this functionality.

## Credits

rdkafka-hs is a fork of [hw-kafka-client](https://github.com/haskell-works/hw-kafka-client)
by Alexey Raga and its contributors, which was itself inspired by
[Haskakafka](https://github.com/cosbynator/haskakafka).
