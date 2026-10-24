module Main (main) where

import ConsumerExample
import Data.Text qualified as Text
import Kafka.Consumer qualified as C
import Kafka.Producer qualified as P
import ProducerExample
import System.Environment (getArgs)
import System.Exit (die)

main :: IO ()
main = do
  args <- getArgs
  case args of
    [] -> run runProducerExample runConsumerExample
    [broker, topic, group] ->
      run
        (runProducerExampleWith (producerProps <> P.brokersList [P.BrokerAddress (Text.pack broker)]) (P.TopicName (Text.pack topic)))
        (runConsumerExampleWith (consumerProps <> C.brokersList [C.BrokerAddress (Text.pack broker)] <> C.groupId (C.ConsumerGroupId (Text.pack group))) (C.topics [C.TopicName (Text.pack topic)] <> C.offsetReset C.Earliest))
    _ -> die "Usage: kafka-client-example [BROKER TOPIC GROUP]"
  where
    run :: IO () -> IO () -> IO ()
    run producer consumer = do
      putStrLn "Running producer example..."
      producer
      putStrLn "Running consumer example..."
      consumer
      putStrLn "Ok."
