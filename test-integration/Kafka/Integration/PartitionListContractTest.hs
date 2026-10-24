module Kafka.Integration.PartitionListContractTest (tests) where

import Control.Concurrent (forkFinally, forkIOWithUnmask, killThread, threadDelay)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, readMVar, tryReadMVar)
import Control.Exception (AsyncException (ThreadKilled), IOException, SomeException, bracket, bracket_, finally, fromException, throwIO, try)
import Control.Monad (replicateM_)
import Data.Either (isLeft)
import Data.Map.Strict qualified as Map
import Data.Text qualified as Text
import Foreign.C.Types (CInt (..))
import GHC.Conc (BlockReason (BlockedOnException), ThreadStatus (ThreadBlocked), threadStatus)
import GHC.IO.Encoding (TextEncoding, getForeignEncoding, latin1, setForeignEncoding, utf8)
import Kafka.Consumer
import Kafka.TestEnv
import System.Mem (performGC)
import System.Timeout (timeout)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit

foreign import ccall unsafe "rdkafka_hs_test_lists_begin" begin :: CInt -> IO ()

foreign import ccall unsafe "rdkafka_hs_test_lists_end" end :: IO ()

foreign import ccall unsafe "rdkafka_hs_test_lists_release" release :: IO ()

foreign import ccall unsafe "rdkafka_hs_test_lists_stat" stat :: CInt -> IO CInt

foreign import ccall unsafe "rdkafka_hs_test_lists_control" control :: CInt -> IO ()

withLists :: Int -> IO a -> IO a
withLists mode = bracket_ (begin (fromIntegral mode)) (release >> performGC >> threadDelay 10000 >> end)

counts :: IO (Int, Int, Int)
counts = (,,) <$> value 0 <*> value 1 <*> value 2
  where
    value i = fromIntegral <$> stat i

balanced :: Int -> IO ()
balanced n = counts >>= (@?= (n, n, 0))

encoding :: TextEncoding -> IO a -> IO a
encoding selected action = bracket getForeignEncoding setForeignEncoding $ \_ -> setForeignEncoding selected >> action

tests :: TestEnv -> TestTree
tests env =
  testGroup "partition-list-ownership" $
    [ testCase "counter detects a retained native list before explicit release" $ withLists 0 $ do
        control 0
        flip finally (control 1) (counts >>= (@?= (1, 0, 1)))
        balanced 1
    ]
      <> [ testGroup
             label
             [ testCase "pair conversion failure releases the partial list before GC" $ fixture False "pair-encoding" $ \_ c -> withLists 0 $ do
                 result <- try @IOException $ encoding latin1 $ position c [("ascii", PartitionId 0), ("λ", PartitionId 0)]
                 assertBool (show result) (isLeft result)
                 balanced 1,
               testCase "offset conversion failure releases the partial list before GC" $ fixture False "offset-encoding" $ \_ c -> withLists 0 $ do
                 result <- try @IOException $ encoding latin1 $ storeOffsets c [part "ascii", part "λ"]
                 assertBool (show result) (isLeft result)
                 balanced 1,
               testCase "ordinary input lists release after exact native results" $ fixture False "input-success" $ \topic c -> withLists 0 $ do
                 replicateM_ 3 $ position c [(topic, PartitionId 0)] >>= (@?= Right [TopicPartition topic (PartitionId 0) PartitionOffsetInvalid])
                 balanced 3
                 assign c [part topic] >>= (@?= Nothing)
                 balanced (if mode == CallbackPollModeAsync then 5 else 4),
               testCase "assignment outputs release after exact decoding" $ fixture False "assignment-success" $ \topic c -> do
                 assign c [part topic] >>= (@?= Nothing)
                 withLists 0 $ do
                   replicateM_ 3 $ assignment c >>= (@?= Right (Map.singleton topic [PartitionId 0]))
                   balanced 3,
               testCase "subscription outputs release after exact decoding" $ fixture True "subscription-success" $ \topic c -> withLists 0 $ do
                 replicateM_ 3 $ subscription c >>= (@?= Right [(topic, SubscribedPartitionsAll)])
                 balanced 3,
               testCase "assignment decoder failure releases the owned output" $ fixture False "assignment-decode" $ \topic c -> do
                 assign c [part topic] >>= (@?= Nothing)
                 withLists 2 $ do
                   result <- try @IOException $ encoding utf8 $ assignment c
                   assertBool (show result) (isLeft result)
                   balanced 1,
               testCase "subscription decoder failure releases the owned output" $ fixture True "subscription-decode" $ \_ c -> withLists 3 $ do
                 result <- try @IOException $ encoding utf8 $ subscription c
                 assertBool (show result) (isLeft result)
                 balanced 1,
               testCase "cancellation at input allocation releases the owned list" $ fixture False "input-cancel" $ \topic c -> withLists 4 $ cancelled (position c [(topic, PartitionId 0)]),
               testCase "cancellation at assignment return releases the owned output" $ fixture False "output-cancel" $ \topic c -> do
                 assign c [part topic] >>= (@?= Nothing)
                 withLists 5 $ cancelled (assignment c)
             ]
         | (label, mode) <- [("async", CallbackPollModeAsync), ("sync", CallbackPollModeSync)],
           let fixture subscribed name action = withTopic env (Text.pack ("lists-" <> label <> "-" <> name)) 1 $ \topic _ ->
                 let props = brokersList (teBrokers env) <> groupId (makeGroupId env (Text.pack ("lists-" <> label <> "-" <> name))) <> noAutoCommit <> callbackPollMode mode
                     create = if subscribed then newConsumer props (topics [topic] <> offsetReset Earliest) else newAssignedConsumer props
                  in bracket (create >>= either throwIO pure) releaseConsumer (action topic)
         ]
  where
    part topic = TopicPartition topic (PartitionId 0) (PartitionOffset 0)

bounded :: IO a -> IO a
bounded action = timeout 10000000 action >>= maybe (assertFailure "partition-list boundary timed out") pure

cancelled :: IO a -> IO ()
cancelled action = do
  done <- newEmptyMVar
  worker <- forkIOWithUnmask $ \unmask -> try @SomeException (unmask action) >>= putMVar done
  flip finally (release >> bounded (readMVar done)) $ do
    let awaitBoundary = stat 3 >>= \n -> if n > 0 then pure () else threadDelay 1000 >> awaitBoundary
    bounded awaitBoundary
    counts >>= (@?= (1, 0, 1))
    killed <- newEmptyMVar
    killer <- forkFinally (killThread worker) (putMVar killed)
    let awaitPending = threadStatus killer >>= \s -> if s == ThreadBlocked BlockedOnException then pure () else threadDelay 1000 >> awaitPending
    bounded awaitPending
    tryReadMVar done >>= maybe (pure ()) (const (assertFailure "operation returned while allocation was held"))
    release
    bounded (readMVar done) >>= \case
      Left err -> (fromException err :: Maybe AsyncException) @?= Just ThreadKilled
      Right _ -> assertFailure "pending cancellation was not delivered"
    bounded (readMVar killed) >>= either throwIO pure
    balanced 1
