{-# LANGUAGE ForeignFunctionInterface #-}

module Kafka.Internal.CallbackOwnerSpec (tests) where

import Control.Concurrent (forkFinally, forkIOWithUnmask, threadDelay)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, readMVar, tryReadMVar)
import Control.Exception (MaskingState (MaskedInterruptible), SomeException, finally, getMaskingState, throwIO, try)
import Control.Monad (void)
import Data.Either (isLeft)
import Data.IORef (IORef, atomicModifyIORef', modifyIORef', newIORef, readIORef)
import Data.Map.Strict qualified as M
import Foreign.Ptr (FunPtr, castFunPtr, freeHaskellFunPtr)
import GHC.Conc (BlockReason (BlockedOnMVar), ThreadStatus (ThreadBlocked), threadStatus)
import Kafka.Internal.CallbackOwner
import System.Timeout (timeout)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, assertFailure, testCase, (@?=))

foreign import ccall "wrapper" wrap :: IO () -> IO (FunPtr (IO ()))

foreign import ccall "dynamic" invoke :: FunPtr (IO ()) -> IO ()

tests :: TestTree
tests =
  testGroup
    "Callback owner"
    [ testCase "bridge allocation executes masked before ownership is recorded" $ do
        freed <- newIORef M.empty
        owner <- newCallbackOwnerWith (countFree freed)
        observed <- newEmptyMVar
        done <- newEmptyMVar
        void $ forkIOWithUnmask $ \unmask -> do
          result <-
            try @SomeException $
              unmask $
                installCallback
                  owner
                  (getMaskingState >>= putMVar observed >> wrap (pure ()))
                  (const (pure ()))
          putMVar done result
        bounded (readMVar done) >>= either throwIO pure
        bounded (readMVar observed) >>= (@?= MaskedInterruptible)
        releaseCallbackOwner owner
        counts <- readIORef freed
        M.size counts @?= 1
        sum counts @?= 1,
      testCase "installed and replaced bridges release exactly once" $ do
        freed <- newIORef M.empty
        owner <- newCallbackOwnerWith (countFree freed)
        a <- wrap (pure ())
        b <- wrap (pure ())
        installCallback owner (pure a) (const (pure ()))
        installCallback owner (pure b) (const (pure ()))
        releaseUnattachedCallbacks owner
        releaseCallbackOwner owner
        readIORef freed >>= (@?= M.fromList [(castFunPtr a, 1), (castFunPtr b, 1)]),
      testCase "failed native install still owns its allocated bridge" $ do
        freed <- newIORef M.empty
        owner <- newCallbackOwnerWith (countFree freed)
        pointer <- wrap (pure ())
        result <- try @SomeException $ installCallback owner (pure pointer) (const (throwIO (userError "install")))
        assertBool (show result) (isLeft result)
        releaseUnattachedCallbacks owner
        readIORef freed >>= (@?= M.singleton (castFunPtr pointer) 1),
      testCase "failed allocation has no release" $ do
        freed <- newIORef M.empty
        owner <- newCallbackOwnerWith (countFree freed)
        result <- try @SomeException $ installCallback owner (throwIO (userError "allocate") :: IO (FunPtr (IO ()))) (const (pure ()))
        assertBool (show result) (isLeft result)
        releaseUnattachedCallbacks owner
        readIORef freed >>= (@?= M.empty),
      testCase "transfer preserves a callable bridge until client release" $ do
        freed <- newIORef M.empty
        called <- newIORef (0 :: Int)
        owner <- newCallbackOwnerWith (countFree freed)
        pointer <- wrap (modifyIORef' called (+ 1))
        installCallback owner (pure pointer) (const (pure ()))
        attachCallbackOwner owner
        releaseUnattachedCallbacks owner
        readIORef freed >>= (@?= M.empty)
        invoke pointer
        readIORef called >>= (@?= 1)
        releaseCallbackOwner owner
        releaseCallbackOwner owner
        readIORef freed >>= (@?= M.singleton (castFunPtr pointer) 1),
      testCase "transferred and released owners reject reuse before allocating" $ do
        owner <- newCallbackOwner
        allocated <- newIORef (0 :: Int)
        attachCallbackOwner owner
        let reject = do
              result <- try @SomeException $ installCallback owner (modifyIORef' allocated (+ 1) >> wrap (pure ())) (const (pure ()))
              assertBool (show result) (isLeft result)
              checked <- try @SomeException (checkCallbackOwner owner)
              assertBool (show checked) (isLeft checked)
              transferred <- try @SomeException (attachCallbackOwner owner)
              assertBool (show transferred) (isLeft transferred)
        reject
        releaseCallbackOwner owner
        reject
        readIORef allocated >>= (@?= 0),
      testCase "release retry does not repeat completed releases" $ do
        freed <- newIORef M.empty
        attempts <- newIORef (0 :: Int)
        owner <- newCallbackOwnerWith $ \pointer -> do
          attempt <- atomicModifyIORef' attempts (\n -> (n + 1, n + 1))
          if attempt == 2 then throwIO (userError "before free") else countFree freed pointer
        a <- wrap (pure ())
        b <- wrap (pure ())
        installCallback owner (pure a) (const (pure ()))
        installCallback owner (pure b) (const (pure ()))
        result <- try @SomeException (releaseCallbackOwner owner)
        assertBool (show result) (isLeft result)
        releaseCallbackOwner owner
        readIORef freed >>= (@?= M.fromList [(castFunPtr a, 1), (castFunPtr b, 1)]),
      testCase "concurrent releases join before returning" $ do
        entered <- newEmptyMVar
        resume <- newEmptyMVar
        freed <- newIORef M.empty
        owner <- newCallbackOwnerWith $ \pointer -> putMVar entered () >> bounded (readMVar resume) >> countFree freed pointer
        pointer <- wrap (pure ())
        installCallback owner (pure pointer) (const (pure ()))
        doneA <- newEmptyMVar
        doneB <- newEmptyMVar
        void $ forkFinally (releaseCallbackOwner owner) (putMVar doneA)
        bounded (readMVar entered)
        flip finally (putMVar resume ()) $ do
          second <- forkFinally (releaseCallbackOwner owner) (putMVar doneB)
          let awaitBlocked =
                threadStatus second >>= \status ->
                  if status == ThreadBlocked BlockedOnMVar then pure () else threadDelay 1000 >> awaitBlocked
          bounded awaitBlocked
          tryReadMVar doneA >>= maybe (pure ()) (const (assertFailure "first release returned before free"))
          tryReadMVar doneB >>= maybe (pure ()) (const (assertFailure "second release returned before free"))
        bounded (readMVar doneA) >>= either throwIO pure
        bounded (readMVar doneB) >>= either throwIO pure
        readIORef freed >>= (@?= M.singleton (castFunPtr pointer) 1)
    ]

countFree :: IORef (M.Map (FunPtr ()) Int) -> FunPtr () -> IO ()
countFree counts pointer = do
  modifyIORef' counts (M.insertWith (+) pointer 1)
  freeHaskellFunPtr pointer

bounded :: IO a -> IO a
bounded action = timeout 5000000 action >>= maybe (assertFailure "owner stage timed out") pure
