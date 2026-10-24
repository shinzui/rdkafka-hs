-- | A client-independent owner for persistent Haskell callback bridges. This
-- owner must never capture the owning client ForeignPtr. Native configuration
-- copies borrow the same bridges; only client teardown releases them.
module Kafka.Internal.CallbackOwner
  ( CallbackOwner,
    newCallbackOwner,
    newCallbackOwnerWith,
    installCallback,
    checkCallbackOwner,
    attachCallbackOwner,
    releaseCallbackOwner,
    releaseUnattachedCallbacks,
    containCallbackException,
    containCallbackResult,
  )
where

import Control.Concurrent (forkIOWithUnmask)
import Control.Concurrent.MVar (MVar, modifyMVarMasked_, newMVar, withMVar)
import Control.Exception (SomeException, catch, mask_, onException, throwIO)
import Control.Monad (void)
import Data.IORef (IORef, modifyIORef', newIORef, readIORef, writeIORef)
import Foreign.Ptr (FunPtr, castFunPtr, freeHaskellFunPtr)

data Phase = Building | Attached | Released

data CallbackOwner = CallbackOwner (MVar Phase) (IORef [FunPtr ()]) (FunPtr () -> IO ())

newCallbackOwner :: IO CallbackOwner
newCallbackOwner = newCallbackOwnerWith freeHaskellFunPtr

-- | The releaser must either throw before freeing, or return after freeing.
-- This internal seam lets tests count actual allocations and releases without
-- adding instrumentation to normal clients.
newCallbackOwnerWith :: (FunPtr () -> IO ()) -> IO CallbackOwner
newCallbackOwnerWith free = CallbackOwner <$> newMVar Building <*> newIORef [] <*> pure free

installCallback :: CallbackOwner -> IO (FunPtr a) -> (FunPtr a -> IO ()) -> IO ()
installCallback owner@(CallbackOwner phase pointers _) allocate install = mask_ $ do
  checkCallbackOwner owner
  pointer <- allocate
  ( withMVar phase $ \case
      Building -> modifyIORef' pointers (castFunPtr pointer :)
      _ -> ioError (userError "callback configuration is no longer mutable")
    )
    `onException` freeHaskellFunPtr pointer
  install pointer

checkCallbackOwner :: CallbackOwner -> IO ()
checkCallbackOwner (CallbackOwner phase _ _) = withMVar phase $ \case
  Building -> pure ()
  _ -> ioError (userError "callback configuration cannot construct another client")

attachCallbackOwner :: CallbackOwner -> IO ()
attachCallbackOwner (CallbackOwner phase _ _) = modifyMVarMasked_ phase $ \case
  Building -> pure Attached
  _ -> ioError (userError "callback owner already transferred or released")

releaseCallbackOwner :: CallbackOwner -> IO ()
releaseCallbackOwner = releaseWhen (const True)

-- | After transfer the client finalizer is the sole releaser. A constructor
-- exception after native creation must not free live callback addresses.
releaseUnattachedCallbacks :: CallbackOwner -> IO ()
releaseUnattachedCallbacks = releaseWhen $ \case
  Building -> True
  _ -> False

releaseWhen :: (Phase -> Bool) -> CallbackOwner -> IO ()
releaseWhen shouldRelease (CallbackOwner phase pointers free) = modifyMVarMasked_ phase $ \current ->
  if shouldRelease current then drain >> pure Released else pure current
  where
    -- Commit each completed release while masked. A later failing releaser
    -- leaves only the remaining pointers for a retry.
    drain =
      readIORef pointers >>= \case
        [] -> pure ()
        pointer : rest -> free pointer >> writeIORef pointers rest >> drain

-- | Report on a fresh Haskell thread, never through a foreign callback frame.
-- Rebalance installers also use this around each user action so native
-- assignment/revocation still runs after a user callback throws.
containCallbackException :: IO () -> IO ()
containCallbackException = containCallbackResult ()

containCallbackResult :: a -> IO a -> IO a
containCallbackResult fallback action =
  action `catch` \(exception :: SomeException) -> do
    void $ forkIOWithUnmask (\unmask -> unmask (throwIO exception))
    pure fallback
