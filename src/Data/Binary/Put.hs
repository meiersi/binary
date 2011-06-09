{-# LANGUAGE CPP, GeneralizedNewtypeDeriving #-}
-- TODO: Implement instances by ourselves

-----------------------------------------------------------------------------
-- |
-- Module      : Data.Binary.Put
-- Copyright   : Lennart Kolmodin
-- License     : BSD3-style (see LICENSE)
-- 
-- Maintainer  : Lennart Kolmodin <kolmodin@dtek.chalmers.se>
-- Stability   : stable
-- Portability : Portable to Hugs and GHC. Requires MPTCs
--
-- The Put monad. A monad for efficiently constructing lazy bytestrings.
--
-----------------------------------------------------------------------------

module Data.Binary.Put (

    -- * The Put type
      Put
    , PutM(..)
    , runPut
    -- , runPutM   -- cannot be supported efficiently
    , putBuilder
    , execPut

    -- * Flushing the implicit parse state
    , flush

    -- * Primitives
    , putWord8
    , putByteString
    , putLazyByteString

    -- * Big-endian primitives
    , putWord16be
    , putWord32be
    , putWord64be

    -- * Little-endian primitives
    , putWord16le
    , putWord32le
    , putWord64le

    -- * Host-endian, unaligned writes
    , putWordhost           -- :: Word   -> Put
    , putWord16host         -- :: Word16 -> Put
    , putWord32host         -- :: Word32 -> Put
    , putWord64host         -- :: Word64 -> Put

  ) where

import qualified Data.Binary.Builder as B
import qualified Data.ByteString.Builder.Internal as B

import Data.Word
import qualified Data.ByteString      as S
import qualified Data.ByteString.Lazy as L

#ifdef APPLICATIVE_IN_BASE
import Control.Applicative
#endif


------------------------------------------------------------------------

-- | The PutM type. A Writer monad over the efficient Builder monoid.
newtype PutM a = Put { unPut :: B.Put a }
  deriving( Functor, Monad )

-- | Put merely lifts Builder into a Writer monad, applied to ().
type Put = PutM ()

{- TODO: Adapt to new construction
#ifdef APPLICATIVE_IN_BASE
instance Applicative PutM where
        pure    = return
        m <*> k = Put $
            let PairS f w  = unPut m
                PairS x w' = unPut k
            in PairS (f x) (w `mappend` w')
#endif
-}

putBuilder :: B.Builder -> Put
putBuilder = Put . B.putBuilder
{-# INLINE putBuilder #-}

-- | Run the 'Put' monad
execPut :: PutM a -> B.Builder
execPut = B.fromPut . unPut
{-# INLINE execPut #-}

-- | Run the 'Put' monad with a serialiser
runPut :: Put -> L.ByteString
runPut = B.toLazyByteString . execPut
{-# INLINE runPut #-}

{- REMOVE: No way to support this efficiently. 

-- | Run the 'Put' monad with a serialiser and get its result
runPutM :: PutM a -> (a, L.ByteString)
runPutM (Put (PairS f s)) = (f, toLazyByteString s)
{-# INLINE runPutM #-}

-}

------------------------------------------------------------------------

-- | Pop the ByteString we have constructed so far, if any, yielding a
-- new chunk in the result ByteString.
flush               :: Put
flush               = putBuilder B.flush
{-# INLINE flush #-}

-- | Efficiently write a byte into the output buffer
putWord8            :: Word8 -> Put
putWord8            = putBuilder . B.singleton
{-# INLINE putWord8 #-}

-- | An efficient primitive to write a strict ByteString into the output buffer.
-- It flushes the current buffer, and writes the argument into a new chunk.
putByteString       :: S.ByteString -> Put
putByteString       = putBuilder . B.fromByteString
{-# INLINE putByteString #-}

-- | Write a lazy ByteString efficiently, simply appending the lazy
-- ByteString chunks to the output buffer
putLazyByteString   :: L.ByteString -> Put
putLazyByteString   = putBuilder . B.fromLazyByteString
{-# INLINE putLazyByteString #-}

-- | Write a Word16 in big endian format
putWord16be         :: Word16 -> Put
putWord16be         = putBuilder . B.putWord16be
{-# INLINE putWord16be #-}

-- | Write a Word16 in little endian format
putWord16le         :: Word16 -> Put
putWord16le         = putBuilder . B.putWord16le
{-# INLINE putWord16le #-}

-- | Write a Word32 in big endian format
putWord32be         :: Word32 -> Put
putWord32be         = putBuilder . B.putWord32be
{-# INLINE putWord32be #-}

-- | Write a Word32 in little endian format
putWord32le         :: Word32 -> Put
putWord32le         = putBuilder . B.putWord32le
{-# INLINE putWord32le #-}

-- | Write a Word64 in big endian format
putWord64be         :: Word64 -> Put
putWord64be         = putBuilder . B.putWord64be
{-# INLINE putWord64be #-}

-- | Write a Word64 in little endian format
putWord64le         :: Word64 -> Put
putWord64le         = putBuilder . B.putWord64le
{-# INLINE putWord64le #-}

------------------------------------------------------------------------

-- | /O(1)./ Write a single native machine word. The word is
-- written in host order, host endian form, for the machine you're on.
-- On a 64 bit machine the Word is an 8 byte value, on a 32 bit machine,
-- 4 bytes. Values written this way are not portable to
-- different endian or word sized machines, without conversion.
--
putWordhost         :: Word -> Put
putWordhost         = putBuilder . B.putWordhost
{-# INLINE putWordhost #-}

-- | /O(1)./ Write a Word16 in native host order and host endianness.
-- For portability issues see @putWordhost@.
putWord16host       :: Word16 -> Put
putWord16host       = putBuilder . B.putWord16host
{-# INLINE putWord16host #-}

-- | /O(1)./ Write a Word32 in native host order and host endianness.
-- For portability issues see @putWordhost@.
putWord32host       :: Word32 -> Put
putWord32host       = putBuilder . B.putWord32host
{-# INLINE putWord32host #-}

-- | /O(1)./ Write a Word64 in native host order
-- On a 32 bit machine we write two host order Word32s, in big endian form.
-- For portability issues see @putWordhost@.
putWord64host       :: Word64 -> Put
putWord64host       = putBuilder . B.putWord64host
{-# INLINE putWord64host #-}
