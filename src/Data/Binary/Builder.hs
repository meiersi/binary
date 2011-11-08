{-# LANGUAGE CPP, BangPatterns #-}
#if __GLASGOW_HASKELL__ >= 701
{-# LANGUAGE Safe #-}
#endif
-----------------------------------------------------------------------------
-- |
-- Module      : Data.Binary.Builder
-- Copyright   : Lennart Kolmodin, Ross Paterson, Simon Meier
-- License     : BSD3-style (see LICENSE)
-- 
-- Maintainer  : Lennart Kolmodin <kolmodin@dtek.chalmers.se>
-- Stability   : experimental
-- Portability : portable to Hugs and GHC
--
-- Efficient construction of lazy bytestrings.
--
-----------------------------------------------------------------------------

module Data.Binary.Builder (

    -- * The Builder type
      Builder
    , toLazyByteString

    -- * Constructing Builders
    , empty
    , singleton
    , append
    , fromByteString        -- :: S.ByteString -> Builder
    , fromLazyByteString    -- :: L.ByteString -> Builder

    -- * Flushing the buffer state
    , flush

    -- * Derived Builders
    -- ** Big-endian writes
    , putWord16be           -- :: Word16 -> Builder
    , putWord32be           -- :: Word32 -> Builder
    , putWord64be           -- :: Word64 -> Builder

    -- ** Little-endian writes
    , putWord16le           -- :: Word16 -> Builder
    , putWord32le           -- :: Word32 -> Builder
    , putWord64le           -- :: Word64 -> Builder

    -- ** Host-endian, unaligned writes
    , putWordhost           -- :: Word -> Builder
    , putWord16host         -- :: Word16 -> Builder
    , putWord32host         -- :: Word32 -> Builder
    , putWord64host         -- :: Word64 -> Builder

      -- ** Unicode
    , putCharUtf8

  ) where

import Data.Monoid

import qualified Data.ByteString      as S
import qualified Data.ByteString.Lazy as L

import Data.ByteString.Lazy.Builder
import Data.ByteString.Lazy.Builder.Extras


import Foreign

-- | /O(1)./ The empty Builder, satisfying
--
--  * @'toLazyByteString' 'empty' = 'L.empty'@
--
{-# INLINE empty #-}
empty :: Builder
empty = mempty

-- | /O(1)./ A Builder taking a single byte, satisfying
--
--  * @'toLazyByteString' ('singleton' b) = 'L.singleton' b@
--
{-# INLINE singleton #-}
singleton :: Word8 -> Builder
singleton = word8

-- | /O(1)./ The concatenation of two Builders, an associative operation
-- with identity 'empty', satisfying
--
--  * @'toLazyByteString' ('append' x y) = 'L.append' ('toLazyByteString' x) ('toLazyByteString' y)@
--
{-# INLINE append #-}
append :: Builder -> Builder -> Builder
append = mappend

-- | /O(1)./ A Builder taking a 'S.ByteString', satisfying
--
--  * @'toLazyByteString' ('fromByteString' bs) = 'L.fromChunks' [bs]@
--
{-# INLINE fromByteString #-}
fromByteString :: S.ByteString -> Builder
fromByteString = byteString

-- | /O(1)./ A Builder taking a lazy 'L.ByteString', satisfying
--
--  * @'toLazyByteString' ('fromLazyByteString' bs) = bs@
--
{-# INLINE fromLazyByteString #-}
fromLazyByteString :: L.ByteString -> Builder
fromLazyByteString = lazyByteString

-- | Write a Word16 in big endian format
{-# INLINE putWord16be #-}
putWord16be :: Word16 -> Builder
putWord16be = word16BE

-- | Write a Word32 in big endian format
{-# INLINE putWord32be #-}
putWord32be :: Word32 -> Builder
putWord32be = word32BE

-- | Write a Word64 in big endian format
{-# INLINE putWord64be #-}
putWord64be :: Word64 -> Builder
putWord64be = word64BE

-- | Write a Word16 in little endian format
{-# INLINE putWord16le #-}
putWord16le :: Word16 -> Builder
putWord16le = word16LE

-- | Write a Word32 in little endian format
{-# INLINE putWord32le #-}
putWord32le :: Word32 -> Builder
putWord32le = word32LE

-- | Write a Word64 in little endian format
{-# INLINE putWord64le #-}
putWord64le :: Word64 -> Builder
putWord64le = word64LE

-- | /O(1)./ A Builder taking a single native machine word. The word is
-- written in host order, host endian form, for the machine you're on.
-- On a 64 bit machine the Word is an 8 byte value, on a 32 bit machine,
-- 4 bytes. Values written this way are not portable to
-- different endian or word sized machines, without conversion.
--
{-# INLINE putWordhost #-}
putWordhost :: Word -> Builder
putWordhost = wordHost

-- | Write a Word16 in native host order and host endianness.
-- 2 bytes will be written, unaligned.
{-# INLINE putWord16host #-}
putWord16host :: Word16 -> Builder
putWord16host = word16Host

-- | Write a Word32 in native host order and host endianness.
-- 4 bytes will be written, unaligned.
{-# INLINE putWord32host #-}
putWord32host :: Word32 -> Builder
putWord32host = word32Host

-- | Write a Word64 in native host order.
-- On a 32 bit machine we write two host order Word32s, in big endian form.
-- 8 bytes will be written, unaligned.
{-# INLINE putWord64host #-}
putWord64host :: Word64 -> Builder
putWord64host = word64Host

-- | Write a character using UTF-8 encoding.
{-# INLINE putCharUtf8 #-}
putCharUtf8 :: Char -> Builder
putCharUtf8 = charUtf8
