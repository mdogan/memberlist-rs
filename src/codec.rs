// Copyright (c) 2026 Mehmet Dogan
// SPDX-License-Identifier: MPL-2.0

//! A small msgpack codec that writes the same bytes as the Go implementation.
//!
//! The Go code uses `go-msgpack` with a default `MsgpackHandle`. That handle
//! has a few rules that general purpose msgpack libraries do not follow, so we
//! encode by hand:
//!
//! - A struct is a map from field name to value, with the fields sorted by
//!   name.
//! - Strings and byte slices both use the old "raw" family (fixstr, str16,
//!   str32). str8 and the bin family are never written.
//! - Unsigned values use the uint family, signed values (Go `int`) use the
//!   int family, even when they are positive.
//! - An empty byte slice is written as nil.
//!
//! Decoding is more relaxed and accepts every msgpack form of a value.

use std::fmt;

const NIL: u8 = 0xc0;
const FALSE: u8 = 0xc2;
const TRUE: u8 = 0xc3;
const BIN8: u8 = 0xc4;
const BIN16: u8 = 0xc5;
const BIN32: u8 = 0xc6;
const EXT8: u8 = 0xc7;
const EXT16: u8 = 0xc8;
const EXT32: u8 = 0xc9;
const FLOAT32: u8 = 0xca;
const FLOAT64: u8 = 0xcb;
const UINT8: u8 = 0xcc;
const UINT16: u8 = 0xcd;
const UINT32: u8 = 0xce;
const UINT64: u8 = 0xcf;
const INT8: u8 = 0xd0;
const INT16: u8 = 0xd1;
const INT32: u8 = 0xd2;
const INT64: u8 = 0xd3;
const FIXEXT1: u8 = 0xd4;
const FIXEXT2: u8 = 0xd5;
const FIXEXT4: u8 = 0xd6;
const FIXEXT8: u8 = 0xd7;
const FIXEXT16: u8 = 0xd8;
const STR8: u8 = 0xd9;
const STR16: u8 = 0xda;
const STR32: u8 = 0xdb;
const ARRAY16: u8 = 0xdc;
const ARRAY32: u8 = 0xdd;
const MAP16: u8 = 0xde;
const MAP32: u8 = 0xdf;

/// The reason a value could not be decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DecodeError {
    /// The input ended in the middle of a value. Holds the least number of
    /// extra bytes that are needed to make progress.
    Incomplete(usize),
    /// The input is not what we expected.
    Invalid(String),
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DecodeError::Incomplete(_) => write!(f, "unexpected EOF"),
            DecodeError::Invalid(msg) => write!(f, "msgpack decode error: {msg}"),
        }
    }
}

fn invalid<T>(msg: impl Into<String>) -> Result<T, DecodeError> {
    Err(DecodeError::Invalid(msg.into()))
}

pub(crate) fn write_nil(out: &mut Vec<u8>) {
    out.push(NIL);
}

pub(crate) fn write_bool(out: &mut Vec<u8>, v: bool) {
    out.push(if v { TRUE } else { FALSE });
}

/// Writes an unsigned integer the way Go's `EncodeUint` does.
pub(crate) fn write_uint(out: &mut Vec<u8>, v: u64) {
    if v <= i8::MAX as u64 {
        out.push(v as u8);
    } else if v <= u8::MAX as u64 {
        out.extend_from_slice(&[UINT8, v as u8]);
    } else if v <= u16::MAX as u64 {
        out.push(UINT16);
        out.extend_from_slice(&(v as u16).to_be_bytes());
    } else if v <= u32::MAX as u64 {
        out.push(UINT32);
        out.extend_from_slice(&(v as u32).to_be_bytes());
    } else {
        out.push(UINT64);
        out.extend_from_slice(&v.to_be_bytes());
    }
}

/// Writes a signed integer the way Go's `EncodeInt` does.
pub(crate) fn write_int(out: &mut Vec<u8>, v: i64) {
    if v > i8::MAX as i64 {
        if v <= i16::MAX as i64 {
            out.push(INT16);
            out.extend_from_slice(&(v as i16).to_be_bytes());
        } else if v <= i32::MAX as i64 {
            out.push(INT32);
            out.extend_from_slice(&(v as i32).to_be_bytes());
        } else {
            out.push(INT64);
            out.extend_from_slice(&v.to_be_bytes());
        }
    } else if v >= -32 {
        out.push(v as u8);
    } else if v >= i8::MIN as i64 {
        out.extend_from_slice(&[INT8, v as u8]);
    } else if v >= i16::MIN as i64 {
        out.push(INT16);
        out.extend_from_slice(&(v as i16).to_be_bytes());
    } else if v >= i32::MIN as i64 {
        out.push(INT32);
        out.extend_from_slice(&(v as i32).to_be_bytes());
    } else {
        out.push(INT64);
        out.extend_from_slice(&v.to_be_bytes());
    }
}

/// Writes a string or a non-empty byte slice in the old raw family.
pub(crate) fn write_raw(out: &mut Vec<u8>, v: &[u8]) {
    let len = v.len();
    if len < 32 {
        out.push(0xa0 | len as u8);
    } else if len < 65536 {
        out.push(STR16);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        out.push(STR32);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    }
    out.extend_from_slice(v);
}

pub(crate) fn write_map_len(out: &mut Vec<u8>, len: usize) {
    if len < 16 {
        out.push(0x80 | len as u8);
    } else if len < 65536 {
        out.push(MAP16);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        out.push(MAP32);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    }
}

/// Reads msgpack values from a byte slice.
pub(crate) struct Decoder<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Decoder<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        Decoder { buf, pos: 0 }
    }

    /// The number of bytes consumed so far.
    pub(crate) fn position(&self) -> usize {
        self.pos
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        let remaining = self.buf.len() - self.pos;
        if remaining < n {
            return Err(DecodeError::Incomplete(n - remaining));
        }
        let out = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }

    fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, DecodeError> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }

    fn u32(&mut self) -> Result<u32, DecodeError> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64, DecodeError> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }

    /// Reads any integer as a 128 bit value, so the caller can range check it.
    /// Nil reads as zero, like a Go decode into an integer field.
    fn integer(&mut self) -> Result<i128, DecodeError> {
        let b = self.u8()?;
        Ok(match b {
            0x00..=0x7f => b as i128,
            0xe0..=0xff => (b as i8) as i128,
            NIL => 0,
            UINT8 => self.u8()? as i128,
            UINT16 => self.u16()? as i128,
            UINT32 => self.u32()? as i128,
            UINT64 => self.u64()? as i128,
            INT8 => (self.u8()? as i8) as i128,
            INT16 => (self.u16()? as i16) as i128,
            INT32 => (self.u32()? as i32) as i128,
            INT64 => (self.u64()? as i64) as i128,
            _ => {
                return invalid(format!(
                    "expected an integer, found descriptor byte {b:#04x}"
                ));
            }
        })
    }

    pub(crate) fn read_uint(&mut self, max: u64) -> Result<u64, DecodeError> {
        let v = self.integer()?;
        if v < 0 || v > max as i128 {
            return invalid(format!("integer {v} is out of range (max {max})"));
        }
        Ok(v as u64)
    }

    pub(crate) fn read_int(&mut self) -> Result<i64, DecodeError> {
        let v = self.integer()?;
        if v > i64::MAX as i128 {
            return invalid(format!("integer {v} overflows a signed 64 bit value"));
        }
        Ok(v as i64)
    }

    pub(crate) fn read_bool(&mut self) -> Result<bool, DecodeError> {
        match self.u8()? {
            TRUE => Ok(true),
            FALSE | NIL => Ok(false),
            b => invalid(format!("expected a bool, found descriptor byte {b:#04x}")),
        }
    }

    /// Reads a string or a byte slice. Nil reads as empty.
    pub(crate) fn read_bytes(&mut self) -> Result<&'a [u8], DecodeError> {
        let b = self.u8()?;
        let len = match b {
            0xa0..=0xbf => (b & 0x1f) as usize,
            NIL => 0,
            STR8 | BIN8 => self.u8()? as usize,
            STR16 | BIN16 => self.u16()? as usize,
            STR32 | BIN32 => self.u32()? as usize,
            _ => {
                return invalid(format!(
                    "expected bytes or a string, found descriptor byte {b:#04x}"
                ));
            }
        };
        self.take(len)
    }

    /// Reads a map header. Nil reads as an empty map.
    pub(crate) fn read_map_len(&mut self) -> Result<usize, DecodeError> {
        let b = self.u8()?;
        Ok(match b {
            0x80..=0x8f => (b & 0x0f) as usize,
            NIL => 0,
            MAP16 => self.u16()? as usize,
            MAP32 => self.u32()? as usize,
            _ => return invalid(format!("expected a map, found descriptor byte {b:#04x}")),
        })
    }

    /// Skips one value of any type. This is used for unknown struct fields.
    ///
    /// It keeps a count of pending values instead of recursing, so that a
    /// deeply nested input cannot overflow the stack.
    pub(crate) fn skip(&mut self) -> Result<(), DecodeError> {
        let mut pending: u64 = 1;
        while pending > 0 {
            pending -= 1;
            let b = self.u8()?;
            match b {
                0x00..=0x7f | 0xe0..=0xff | NIL | FALSE | TRUE => {}
                0x80..=0x8f => pending += 2 * (b & 0x0f) as u64,
                0x90..=0x9f => pending += (b & 0x0f) as u64,
                0xa0..=0xbf => {
                    self.take((b & 0x1f) as usize)?;
                }
                STR8 | BIN8 => {
                    let n = self.u8()? as usize;
                    self.take(n)?;
                }
                STR16 | BIN16 => {
                    let n = self.u16()? as usize;
                    self.take(n)?;
                }
                STR32 | BIN32 => {
                    let n = self.u32()? as usize;
                    self.take(n)?;
                }
                UINT8 | INT8 => {
                    self.take(1)?;
                }
                UINT16 | INT16 => {
                    self.take(2)?;
                }
                UINT32 | INT32 | FLOAT32 => {
                    self.take(4)?;
                }
                UINT64 | INT64 | FLOAT64 => {
                    self.take(8)?;
                }
                FIXEXT1 => {
                    self.take(2)?;
                }
                FIXEXT2 => {
                    self.take(3)?;
                }
                FIXEXT4 => {
                    self.take(5)?;
                }
                FIXEXT8 => {
                    self.take(9)?;
                }
                FIXEXT16 => {
                    self.take(17)?;
                }
                EXT8 => {
                    let n = self.u8()? as usize;
                    self.take(n + 1)?;
                }
                EXT16 => {
                    let n = self.u16()? as usize;
                    self.take(n + 1)?;
                }
                EXT32 => {
                    let n = self.u32()? as usize;
                    self.take(n + 1)?;
                }
                ARRAY16 => pending += self.u16()? as u64,
                ARRAY32 => pending += self.u32()? as u64,
                MAP16 => pending += 2 * self.u16()? as u64,
                MAP32 => pending += 2 * self.u32()? as u64,
                0xc1 => return invalid("descriptor byte 0xc1 is never used"),
            }
        }
        Ok(())
    }
}

/// A value that can be a field of a wire message.
pub(crate) trait Value: Sized + Default {
    fn encode(&self, out: &mut Vec<u8>);
    fn decode(d: &mut Decoder<'_>) -> Result<Self, DecodeError>;
    /// True if the value is the zero value, used for `omitempty` fields.
    fn is_empty(&self) -> bool;
}

macro_rules! uint_value {
    ($($ty:ty),*) => {$(
        impl Value for $ty {
            fn encode(&self, out: &mut Vec<u8>) {
                write_uint(out, *self as u64);
            }
            fn decode(d: &mut Decoder<'_>) -> Result<Self, DecodeError> {
                Ok(d.read_uint(<$ty>::MAX as u64)? as $ty)
            }
            fn is_empty(&self) -> bool {
                *self == 0
            }
        }
    )*};
}
uint_value!(u8, u16, u32);

/// A Go `int`.
impl Value for i64 {
    fn encode(&self, out: &mut Vec<u8>) {
        write_int(out, *self);
    }
    fn decode(d: &mut Decoder<'_>) -> Result<Self, DecodeError> {
        d.read_int()
    }
    fn is_empty(&self) -> bool {
        *self == 0
    }
}

impl Value for bool {
    fn encode(&self, out: &mut Vec<u8>) {
        write_bool(out, *self);
    }
    fn decode(d: &mut Decoder<'_>) -> Result<Self, DecodeError> {
        d.read_bool()
    }
    fn is_empty(&self) -> bool {
        !*self
    }
}

impl Value for String {
    fn encode(&self, out: &mut Vec<u8>) {
        write_raw(out, self.as_bytes());
    }
    fn decode(d: &mut Decoder<'_>) -> Result<Self, DecodeError> {
        match std::str::from_utf8(d.read_bytes()?) {
            Ok(s) => Ok(s.to_owned()),
            Err(_) => invalid("string is not valid UTF-8"),
        }
    }
    fn is_empty(&self) -> bool {
        String::is_empty(self)
    }
}

/// A Go `[]byte`. Go writes a nil slice as nil, and we treat an empty vector
/// as a nil slice.
impl Value for Vec<u8> {
    fn encode(&self, out: &mut Vec<u8>) {
        if self.is_empty() {
            write_nil(out);
        } else {
            write_raw(out, self);
        }
    }
    fn decode(d: &mut Decoder<'_>) -> Result<Self, DecodeError> {
        Ok(d.read_bytes()?.to_vec())
    }
    fn is_empty(&self) -> bool {
        Vec::is_empty(self)
    }
}

/// A wire message: a struct that is encoded as a msgpack map.
pub(crate) trait Message: Sized {
    fn encode_to(&self, out: &mut Vec<u8>);
    fn decode_from(d: &mut Decoder<'_>) -> Result<Self, DecodeError>;
}

/// Decodes one message from the start of a byte slice. Extra bytes after the
/// message are ignored, like the Go decoder does.
pub(crate) fn decode<T: Message>(buf: &[u8]) -> Result<T, DecodeError> {
    T::decode_from(&mut Decoder::new(buf))
}

/// Defines a wire message struct.
///
/// Each field has its Go field name, which is the key on the wire. The fields
/// must be listed in the byte order of those names, because that is the order
/// the Go encoder writes them in. A field marked `omitempty` is left out when
/// it has its zero value.
macro_rules! message {
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident {
            $(
                $(#[$fmeta:meta])*
                $fvis:vis $field:ident : $ty:ty = $key:literal $(, $omit:ident)? ;
            )*
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Default, PartialEq, Eq)]
        $vis struct $name {
            $( $(#[$fmeta])* $fvis $field: $ty, )*
        }

        impl $name {
            /// The wire keys of this message, in encoding order.
            #[allow(dead_code)]
            pub(crate) const KEYS: &'static [&'static str] = &[$($key),*];
        }

        impl $crate::codec::Message for $name {
            fn encode_to(&self, out: &mut Vec<u8>) {
                use $crate::codec::Value;
                let mut len = 0usize;
                $(
                    if !(message!(@omit $($omit)?) && Value::is_empty(&self.$field)) {
                        len += 1;
                    }
                )*
                $crate::codec::write_map_len(out, len);
                $(
                    if !(message!(@omit $($omit)?) && Value::is_empty(&self.$field)) {
                        $crate::codec::write_raw(out, $key.as_bytes());
                        Value::encode(&self.$field, out);
                    }
                )*
            }

            fn decode_from(
                d: &mut $crate::codec::Decoder<'_>,
            ) -> Result<Self, $crate::codec::DecodeError> {
                let mut out = <$name>::default();
                let len = d.read_map_len()?;
                for _ in 0..len {
                    let key = d.read_bytes()?;
                    $(
                        if key == $key.as_bytes() {
                            out.$field = $crate::codec::Value::decode(d)?;
                            continue;
                        }
                    )*
                    d.skip()?;
                }
                Ok(out)
            }
        }
    };
    (@omit omitempty) => { true };
    (@omit) => { false };
}
pub(crate) use message;
