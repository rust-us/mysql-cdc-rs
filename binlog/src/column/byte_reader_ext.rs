use std::io::Cursor;
use std::any::TypeId;
use byteorder::{ByteOrder, ReadBytesExt, LittleEndian, BigEndian};
use common::err::decode_error::ReError;

/// Extension trait for reading additional integer types from cursors
pub trait ByteReaderExt {
    fn read_u24<T: ByteOrder>(&mut self) -> Result<u32, ReError>;
    fn read_i24<T: ByteOrder>(&mut self) -> Result<i32, ReError>;
    fn read_uint<T: ByteOrder>(&mut self, nbytes: usize) -> Result<u64, ReError>;
}

impl ByteReaderExt for Cursor<&[u8]> {
    fn read_u24<T: ByteOrder>(&mut self) -> Result<u32, ReError> {
        let mut buf = [0u8; 3];
        std::io::Read::read_exact(self, &mut buf)?;
        
        // Check if T is LittleEndian or BigEndian
        let is_little_endian = std::any::TypeId::of::<T>() == std::any::TypeId::of::<LittleEndian>();
        
        let value = if is_little_endian {
            u32::from_le_bytes([buf[0], buf[1], buf[2], 0])
        } else {
            u32::from_be_bytes([0, buf[0], buf[1], buf[2]])
        };
        
        Ok(value)
    }

    fn read_i24<T: ByteOrder>(&mut self) -> Result<i32, ReError> {
        let mut buf = [0u8; 3];
        std::io::Read::read_exact(self, &mut buf)?;
        
        // Check if T is LittleEndian or BigEndian
        let is_little_endian = std::any::TypeId::of::<T>() == std::any::TypeId::of::<LittleEndian>();
        
        let unsigned = if is_little_endian {
            u32::from_le_bytes([buf[0], buf[1], buf[2], 0])
        } else {
            u32::from_be_bytes([0, buf[0], buf[1], buf[2]])
        };
        
        // Sign extend from 24 bits to 32 bits
        let value = if unsigned & 0x800000 != 0 {
            (unsigned | 0xFF000000) as i32
        } else {
            unsigned as i32
        };
        
        Ok(value)
    }

    fn read_uint<T: ByteOrder>(&mut self, nbytes: usize) -> Result<u64, ReError> {
        if nbytes == 0 || nbytes > 8 {
            return Err(ReError::String(format!("Invalid byte count for uint: {}", nbytes)));
        }
        
        let mut buf = [0u8; 8];
        std::io::Read::read_exact(self, &mut buf[..nbytes])?;
        
        // Check if T is LittleEndian or BigEndian
        let is_little_endian = std::any::TypeId::of::<T>() == std::any::TypeId::of::<LittleEndian>();
        
        let value = if is_little_endian {
            u64::from_le_bytes(buf)
        } else {
            // Big endian - shift bytes to the right positions
            let mut be_buf = [0u8; 8];
            be_buf[8-nbytes..].copy_from_slice(&buf[..nbytes]);
            u64::from_be_bytes(be_buf)
        };
        
        Ok(value)
    }
}