use std::io::Cursor;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use getset::{Getters, Setters};

use common::err::CResult;
use common::file_util;

use crate::storage::file_system::FileSystem;
use crate::storage::storage_config::{SEGMENT_HEADER_SIZE_BYTES, VERSION};

/// segment文件头(不可更改，总大小：64byte).
/// ```txt
/// 4字节：segmentId,
/// 4字节：version,
/// 8字节：第一个entry的index值,
/// 8字节：segment最大容量,
/// 4字节：segment最多存Entry数量
/// 36字节预留空间(用于后续扩展...)
/// ```
#[derive(Debug, Getters, Setters)]
pub(crate) struct SegmentHeader {
    // segmentId
    #[getset(get = "pub")]
    id: u32,

    // version
    #[getset(get = "pub")]
    version: u32,

    // 第一个entry的index值
    #[getset(get = "pub")]
    first_index: u64,

    // segment最大容量（单位：byte）
    #[getset(get = "pub")]
    max_segment_size: u64,

    // segment最大存Entry数量
    #[getset(get = "pub")]
    max_entries: u32,
}

impl SegmentHeader {
    /// 初始化文件头
    pub fn new(file_path: &str,
               id: u32,
               first_index: u64,
               max_segment_size: u64,
               max_entries: u32) -> CResult<Self> {
        let mut bytes_buffer: [u8; SEGMENT_HEADER_SIZE_BYTES] = [0; SEGMENT_HEADER_SIZE_BYTES];
        let mut c = Cursor::new(&mut bytes_buffer[0..]);
        c.write_u32::<LittleEndian>(id)?;
        c.write_u32::<LittleEndian>(VERSION)?;
        c.write_u64::<LittleEndian>(first_index)?;
        c.write_u64::<LittleEndian>(max_segment_size)?;
        c.write_u32::<LittleEndian>(max_entries)?;
        // 初始化
        file_util::update_file_bytes(file_path, 0, &bytes_buffer)?;
        Ok(Self {
            id,
            version: VERSION,
            first_index,
            max_segment_size,
            max_entries,
        })
    }
}

impl FileSystem for SegmentHeader {
    fn from_file(file_path: &str, _start_offset: u64, _len: usize) -> CResult<Self> {
        let file_buffer = file_util::read_file_bytes(file_path, 0, SEGMENT_HEADER_SIZE_BYTES)?;

        let mut cursor = Cursor::new(file_buffer);
        cursor.set_position(0);
        let id = cursor.read_u32::<LittleEndian>()?;

        cursor.set_position(4);
        let version = cursor.read_u32::<LittleEndian>()?;

        cursor.set_position(8);
        let first_index = cursor.read_u64::<LittleEndian>()?;

        cursor.set_position(16);
        let max_segment_size = cursor.read_u64::<LittleEndian>()?;

        cursor.set_position(24);
        let max_entries = cursor.read_u32::<LittleEndian>()?;

        Ok(Self {
            id,
            version,
            first_index,
            max_segment_size,
            max_entries,
        })
    }

    fn flush(&self) -> CResult<()> {
        Ok(())
    }
}