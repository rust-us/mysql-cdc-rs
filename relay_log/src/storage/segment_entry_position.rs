use std::fmt::{Debug, Formatter};
use std::fs::OpenOptions;
use std::io::{Cursor, Write};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use memmap2::{MmapMut, MmapOptions};
use tracing::error;

use common::err::CResult;
use common::file_util;

use crate::storage::file_system::FileSystem;
use crate::storage::storage_config::SEGMENT_HEADER_SIZE_BYTES;

/// entry位置信息.
///
/// 固定字节大小 = 4 + max_entries * 8
pub struct SegmentEntryPosition {
    // 当前Segment中Entry数量
    entry_count: u32,
    // 当前Segment中Entry的位置信息
    position_info: Vec<u64>,
    // 文件缓存
    file_buffer: MmapMut,
}

impl SegmentEntryPosition {
    /// 初始化
    pub fn new(file_path: &str, max_entries: u32) -> CResult<Self> {
        let entry_count: u32 = 0;
        let mut position_info: Vec<u64> = vec![0; max_entries as usize];

        let len = 4 + max_entries * 8;
        let bytes_buffer: Vec<u8> = vec![0; len as usize];

        let start_offset = SEGMENT_HEADER_SIZE_BYTES as u64;
        // 文件初始化
        file_util::update_file_bytes(file_path, start_offset, &bytes_buffer)?;

        // 打开文件内存映射
        let mmap = Self::open_file_mem_map(file_path, start_offset, len as usize)?;

        Ok(Self {
            entry_count,
            position_info,
            file_buffer: mmap,
        })
    }

    /// 打开文件内存映射（可读可写）
    fn open_file_mem_map(file_path: &str, start_offset: u64, len: usize) -> CResult<MmapMut> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .append(false)
            .open(file_path)?;
        return unsafe {
            Ok(MmapOptions::new().offset(start_offset).len(len).map_mut(&file)?)
        };
    }

    /// 添加entry位置信息
    pub fn add_position(&mut self, offset: usize, position: u64) -> CResult<()> {
        match self.update_file_position_info(offset, position) {
            Ok(_) => {
                self.entry_count += 1;
                self.position_info[offset] = position;
                match self.update_file_entry_count() {
                    Ok(_) => {
                        Ok(())
                    }
                    Err(e) => {
                        self.entry_count -= 1;
                        self.position_info[offset] = 0;

                        error!("update segment file err: {:?}", e);
                        Err(e)
                    }
                }
            }
            Err(e) => {
                Err(e)
            }
        }
    }

    /// 更新文件中entry数量统计
    fn update_file_entry_count(&mut self) -> CResult<()> {
        (&mut self.file_buffer[0..4]).write_u32::<LittleEndian>(self.entry_count)?;
        Ok(())
    }

    /// 更新文件中entry位置信息
    fn update_file_position_info(&mut self, offset: usize, position: u64) -> CResult<()> {
        let pos_start = 4 + (offset * 8);
        let pos_end = pos_start + 8;
        (&mut self.file_buffer[pos_start..pos_end]).write_u64::<LittleEndian>(position)?;
        Ok(())
    }

    /// 返回entry位置信息(内存)
    pub fn get_position(&self, offset: usize) -> u64 {
        self.position_info[offset]
    }

    /// 返回entry位置信息(磁盘)
    pub fn get_position_disk(&self, offset: usize) -> CResult<u64> {
        let pos_start = 4 + (offset * 8);
        let pos_end = pos_start + 8;
        let pos = (&self.file_buffer[pos_start..pos_end]).read_u64::<LittleEndian>()?;
        Ok(pos)
    }

    /// 返回entry数量(内存)
    pub fn get_entry_count(&self) -> u32 {
        self.entry_count
    }

    /// 返回entry数量(磁盘)
    pub fn get_entry_count_disk(&self) -> CResult<u32> {
        let count = (&self.file_buffer[0..4]).read_u32::<LittleEndian>()?;
        Ok(count)
    }
}

impl FileSystem for SegmentEntryPosition {
    fn from_file(file_path: &str, start_offset: u64, len: usize) -> CResult<Self> {
        let mmap = Self::open_file_mem_map(file_path, start_offset, len)?;

        let bytes_buffer = &mmap[0..];
        let mut cusor = Cursor::new(bytes_buffer);

        cusor.set_position(0);
        let entry_count = cusor.read_u32::<LittleEndian>()?;

        cusor.set_position(4);
        let max_entries = (len - 4) / 8;
        let mut position_info: Vec<u64> = vec![0; max_entries];
        cusor.read_u64_into::<LittleEndian>(&mut position_info)?;
        Ok(Self {
            entry_count,
            position_info,
            file_buffer: mmap,
        })
    }

    fn flush(&self) -> CResult<()> {
        Ok(self.file_buffer.flush()?)
    }
}

impl Debug for SegmentEntryPosition {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentEntryPosition")
            .field("entry_count", &self.entry_count)
            .finish()
    }
}