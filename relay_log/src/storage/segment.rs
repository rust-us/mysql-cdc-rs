use std::fmt::{Debug, Formatter};
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use checksum::crc32::Crc32;
use tracing::{error, warn};

use common::err::CResult;
use common::err::decode_error::ReError;

use crate::codec::binary_codec::{BinaryCodec, CodecStyle};
use crate::codec::binary_codec::CodecStyle::LittleVar;
use crate::codec::codec::Codec;
use crate::relay_log::RelayLog;
use crate::storage::file_system::FileSystem;
use crate::storage::segment::SegmentStatus::{ReadOnly, WriteRead};
use crate::storage::segment_entry_position::SegmentEntryPosition;
use crate::storage::segment_file::SegmentFile;
use crate::storage::segment_header::SegmentHeader;
use crate::storage::storage_config::{SEGMENT_FILE_PRE, SEGMENT_HEADER_SIZE_BYTES, VERSION};
use crate::storage::storage_entry::StorageEntry;

const FILE_WRITE_BUFFER_SIZE: usize = 4 * 1024;
const FILE_READ_BUFFER_SIZE: usize = 16 * 1024;

/// 日志存储文件.
///
/// 文件结构:
/// ```txt
/// |=================|
/// |      header     | -> SegmentHeader
/// |-----------------|
/// |  entry position | -> SegmentEntryPosition
/// |-----------------|
/// |   StorageEntry  |
/// |   StorageEntry  |
/// |      ...        |
/// |=================|
/// ```
pub(crate) struct Segment {
    // 文件信息（文件名、文件路径、文件大小）
    segment_file: SegmentFile,
    // 文件头
    header: SegmentHeader,
    // entry偏移量
    entry_position: SegmentEntryPosition,
    // 编解码
    codec: BinaryCodec,
    // 编解码风格
    codec_style: CodecStyle,
    // 文件读取
    reader: Arc<Mutex<BufReader<File>>>,
    // segment状态
    status: SegmentStatus,
}

/// Segment Status
/// - WriteRead: 可读可写
/// - ReadOnly: 只读
pub(crate) enum SegmentStatus {
    // 可读可写模式
    WriteRead(BufWriter<File>),
    // 只读模式
    ReadOnly,
}

impl SegmentStatus {
    pub fn name(&self) -> String {
        match self {
            WriteRead(_) => {
                String::from("WriteRead")
            }
            ReadOnly => {
                String::from("ReadOnly")
            }
        }
    }
}

impl Segment {
    /// 新建一个segment
    pub fn new(segment_dir_path: &str,
               id: u32,
               first_index: u64,
               max_segment_size: u64,
               max_entries: u32) -> CResult<Self> {
        // rlog-{version}-{id}-{index}.log
        let segment_file_name = format!("{}-{}-{}-{}.log", SEGMENT_FILE_PRE, VERSION, id, first_index);
        // /x/x/x/x/rlog-{version}-{id}-{index}.log
        let segment_file_path = PathBuf::from(segment_dir_path).join(&segment_file_name);
        if !segment_file_path.exists() {
            File::create_new(segment_file_path.as_path())?;
        }
        let segment_file_path_str = segment_file_path.to_str().ok_or(ReError::String("segment file not exists.".to_string()))?;
        let header = SegmentHeader::new(segment_file_path_str, id, first_index, max_segment_size, max_entries)?;
        let entry_position = SegmentEntryPosition::new(segment_file_path_str, max_entries)?;
        let init_segment_size = SEGMENT_HEADER_SIZE_BYTES as u64 + (4 + max_entries as u64 * 8);
        let segment_file = SegmentFile::new(segment_file_path_str.to_string(), segment_file_name, init_segment_size);

        let reader = BufReader::with_capacity(FILE_READ_BUFFER_SIZE, File::open(segment_file_path_str)?);
        Ok(Self {
            segment_file,
            header,
            entry_position,
            codec: BinaryCodec::new(),
            codec_style: LittleVar,
            reader: Arc::new(Mutex::new(reader)),
            status: ReadOnly,
        })
    }

    /// 从文件初始化segment
    pub fn from_file(file_path: &str) -> CResult<Self> {
        let segment_file = SegmentFile::from_path(file_path)?;
        let header = SegmentHeader::from_file(file_path, 0, SEGMENT_HEADER_SIZE_BYTES)?;

        let start_offset = SEGMENT_HEADER_SIZE_BYTES as u64;
        let bytes_size = 4 + (*header.max_entries()) * 8;
        let entry_position = SegmentEntryPosition::from_file(file_path, start_offset, bytes_size as usize)?;

        let reader = BufReader::with_capacity(FILE_READ_BUFFER_SIZE, File::open(file_path)?);
        Ok(Self {
            segment_file,
            header,
            entry_position,
            codec: BinaryCodec::new(),
            codec_style: LittleVar,
            reader: Arc::new(Mutex::new(reader)),
            status: ReadOnly,
        })
    }

    /// 计算切片crc32值
    fn checksum(buf: &[u8]) -> u32 {
        let mut crc = Crc32::new();
        crc.checksum(buf)
    }

    /// 开启可写模式
    pub fn write_open(&mut self) -> CResult<()> {
        let f1 = OpenOptions::new()
            .write(true)
            .append(true)
            .open(self.segment_file.path())?;
        let writer = BufWriter::with_capacity(FILE_WRITE_BUFFER_SIZE, f1);
        self.status = WriteRead(writer);
        Ok(())
    }

    /// 关闭可写模式
    pub fn write_close(&mut self) -> CResult<()> {
        match &mut self.status {
            WriteRead(w) => {
                self.entry_position.flush()?;
                w.flush()?;
                self.status = ReadOnly;
            }
            ReadOnly => {}
        }
        Ok(())
    }

    /// 返回当前segment第一个index值
    pub fn first_index(&self) -> u64 {
        if self.is_empty() {
            0
        } else {
            *self.header.first_index()
        }
    }

    /// segment id
    pub fn id(&self) -> u32 {
        *self.header.id()
    }

    /// 返回当前segment最后一个index值
    pub fn last_index(&self) -> u64 {
        if self.is_empty() {
            *self.header.first_index() - 1
        } else {
            *self.header.first_index() + self.entry_position.get_entry_count() as u64 - 1
        }
    }

    /// 返回当前segment下一个index值
    pub fn next_index(&self) -> u64 {
        if self.is_empty() {
            *self.header.first_index()
        } else {
            self.last_index() + 1
        }
    }

    /// 验证当前segment是否包含index
    pub fn contain_index(&self, index: u64) -> bool {
        if self.is_empty() {
            false
        } else {
            index >= self.first_index() && index <= self.last_index()
        }
    }

    /// 当前segment是否为空
    pub fn is_empty(&self) -> bool {
        self.entry_position.get_entry_count() == 0
    }

    /// 当前segment字节大小
    pub fn current_segment_size(&self) -> u64 {
        self.segment_file.size()
    }

    /// append entry(非线程安全，只能单线程写)
    /// </p>
    /// 每个Entry块包含如下内容（字节大小 = 8 + 8 + 8 + 4 + {RelayLogSize}）:
    ///
    /// ```txt
    /// index: 索引id, 8字节
    /// offset: 日志内容偏移量, 8字节
    /// size: 日志内容大小, 8字节
    /// checksum: 日志内容校验值, 4字节
    /// relay_log: 日志内容, 动态大小
    /// ```
    pub fn append(&mut self, entry: &mut StorageEntry) -> CResult<()> {
        match &mut self.status {
            WriteRead(w) => {
                // log serialize
                let log_bytes = self.codec.binary_serialize(&self.codec_style, entry.relay_log())?;

                let start_position = self.segment_file.size();
                let mut entry_size = 0;

                // index
                let index = *entry.index();
                w.write_u64::<LittleEndian>(*entry.index())?;
                entry_size += 8;

                // log size
                let log_size = log_bytes.len() as u64;
                entry.set_log_size(log_size);
                w.write_u64::<LittleEndian>(log_size)?;
                entry_size += 8;

                // checksum
                let checksum = Self::checksum(&log_bytes);
                entry.set_checksum(checksum);
                w.write_u32::<LittleEndian>(checksum)?;
                entry_size += 4;

                // log bytes
                w.write_all(&log_bytes)?;
                entry_size += log_size;

                if self.is_empty() {
                    self.entry_position.add_position(0, start_position)?;
                } else {
                    let offset = index - self.first_index();
                    self.entry_position.add_position(offset as usize, start_position)?;
                }

                self.segment_file.add_entry_size(entry_size);
                Ok(())
            }
            ReadOnly => {
                Err(ReError::String("segment read only.".to_string()))
            }
        }
    }

    /// read entry by index
    pub fn get_entry(&mut self, index: u64) -> CResult<StorageEntry> {
        if self.is_empty() {
            return Err(ReError::String("segment is empty.".to_string()));
        }
        if index < self.first_index() {
            return Err(ReError::String("index less than segment first index.".to_string()));
        }
        if index > self.last_index() {
            return Err(ReError::String("index greater than segment last index.".to_string()));
        }

        let offset = index - self.first_index();
        let entry_position = self.entry_position.get_position(offset as usize);

        let r = Arc::clone(&self.reader);
        let mut reader = r.lock().or_else(|e| {
            error!("segment read lock err: {:?}", &e);
            Err(ReError::Error(e.to_string()))
        })?;

        reader.seek(SeekFrom::Start(entry_position))?;

        let idx = reader.read_u64::<LittleEndian>()?;
        let log_size = reader.read_u64::<LittleEndian>()?;
        let checksum = reader.read_u32::<LittleEndian>()?;

        let mut log_bytes: Vec<u8> = vec![0; log_size as usize];
        reader.read_exact(&mut log_bytes)?;

        // 校验crc32值
        if checksum != Self::checksum(&log_bytes) {
            return Err(ReError::Error("log checksum err.".to_string()));
        }

        let relay_log = self.codec.binary_deserialize::<RelayLog>(&self.codec_style, &log_bytes)?;
        Ok(StorageEntry::new(idx, log_size, checksum, relay_log))
    }

    /// 是否可写
    pub fn is_writable(&self) -> bool {
        match self.status {
            WriteRead(_) => {
                true
            }
            ReadOnly => {
                false
            }
        }
    }

    /// segment is full
    pub fn is_full(&self) -> bool {
        let max_size = *self.header.max_segment_size();
        let max_entries = *self.header.max_entries();
        let current_size = self.segment_file.size();
        let current_entries = self.entry_position.get_entry_count();
        if current_entries >= max_entries || current_size >= max_size {
            true
        } else {
            false
        }
    }

    /// flush the segment writer buf to disk
    pub fn write_flush(&mut self) -> CResult<()> {
        match &mut self.status {
            WriteRead(w) => {
                self.entry_position.flush()?;
                w.flush()?;
            }
            ReadOnly => {}
        }
        Ok(())
    }

    /// 删除segment文件
    pub fn delete(&self) -> CResult<()> {
        let file_path = self.segment_file.path();
        warn!("===删除segment文件: {:?}", file_path);
        Ok(fs::remove_file(file_path)?)
    }
}

impl Debug for Segment {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Segment")
            .field("segment_file", &self.segment_file.name())
            .field("segment_id", &self.header.id())
            .field("first_index", &self.header.first_index())
            .field("entry_count", &self.entry_position.get_entry_count())
            .field("status", &self.status.name())
            .finish()
    }
}
