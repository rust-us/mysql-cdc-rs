use std::path::Path;

use getset::{Getters, Setters};

use common::err::CResult;
use common::err::decode_error::ReError;

use crate::storage::storage_config::SEGMENT_FILE_PRE;

#[derive(Debug, Clone, Getters, Setters)]
pub struct SegmentFile {
    // 文件的绝对路径: /x/x/x/x/rlog-{version}-{id}-{index}.log
    #[getset(get = "pub")]
    path: String,
    // 文件名: rlog-{version}-{id}-{index}.log
    #[getset(get = "pub")]
    name: String,
    // 文件大小
    size: u64,
}

impl SegmentFile {
    pub fn new(path: String, name: String, size: u64) -> Self {
        Self {
            path,
            name,
            size,
        }
    }

    /// /a/s/rlog-{version}-{id}-{index}.log
    pub fn from_path(file_path: &str) -> CResult<Self> {
        let path = file_path.to_string();
        let p = Path::new(file_path);
        let size = p.metadata()?.len();
        let os_name = p.file_name().ok_or(ReError::String("segment file not exists.".to_string()))?;
        let name = os_name.to_str().ok_or(ReError::String("segment file not exists.".to_string()))?.to_string();
        Ok(Self {
            path,
            name,
            size,
        })
    }


    /// 判断文件: rlog-{version}-{id}-{index}.log
    pub fn is_segment_file(file_name: &str) -> CResult<bool> {
        if !file_name.ends_with(".log") {
            return Ok(false);
        }
        if !file_name.starts_with(SEGMENT_FILE_PRE) {
            return Ok(false);
        }
        let parse = Self::segment_file_name_split(file_name)?;
        if parse.0 == 0 || parse.1 == 0 || parse.2 == 0 {
            return Ok(false);
        }
        Ok(true)
    }

    /// current version
    pub fn version(&self) -> CResult<u32> {
        Ok(Self::segment_file_name_split(self.name())?.0)
    }

    /// segment id
    pub fn segment_id(&self) -> CResult<u32> {
        Ok(Self::segment_file_name_split(self.name())?.1)
    }

    /// segment file first index
    pub fn index(&self) -> CResult<u64> {
        Ok(Self::segment_file_name_split(self.name())?.2)
    }

    /// segment文件名解析:`[{version},{id},{index}]`
    fn segment_file_name_split(file_name: &str) -> CResult<(u32, u32, u64)> {
        let s: Vec<&str> = file_name.split(".").collect();
        let name_split: Vec<&str> = s[0].split("-").collect();
        let version = name_split[1].parse::<u32>()?;
        let segment_id = name_split[2].parse::<u32>()?;
        let first_index = name_split[3].parse::<u64>()?;
        Ok((version, segment_id, first_index))
    }

    /// 返回当前segment大小
    pub fn size(&self) -> u64 {
        self.size
    }

    /// segment文件大小
    pub fn size_file(&self) -> CResult<u64> {
        let p = Path::new(self.path());
        Ok(p.metadata()?.len())
    }

    /// 增加entry大小
    pub fn add_entry_size(&mut self, entry_size: u64) {
        self.size += entry_size;
    }
}