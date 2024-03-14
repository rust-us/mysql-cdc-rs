use std::cell::RefCell;
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fmt::{Debug, Display, Formatter};
use std::fs;
use std::path::PathBuf;
use std::rc::Rc;

use tracing::info;
use tracing_subscriber::fmt::format;

use common::err::CResult;
use common::err::decode_error::ReError;

use crate::storage::segment::Segment;
use crate::storage::segment_file::SegmentFile;
use crate::storage::storage_config::StorageConfig;

/// 目标表segment文件管理.
///
pub struct SegmentManager {
    // segment列表
    segments: BTreeMap<u64, Rc<RefCell<Segment>>>,
    // 当前活跃的segment
    current_segment: Rc<RefCell<Segment>>,
    // segment文件夹
    segment_dir: String,
    // 单个segment最大值
    max_segment_size: u64,
    // 单个segment最多entry数
    max_segment_entries: u32,
}

impl SegmentManager {
    /// 初始化一个segment管理器
    pub fn new(storage_config: &StorageConfig, dst_db_name: &str, dst_table_name: &str) -> CResult<Self> {
        let segment_dir = Self::get_segment_dir_path(storage_config.relay_log_dir(), dst_db_name, dst_table_name)?;
        let max_segment_size = *storage_config.max_segment_size();
        let max_segment_entries = *storage_config.max_segment_entries();
        // 加载已有的segment文件
        let mut segments = Self::load_segment(segment_dir.as_str())?;
        info!("load segments: {:?}", &segments);
        if segments.is_empty() {
            // 实例化第一个segment
            let mut segment = Segment::new(&segment_dir,
                                           1,
                                           1,
                                           max_segment_size,
                                           max_segment_entries)?;
            segment.write_open()?;
            let index = segment.first_index();
            let current_segment = Rc::new(RefCell::new(segment));
            segments.insert(index, Rc::clone(&current_segment));
            Ok(Self {
                current_segment,
                segments,
                segment_dir,
                max_segment_size,
                max_segment_entries,
            })
        } else {
            let current_segment = Rc::clone(segments.last_entry().ok_or(ReError::Error("get last segment err.".to_string()))?.get());
            Ok(Self {
                current_segment,
                segments,
                segment_dir,
                max_segment_size,
                max_segment_entries,
            })
        }
    }

    /// 加载目标表所有segment文件
    fn load_segment(segment_dir: &str) -> CResult<BTreeMap<u64, Rc<RefCell<Segment>>>> {
        info!("++++start load segments: {:?}", segment_dir);
        let path = PathBuf::from(segment_dir);
        if !path.exists() {
            fs::create_dir(path.as_path())?;
        }
        let mut segments: BTreeMap<u64, Rc<RefCell<Segment>>> = BTreeMap::new();
        let files = path.read_dir()?;
        for file in files {
            if let Ok(f) = file {
                let file_path = PathBuf::from(f.path());
                if file_path.is_file() {
                    if let Some(segment_file_name) = file_path.file_name().ok_or(ReError::String("".to_string()))?.to_str() {
                        if SegmentFile::is_segment_file(segment_file_name)? {
                            // segment文件全路径
                            let segment_file_path = file_path.to_str().ok_or(ReError::String("".to_string()))?;
                            if let Ok(mut segment) = Segment::from_file(segment_file_path) {
                                if !segment.is_full() {
                                    segment.write_open()?;
                                }
                                // todo 校验segment合法性?
                                segments.insert(segment.first_index(), Rc::new(RefCell::new(segment)));
                            }
                        }
                    }
                }
            }
        }
        Ok(segments)
    }

    /// 获取目标表的日志文件夹路径
    pub fn get_segment_dir_path(relay_log_dir: &str, dst_db_name: &str, dst_table_name: &str) -> CResult<String> {
        let log_dir_name = format!("{}#{}", dst_db_name, dst_table_name);
        let path = PathBuf::from(relay_log_dir).join(&log_dir_name);
        Ok(path.to_str().ok_or(ReError::String("".to_string()))?.to_string())
    }

    /// 当前segment
    pub fn current_segment(&self) -> Rc<RefCell<Segment>> {
        Rc::clone(&self.current_segment)
    }

    /// 最后一个segment
    pub fn last_segment(&mut self) -> CResult<Rc<RefCell<Segment>>> {
        Ok(Rc::clone(self.segments.last_entry().ok_or(ReError::Error("get last segment err.".to_string()))?.get()))
    }

    /// 第一个segment
    pub fn first_segment(&mut self) -> CResult<Rc<RefCell<Segment>>> {
        Ok(Rc::clone(self.segments.first_entry().ok_or(ReError::Error("get last segment err.".to_string()))?.get()))
    }

    /// 返回index所在的segment
    pub fn segment(&mut self, index: u64) -> CResult<Rc<RefCell<Segment>>> {
        if self.current_segment.borrow().contain_index(index) {
            Ok(Rc::clone(&self.current_segment))
        } else {
            for (i, s) in &self.segments {
                if s.borrow().contain_index(index) {
                    return Ok(Rc::clone(s));
                }
            }
            Err(ReError::Error(format!("unknown index: {}.", index)))
        }
    }

    /// 创建下一个segment
    pub fn create_next_segment(&mut self) -> CResult<Rc<RefCell<Segment>>> {
        let last_segment = self.last_segment()?;
        if last_segment.borrow().is_writable() {
            // 关闭最后一个segment可写模式
            last_segment.borrow_mut().write_close()?;
        }
        let next_segment_id = last_segment.borrow().id() + 1;
        let next_segment_first_index = last_segment.borrow().last_index() + 1;
        let mut next_segment = Segment::new(&self.segment_dir,
                                            next_segment_id,
                                            next_segment_first_index,
                                            self.max_segment_size,
                                            self.max_segment_entries)?;
        next_segment.write_open()?;
        self.current_segment = Rc::new(RefCell::new(next_segment));
        let r = self.segments.insert(next_segment_first_index, Rc::clone(&self.current_segment));
        if let Some(old) = r {
            drop(old);
        }
        Ok(Rc::clone(&self.current_segment))
    }
}

impl Debug for SegmentManager {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentManager")
            .field("segment_num", &self.segments.len())
            .field("segments", &self.segments)
            .field("current_segment", &self.current_segment)
            .finish()
    }
}