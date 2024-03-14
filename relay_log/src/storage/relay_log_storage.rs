use std::cell::RefCell;
use std::rc::Rc;

use common::err::CResult;

use crate::relay_log::RelayLog;
use crate::storage::segment::Segment;
use crate::storage::segment_manager::SegmentManager;
use crate::storage::storage_config::StorageConfig;
use crate::storage::storage_entry::StorageEntry;

/// todo 目标表日志存储
pub struct RelayLogStorage {
    // 目标库
    dst_db_name: String,
    // 目标表
    dst_table_name: String,
    // segment管理器
    pub segment_manager: SegmentManager,
    // 环形队列
    entry_buffer: EntryRingBuffer,
    // 日志整理
    // log_compactor: Compactor,
}

struct EntryRingBuffer {
    entry_buffer_num: usize,
    // entry缓存
    entry_buffer: Vec<Option<Rc<StorageEntry>>>,

}

impl EntryRingBuffer {
    pub fn new(entry_buffer_num: usize) -> Self {
        let entry_buffer: Vec<Option<Rc<StorageEntry>>> = vec![None; entry_buffer_num];
        Self {
            entry_buffer_num,
            entry_buffer,
        }
    }

    pub fn add(&mut self, entry: StorageEntry) {
        let offset = self.offset(*entry.index());
        self.entry_buffer[offset] = Some(Rc::new(entry));
    }

    pub fn get(&self, index: u64) -> Option<Rc<StorageEntry>> {
        let e = &self.entry_buffer[self.offset(index)];
        match e {
            None => {
                None
            }
            Some(e) => {
                if *e.index() == index {
                    Some(Rc::clone(e))
                } else {
                    None
                }
            }
        }
    }

    fn offset(&self, index: u64) -> usize {
        let mut offset = index as usize % self.entry_buffer_num;
        if offset < 0 {
            offset = 0;
        }
        offset
    }
}

impl RelayLogStorage {
    pub fn new(storage_config: &StorageConfig, dst_db_name: String, dst_table_name: String) -> CResult<Self> {
        let segment_manager = SegmentManager::new(storage_config, &dst_db_name, &dst_table_name)?;
        let entry_buffer = EntryRingBuffer::new(*storage_config.entry_buffer_num());
        Ok(Self {
            dst_db_name,
            dst_table_name,
            segment_manager,
            entry_buffer,
        })
    }

    /// 追加中继日志
    pub fn append_relay_log(&mut self, log: RelayLog) -> CResult<()> {
        let mut entry = self.create_entry(log)?;
        let current_segment = self.current_usable_segment()?;
        // append to disk
        current_segment.borrow_mut().append(&mut entry)?;
        // add buf
        self.entry_buffer.add(entry);

        // todo send storage_event

        Ok(())
    }

    /// get an entry by index
    pub fn get_entry(&mut self, index: u64) -> CResult<Rc<StorageEntry>> {
        if let Some(entry) = &self.entry_buffer.get(index) {
            Ok(Rc::clone(entry))
        } else {
            let segment = self.segment_manager.segment(index)?;
            let entry = segment.borrow_mut().get_entry(index)?;
            Ok(Rc::new(entry))
        }
    }

    /// 创建中继日志存储实体
    fn create_entry(&mut self, log: RelayLog) -> CResult<StorageEntry> {
        let current_segment = self.current_usable_segment()?;
        let index = current_segment.borrow().next_index();
        Ok(StorageEntry::new(index, 0, 0, log))
    }

    /// 当前可用的segment
    fn current_usable_segment(&mut self) -> CResult<Rc<RefCell<Segment>>> {
        let mut current_segment = self.segment_manager.current_segment();
        if current_segment.borrow().is_full() {
            current_segment.borrow_mut().write_flush()?;
            current_segment = self.segment_manager.create_next_segment()?;
        }
        Ok(current_segment)
    }
}