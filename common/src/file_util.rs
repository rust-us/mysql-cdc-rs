use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};

use crate::err::CResult;

/// 读取文件的某段字节块.
pub fn read_file_bytes(path: &str, start: u64, len: usize) -> CResult<Vec<u8>> {
    let file = File::open(path)?;
    let mut reader = BufReader::with_capacity(len, file);
    reader.seek(SeekFrom::Start(start))?;
    let r = reader.fill_buf()?;
    Ok(r.to_vec())
}

/// 更新文件某段字节块.
pub fn update_file_bytes(path: &str, start: u64, data: &[u8]) -> CResult<()> {
    let mut f = OpenOptions::new()
        .write(true)
        .append(false)
        .open(path)?;
    f.seek(SeekFrom::Start(start))?;
    f.write_all(data)?;
    Ok(f.flush()?)
}