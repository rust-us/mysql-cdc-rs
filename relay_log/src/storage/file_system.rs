use common::err::CResult;

pub trait FileSystem: Sized {
    /// 文件加载
    fn from_file(file_path: &str, start_offset: u64, len: usize) -> CResult<Self>;

    /// 强制刷盘（底层调用内核sync方法）
    fn flush(&self) -> CResult<()>;
}