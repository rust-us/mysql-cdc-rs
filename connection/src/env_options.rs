use std::cell::RefCell;
use std::sync::Arc;
use serde::Serialize;

pub type EnvOptionsRef = Arc<RefCell<EnvOptions>>;

#[derive(Debug, Serialize, Clone)]
pub struct EnvOptions {
    /// 是否为 debug。 true 为阻debug模式，  false 为正常模式
    debug: bool,

    /// 是否为阻塞式。 true 为阻塞， false 为非阻塞
    blocked: bool,
}

impl EnvOptions {
    pub fn new(debug: bool, blocked: bool,) -> Self {
        EnvOptions {
            debug,
            blocked,
        }
    }

    pub fn debug() -> Self {
        EnvOptions::new(true, false)
    }

    pub fn blocked() -> Self {
        EnvOptions::new(false, true)
    }

}

impl Default for EnvOptions {
    fn default() -> Self {
        EnvOptions::new(false, false)
    }
}

impl EnvOptions {
    pub fn is_debug(&self) -> bool {
        self.debug
    }

    pub fn is_blocked(&self) -> bool {
        self.blocked
    }
}

#[cfg(test)]
mod test {
    use common::column::column_type::ColumnType;

    #[test]
    fn test() {
        assert_eq!(1, 1);

        let dd = ColumnType::Geometry;
        let c = dd.clone() as u8;
        assert_eq!(255, c);
    }
}