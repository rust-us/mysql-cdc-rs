use serde::Serialize;

// /// Result returning Error
// pub type CResult<T> = std::result::Result<T, dyn Error>;

#[derive(Serialize)]
pub struct R {
    code: u16,
    message: String,
}

impl Default for R {
    fn default() -> Self {
        R::success("")
    }
}

impl R {
    pub fn success(msg: &str) -> Self {
        R {
            code: 0,
            message: msg.to_string(),
        }
    }

    pub fn error(code:u16, msg: &str) -> Self {
        R {
            code,
            message: msg.to_string(),
        }
    }
}