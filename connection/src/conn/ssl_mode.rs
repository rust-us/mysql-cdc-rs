use serde::Serialize;

use common::err::CResult;
use common::err::decode_error::ReError;

/// Represents the SSL strategy used to connect to the server.
#[derive(Clone, Serialize, Copy, PartialEq, Debug)]
pub enum SslMode {
    /// Establishes an unencrypted connection.
    Disabled = 0,

    /// Tries to establish an encrypted connection without verifying CA/Host. Falls back to an unencrypted connection.
    IfAvailable = 1,

    /// Require an encrypted connection without verifying CA/Host.
    Require = 2,

    /// Verify that the certificate belongs to the Certificate Authority.
    RequireVerifyCa = 3,

    /// Verify that the certificate belongs to the Certificate Authority and matches Host.
    RequireVerifyFull = 4,
}

impl Default for SslMode {
    fn default() -> Self {
        SslMode::Disabled
    }
}

impl SslMode {
    /// 从配置字符串解析 SSL 模式(大小写不敏感，兼容多种写法)。
    pub fn from_name(name: &str) -> CResult<SslMode> {
        let normalized = name.trim().to_ascii_uppercase().replace(['-', ' '], "_");
        match normalized.as_str() {
            "" | "DISABLED" | "DISABLE" | "OFF" => Ok(SslMode::Disabled),
            "IF_AVAILABLE" | "PREFERRED" | "PREFER" => Ok(SslMode::IfAvailable),
            "REQUIRED" | "REQUIRE" => Ok(SslMode::Require),
            "VERIFY_CA" | "VERIFY_CA_OR_IDENTITY" | "REQUIRED_VERIFY_CA" => Ok(SslMode::RequireVerifyCa),
            "VERIFY_IDENTITY" | "VERIFY_FULL" | "REQUIRED_VERIFY_FULL" => Ok(SslMode::RequireVerifyFull),
            other => Err(ReError::ConfigFileParseErr(format!(
                "unknown ssl mode `{}`, expect one of [DISABLED | IF_AVAILABLE | REQUIRED | VERIFY_CA | VERIFY_IDENTITY]",
                other
            ))),
        }
    }

    /// 是否启用了加密(非 Disabled)
    pub fn is_enabled(&self) -> bool {
        *self != SslMode::Disabled
    }

    /// 是否强制要求加密(服务器不支持时必须报错，而不是降级明文)
    pub fn requires_encryption(&self) -> bool {
        matches!(
            self,
            SslMode::Require | SslMode::RequireVerifyCa | SslMode::RequireVerifyFull
        )
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            SslMode::Disabled => "DISABLED",
            SslMode::IfAvailable => "IF_AVAILABLE",
            SslMode::Require => "REQUIRED",
            SslMode::RequireVerifyCa => "VERIFY_CA",
            SslMode::RequireVerifyFull => "VERIFY_IDENTITY",
        }
    }
}

#[cfg(test)]
mod test {
    use super::SslMode;

    #[test]
    fn test_parse_ssl_mode() {
        assert_eq!(SslMode::Disabled, SslMode::from_name("").unwrap());
        assert_eq!(SslMode::Disabled, SslMode::from_name("disabled").unwrap());
        assert_eq!(SslMode::IfAvailable, SslMode::from_name("IF_AVAILABLE").unwrap());
        assert_eq!(SslMode::IfAvailable, SslMode::from_name("preferred").unwrap());
        assert_eq!(SslMode::Require, SslMode::from_name("Required").unwrap());
        assert_eq!(SslMode::RequireVerifyCa, SslMode::from_name("verify-ca").unwrap());
        assert_eq!(SslMode::RequireVerifyFull, SslMode::from_name("VERIFY_IDENTITY").unwrap());
        assert!(SslMode::from_name("bogus").is_err());
    }

    #[test]
    fn test_ssl_mode_semantics() {
        assert!(!SslMode::Disabled.is_enabled());
        assert!(SslMode::IfAvailable.is_enabled());
        assert!(!SslMode::IfAvailable.requires_encryption());
        assert!(SslMode::Require.requires_encryption());
        assert!(SslMode::RequireVerifyFull.requires_encryption());
    }
}
