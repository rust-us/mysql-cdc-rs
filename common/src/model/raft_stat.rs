use mysql_common::proto::Text;
use mysql_common::Row;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use crate::err::CResult;
use crate::err::DecodeError::ReError;

#[derive(Default, Eq, PartialEq, Debug)]
pub struct State {
    route: Vec<ShardRaftRoute>,
}

#[derive(Eq, PartialEq, Debug)]
pub struct ShardRaftRoute {
    shard_name: String,
    shard_id: i64,
    host: String,
    port: i32,
    sysgroup_status: SyncGroupRuntimeStatus,
    current_status: MemberStatus,
    detination_status: MemberStatus,
}

#[derive(TryFromPrimitive, IntoPrimitive, Debug, Copy, Clone)]
#[repr(i32)]
#[derive(Eq, PartialEq)]
pub enum SyncGroupRuntimeStatus {
    INITED = 0,
    RUNNING = 1,
    STOPPED = 2,
}

#[derive(TryFromPrimitive, IntoPrimitive, Debug, Copy, Clone)]
#[repr(i32)]
#[derive(Eq, PartialEq)]
pub enum MemberStatus {
    ACTIVE = 0,
    PASSIVE = 1,
    LEAVED = 2,
}


impl ShardRaftRoute {
    #[inline]
    pub fn is_healthy(&self) -> bool {
        self.sysgroup_status == SyncGroupRuntimeStatus::RUNNING
            && self.current_status == MemberStatus::ACTIVE
    }
}

impl State {

    #[inline]
    pub fn create(route: Vec<ShardRaftRoute>) -> Self {
        Self { route }
    }


}

impl TryFrom<Row> for ShardRaftRoute {
    type Error = ReError;

    fn try_from(row: Row) -> Result<Self, Self::Error> {
        if row.len() != 6 {
            return Err(ReError::RcMysqlQueryErr(format!(
                "table KEPLER_SHARD_RAFT_ROUTE column must be 6, but is: {}", row.len()
            )));
        }
        let shard_name: String = row.get(0).ok_or(ReError::RcMysqlQueryErr(
            "table KEPLER_SHARD_RAFT_ROUTE column shard_name must be not null".to_string()
        ))?;
        let shard_id: i64 = row.get(1).ok_or(ReError::RcMysqlQueryErr(
            "table KEPLER_SHARD_RAFT_ROUTE column shard_id must be not null".to_string()
        ))?;
        let ip_port: String = row.get(2).ok_or(ReError::RcMysqlQueryErr(
            "table KEPLER_SHARD_RAFT_ROUTE column ip_port must be not null".to_string()
        ))?;
        let sysgroup_status: String = row.get(3).ok_or(ReError::RcMysqlQueryErr(
            "table KEPLER_SHARD_RAFT_ROUTE column ip_port must be not null".to_string()
        ))?;
        let current_status: String = row.get(4).ok_or(ReError::RcMysqlQueryErr(
            "table KEPLER_SHARD_RAFT_ROUTE current_status ip_port must be not null".to_string()
        ))?;
        let detination_status: String = row.get(5).ok_or(ReError::RcMysqlQueryErr(
            "table KEPLER_SHARD_RAFT_ROUTE column detination_status must be not null".to_string()
        ))?;
        let (host, port) = if let Some((host, ip)) = ip_port.split_once(":") {
            (String::from(host), i32::from_str_radix(ip, 10).map_err(|_| {
                ReError::RcMysqlQueryErr(
                    "table KEPLER_SHARD_RAFT_ROUTE ip_port format error".to_string()
                )
            })?)
        } else {
            return Err(ReError::RcMysqlQueryErr(
                "table KEPLER_SHARD_RAFT_ROUTE ip_port format error".to_string()
            ));
        };
        Ok(Self {
            shard_name,
            shard_id,
            host,
            port,
            sysgroup_status: SyncGroupRuntimeStatus::try_from(sysgroup_status)?,
            current_status: MemberStatus::try_from(current_status)?,
            detination_status: MemberStatus::try_from(detination_status)?,
        })
    }
}


impl TryFrom<String> for SyncGroupRuntimeStatus {
    type Error = ReError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value == "INITED" {
            Ok(Self::INITED)
        } else if value == "RUNNING" {
            Ok(Self::RUNNING)
        } else if value == "STOPPED" {
            Ok(Self::STOPPED)
        } else {
            Err(ReError::RcMysqlQueryErr(
                format!("table stats fresh, unknown SyncGroupRuntimeStatus: {}", value)
            ))
        }
    }
}

impl TryFrom<String> for MemberStatus {
    type Error = ReError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value == "ACTIVE" {
            Ok(Self::ACTIVE)
        } else if value == "PASSIVE" {
            Ok(Self::PASSIVE)
        } else if value == "LEAVED" {
            Ok(Self::LEAVED)
        } else {
            Err(ReError::RcMysqlQueryErr(
                format!("table stats fresh, unknown MemberStatus: {}", value)
            ))
        }
    }
}