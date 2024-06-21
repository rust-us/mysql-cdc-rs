use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use mysql_common::Row;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use once_cell::sync::OnceCell;

use crate::err::CResult;
use crate::err::decode_error::ReError;

pub type ShardRaftGroupRef = Arc<RwLock<Vec<ShardRaftRoute>>>;

#[derive(Default, Debug)]
pub struct RaftState {
    route: HashMap<i64, ShardRaftGroupRef>,
}

#[derive(Debug)]
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

/// raft state
pub static mut RAFT_STATE: OnceCell<RaftState> = OnceCell::new();

/// init static raft state, this function only work once
pub fn init_raft_state() {
    unsafe {
        let _ = RAFT_STATE.get_or_init(|| {
            RaftState {
                route: Default::default(),
            }
        });
    }
}

pub fn get_raft_state_mut() -> &'static mut RaftState {
    unsafe {
        RAFT_STATE.get_mut().expect("RAFT_STATE not init yet when calling get_raft_state_mut!")
    }
}

pub fn get_raft_state() -> &'static RaftState {
    unsafe {
        RAFT_STATE.get().expect("RAFT_STATE not init yet when calling get_raft_state!")
    }
}

impl ShardRaftRoute {
    #[inline]
    pub fn is_healthy(&self) -> bool {
        self.sysgroup_status == SyncGroupRuntimeStatus::RUNNING
            && self.current_status == MemberStatus::ACTIVE
    }
}

// map lock error
macro_rules! mle {
    ($p: expr) => {
        $p.map_err(|_| ReError::OpRaftErr("raft state lock failed".into()))
    };
}

impl RaftState {
    #[inline]
    pub fn update(&mut self, route: ShardRaftRoute) -> CResult<()> {
        let group = self.route.entry(route.shard_id).or_insert_with(|| {
            Arc::new(RwLock::new(vec![]))
        });
        let mut group = mle!(group.write())?;
        let find = group
            .iter()
            .enumerate()
            .find_map(|(idx, other)| {
                if &route == other {
                    Some(idx)
                } else {
                    None
                }
            });
        // remove from group vec
        if let Some(idx) = find {
            group.remove(idx);
        }
        group.push(route);
        Ok(())
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

impl PartialEq<Self> for ShardRaftRoute {
    fn eq(&self, other: &Self) -> bool {
        self.shard_id == other.shard_id
            && self.host == other.host
            && self.port == other.port
    }
}

impl Eq for ShardRaftRoute {}