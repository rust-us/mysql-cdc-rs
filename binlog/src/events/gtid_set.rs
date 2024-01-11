use std::collections::{BTreeMap};
use std::fmt::{Display, Formatter};
use std::io;
use std::str::FromStr;
use serde::Serialize;
use uuid::Uuid;

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct MysqlGTIDSet {
    gtid_set: BTreeMap<String, UUIDSet>,

}

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct UUIDSet {
    // uuid value
    sid: String,

    // Vec<(start, stop)>
    intervals: Vec<GnoInterval>,
}

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct GnoInterval {
    start: u64,
    end: u64,
}

impl<'a> FromStr for UUIDSet {
    type Err = io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (uuid, intervals) = s
            .split_once(':')
            .ok_or_else(|| UUIDSet::wrap_err(format!("invalid sid format: {}", s)))?;

        let uuid = Uuid::parse_str(uuid)
            .map_err(|e| UUIDSet::wrap_err(format!("invalid uuid format: {}, error: {}", s, e)))?;

        let intervals = intervals
            .split(':')
            .map(|interval| {
                let nums = interval.split('-').collect::<Vec<_>>();
                if nums.len() != 1 && nums.len() != 2 {
                    return Err(UUIDSet::wrap_err(format!("invalid GnoInterval format: {}", s)));
                }
                if nums.len() == 1 {
                    let start = UUIDSet::parse_interval_num(nums[0], s)?;
                    let interval = GnoInterval::check_and_new(start, start + 1)?;
                    Ok(interval)
                } else {
                    let start = UUIDSet::parse_interval_num(nums[0], s)?;
                    let end = UUIDSet::parse_interval_num(nums[1], s)?;
                    let interval = GnoInterval::check_and_new(start, end + 1)?;
                    Ok(interval)
                }
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            sid: uuid.to_string(),
            intervals,
        })
    }
}


impl UUIDSet {
    pub fn new(sid: String, intervals: Vec<GnoInterval>) -> UUIDSet {
        UUIDSet {
            sid,
            intervals,
        }
    }

    fn wrap_err(msg: String) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidInput, msg)
    }

    fn parse_interval_num(to_parse: &str, full: &str) -> Result<u64, io::Error> {
        let n: u64 = to_parse.parse().map_err(|e| {
            UUIDSet::wrap_err(format!(
                "invalid GnoInterval format: {}, error: {}",
                full, e
            ))
        })?;
        Ok(n)
    }

    fn _combine(intervals: Vec<GnoInterval>) -> Vec<GnoInterval> {
        // todo
        // todo
        // todo

        intervals
    }
}

impl UUIDSet {
    pub fn get_sid(&self) -> String {
        self.sid.clone()
    }

    pub fn get_intervals_len(&self) -> usize {
        self.intervals.len()
    }

    pub fn push_interval(&mut self, interval: GnoInterval) -> bool {
        self.intervals.push(interval);

        true
    }

    /// 把{start,stop}连续的合并掉: [{start:1, stop:4},{start:4, stop:5}] => [{start:1, stop:5}]
    pub fn combine(&mut self) {
        let new_intervals = UUIDSet::_combine(self.intervals.clone());

        self.intervals = new_intervals;
    }
}

impl GnoInterval {
    pub fn new(start: u64, end: u64,) -> GnoInterval {
        GnoInterval {
            start,
            end
        }
    }

    /// Checks if the [start, end) interval is valid and creates it.
    pub fn check_and_new(start: u64, end: u64) -> io::Result<Self> {
        if start >= end {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("start({}) >= end({}) in GnoInterval", start, end),
            ));
        }
        if start == 0 || end == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Gno can't be zero",
            ));
        }
        Ok(Self::new(start, end))
    }
}

impl Display for UUIDSet{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.sid)?;

        let mut it = self.intervals.iter();
        loop {
            if it.is_empty() {
                break;
            }

            let interval: &GnoInterval = it.next().unwrap();
            if interval.start == (interval.end - 1) {
                write!(f, ":{}", interval.start)?;
            } else {
                write!(f, ":{}-{}", interval.start, interval.end - 1)?;
            }
        }

        Ok(())
    }
}

impl Display for MysqlGTIDSet{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.gtid_set.is_empty() {
            let _ = write!(f, "");
            return Ok(());
        }

        let mut has_context = false;
        let mut it = self.gtid_set.iter();
        loop {
            if it.is_empty() {
                break;
            }

            if has_context {
                write!(f, ",")?;
            }

            let (k, v) = it.next().unwrap();
            write!(f, "{:?}", format!("{}", v))?;
            has_context = true;
        }

        Ok(())
    }
}

impl MysqlGTIDSet {

    /// 解析如下格式的字符串为MysqlGTIDSet:
    ///
    /// 726757ad-4455-11e8-ae04-0242ac110002:1 =>
    /// MysqlGTIDSet{ sets: { 726757ad-4455-11e8-ae04-0242ac110002: UUIDSet{ SID:
    /// 726757ad-4455-11e8-ae04-0242ac110002, intervals: [{start:1, stop:2}] } }
    /// }
    ///
    /// 726757ad-4455-11e8-ae04-0242ac110002:1-3 => MysqlGTIDSet{ sets: {
    /// 726757ad-4455-11e8-ae04-0242ac110002: UUIDSet{ SID:
    /// 726757ad-4455-11e8-ae04-0242ac110002, intervals: [{start:1, stop:4}] } }
    /// }
    ///
    /// 726757ad-4455-11e8-ae04-0242ac110002:1-3:4 => MysqlGTIDSet{ sets: {
    /// 726757ad-4455-11e8-ae04-0242ac110002: UUIDSet{ SID:
    /// 726757ad-4455-11e8-ae04-0242ac110002, intervals: [{start:1, stop:5}] } }
    /// }
    ///
    /// 726757ad-4455-11e8-ae04-0242ac110002:1-3:7-9 =>
    /// MysqlGTIDSet{ sets: {
    ///     726757ad-4455-11e8-ae04-0242ac110002: UUIDSet{ SID:
    ///     726757ad-4455-11e8-ae04-0242ac110002, intervals: [{start:1, stop:4}, {start:7, stop: 10}] }
    /// }}
    ///
    ///
    /// 726757ad-4455-11e8-ae04-0242ac110002:1-3,726757ad-4455-11e8-ae04-0242ac110003:4 =>
    /// MysqlGTIDSet{ sets: {
    ///     726757ad-4455-11e8-ae04-0242ac110002: UUIDSet{ SID:
    ///     726757ad-4455-11e8-ae04-0242ac110002, intervals: [{start:1, stop:4}] },
    ///     726757ad-4455-11e8-ae04-0242ac110003: UUIDSet{ SID:
    ///     726757ad-4455-11e8-ae04-0242ac110002, intervals: [{start:4, stop:5}] }
    /// }}
    ///
    pub fn parse(gtid_data: String) -> MysqlGTIDSet {
        let mut gtid_set: BTreeMap<String, UUIDSet> = BTreeMap::new();

        if gtid_data.trim().is_empty() {
            return MysqlGTIDSet {gtid_set}
        }

        // 存在多个GTID时会有回车符
        let gtid_data_split= gtid_data.replace("\n", "");
        let mut iter = gtid_data_split.split(",").into_iter();
        loop {
            let uuid_srt = iter.next();
            if uuid_srt.is_none() {
                break;
            }

            let uuid = uuid_srt.unwrap();
            if let Ok(sid) = UUIDSet::from_str(uuid) {
                gtid_set.insert(sid.get_sid(), sid);
            }
        }

        return MysqlGTIDSet {
            gtid_set,
        };
    }
}

impl MysqlGTIDSet {
    /// 取得所有权
    pub fn into_map(self) -> BTreeMap<String, UUIDSet> {
        self.gtid_set
    }

    /// gtid_set 数据更新
    pub fn update_gtid_set(&mut self, gtid_var: String) {
        let us = UUIDSet::from_str(gtid_var.as_str()).unwrap();

        let sid = us.get_sid();

        if self.gtid_set.contains_key(&sid) {
            let gtid_set_sid = self.gtid_set.get_mut(&sid).unwrap();
            for interval in us.intervals {
                gtid_set_sid.push_interval(interval);
            }

            gtid_set_sid.combine();
        } else {
            self.gtid_set.insert(sid, us.clone());
        }
    }

    pub fn contains_key(&self, sid: &str) -> bool {
        self.gtid_set.contains_key(sid)
    }

    pub fn get(&self, sid: &str) -> Option<&UUIDSet> {
        self.gtid_set.get(sid)
    }
}

#[cfg(test)]
mod test {
    use crate::events::gtid_set::MysqlGTIDSet;

    #[test]
    fn test_parse() {
        let gtid = MysqlGTIDSet::parse(String::from("726757ad-4455-11e8-ae04-0242ac110002:1-3:7-9"));

        assert!(gtid.contains_key("726757ad-4455-11e8-ae04-0242ac110002"));
        assert!(!gtid.contains_key("aaa"));
        assert!(gtid.get("aaa").is_none());
        assert_eq!(gtid.get("726757ad-4455-11e8-ae04-0242ac110002").unwrap().get_sid(), "726757ad-4455-11e8-ae04-0242ac110002");
        assert_eq!(gtid.get("726757ad-4455-11e8-ae04-0242ac110002").unwrap().get_intervals_len(), 2);
    }

    #[test]
    fn test_display() {
        let mut gtid = MysqlGTIDSet::parse(String::from("726757ad-4455-11e8-ae04-0242ac110002:1-3,726757ad-4455-11e8-ae04-0242ac110003:4"));
        gtid.update_gtid_set(String::from("726757ad-4455-11e8-ae04-0242ac110886:1-3:7-9"));

        assert!(gtid.contains_key("726757ad-4455-11e8-ae04-0242ac110002"));
        assert_eq!(format!("{}", gtid), "\"726757ad-4455-11e8-ae04-0242ac110002:1-3\",\"726757ad-4455-11e8-ae04-0242ac110003:4\",\"726757ad-4455-11e8-ae04-0242ac110886:1-3:7-9\"");
        assert_eq!(gtid.to_string(), "\"726757ad-4455-11e8-ae04-0242ac110002:1-3\",\"726757ad-4455-11e8-ae04-0242ac110003:4\",\"726757ad-4455-11e8-ae04-0242ac110886:1-3:7-9\"");
    }
}