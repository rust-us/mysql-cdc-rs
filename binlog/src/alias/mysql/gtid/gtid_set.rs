use std::collections::{BTreeMap};
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use serde::Serialize;
use common::err::CResult;
use common::err::decode_error::ReError;
use crate::alias::mysql::gtid::gtid::Gtid;
use crate::alias::mysql::gtid::interval::Interval;
use crate::alias::mysql::gtid::uuid::Uuid;
use crate::alias::mysql::gtid::uuid_set::UuidSet;

const UUID_LENGTH: usize = 36;

#[derive(Debug, Serialize, Clone)]
pub struct GtidSet {
    /// Gets a list of UuidSet parts in the GtidSet.
    pub uuid_sets: BTreeMap<String, UuidSet>,

}

impl Display for GtidSet {
    /// Returns string representation of the GtidSet.
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut uuids = self
            .uuid_sets
            .values()
            .map(|x| x.to_string())
            .collect::<Vec<String>>();

        uuids.sort();
        write!(f, "{}", uuids.join(","))
    }
}

impl GtidSet {
    pub fn new() -> Self {
        Self {
            uuid_sets: BTreeMap::new(),
        }
    }

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
    pub fn parse(gtid_data: String) -> CResult<GtidSet> {
        if gtid_data.is_empty() || gtid_data.trim().is_empty() {
            return Ok(GtidSet::new());
        }

        // 存在多个GTID时会有回车符
        let gtid_set= gtid_data.replace("\n", "");
        let uuid_sets = gtid_set.split(',').map(|x| x.trim()).collect::<Vec<&str>>();

        let mut result: BTreeMap<String, UuidSet> = BTreeMap::new();
        for uuid_set in uuid_sets {
            let source_id: String = uuid_set.chars().take(UUID_LENGTH).collect();
            let source_id = Uuid::parse(source_id)?;

            let mut intervals = Vec::new();
            let ranges: String = uuid_set.chars().skip(UUID_LENGTH + 1).collect();
            let ranges = ranges.split(':').collect::<Vec<&str>>();

            for token in ranges {
                let range = token.split('-').collect::<Vec<&str>>();
                let interval = match range.len() {
                    1 => Interval::new(range[0].parse()?, range[0].parse()?),
                    2 => Interval::new(range[0].parse()?, range[1].parse()?),
                    _ => return Err(ReError::String(format!("Invalid interval format {}", token))),
                };
                intervals.push(interval);
            }
            result.insert(source_id.uuid.clone(), UuidSet::new(source_id, intervals));
        }

        return Ok(GtidSet {
            uuid_sets: result,
        });
    }
}

impl GtidSet {
    /// 获取uuid set 的map
    pub fn uuid_sets(& self) -> &BTreeMap<String, UuidSet> {
        &self.uuid_sets
    }

    /// Adds a gtid value to the GtidSet.
    pub fn add_gtid_str(&mut self, gtid_var: String) {
        let us = UuidSet::from_str(gtid_var.as_str()).unwrap();

        let sid = us.get_source_id();

        if self.uuid_sets.contains_key(&sid.uuid) {
            let gtid_set_sid = self.uuid_sets.get_mut(&sid.uuid).unwrap();
            for interval in us.intervals {
                gtid_set_sid.push_interval(interval);
            }

            gtid_set_sid.combine();
        } else {
            self.uuid_sets.insert(sid.uuid, us.clone());
        }
    }

    /// Adds a gtid value to the GtidSet.
    pub fn add_gtid(&mut self, gtid: Gtid) -> Result<bool, ReError> {
        let uuid_set = self
            .uuid_sets
            .entry(gtid.source_id.uuid.clone())
            .or_insert(UuidSet::new(gtid.source_id.clone(), Vec::new()));

        Ok(uuid_set.add_gtid(gtid)?)
    }

    pub fn contains_key(&self, sid: &str) -> bool {
        self.uuid_sets.contains_key(sid)
    }

    pub fn get(&self, sid: &str) -> Option<&UuidSet> {
        self.uuid_sets.get(sid)
    }
}

#[cfg(test)]
mod test {
    use crate::alias::mysql::gtid::gtid::Gtid;
    use crate::alias::mysql::gtid::gtid_set::GtidSet;
    use crate::alias::mysql::gtid::interval::Interval;
    use crate::alias::mysql::gtid::uuid::Uuid;

    pub const SERVER_UUID1: &str = "24bc7850-2c16-11e6-a073-0242ac110001";
    pub const SERVER_UUID2: &str = "24bc7850-2c16-11e6-a073-0242ac110002";

    fn create_uuid1() -> Uuid {
        Uuid::parse(String::from(SERVER_UUID1)).unwrap()
    }

    fn create_uuid2() -> Uuid {
        Uuid::parse(String::from(SERVER_UUID2)).unwrap()
    }

    #[test]
    fn test_parse() {
        let gtid = GtidSet::parse(String::from("726757ad-4455-11e8-ae04-0242ac110002:1-3:7-9")).unwrap();

        assert!(gtid.contains_key("726757ad-4455-11e8-ae04-0242ac110002"));
        assert!(!gtid.contains_key("aaa"));
        assert!(gtid.get("aaa").is_none());
        assert_eq!(gtid.get("726757ad-4455-11e8-ae04-0242ac110002").unwrap().get_source_id().uuid, "726757ad-4455-11e8-ae04-0242ac110002");
        assert_eq!(gtid.get("726757ad-4455-11e8-ae04-0242ac110002").unwrap().get_intervals_len(), 2);
    }

    #[test]
    fn test_display() {
        let mut gtid = GtidSet::parse(String::from("726757ad-4455-11e8-ae04-0242ac110002:1-3,726757ad-4455-11e8-ae04-0242ac110003:4")).unwrap();
        gtid.add_gtid_str(String::from("726757ad-4455-11e8-ae04-0242ac110886:1-3:7-9"));

        assert!(gtid.contains_key("726757ad-4455-11e8-ae04-0242ac110002"));
        assert_eq!(format!("{}", gtid), "726757ad-4455-11e8-ae04-0242ac110002:1-3,726757ad-4455-11e8-ae04-0242ac110003:4,726757ad-4455-11e8-ae04-0242ac110886:1-3:7-9");
        assert_eq!(gtid.to_string(), "726757ad-4455-11e8-ae04-0242ac110002:1-3,726757ad-4455-11e8-ae04-0242ac110003:4,726757ad-4455-11e8-ae04-0242ac110886:1-3:7-9");
    }

    #[test]
    fn parse_empty_string_returns_empty_gtid_set() {
        let empty = String::from("");
        let gtid_set = GtidSet::parse(empty.clone()).unwrap();

        assert_eq!(0, gtid_set.uuid_sets.len());
        assert_eq!(empty, gtid_set.to_string());
    }

    #[test]
    fn add_gtids_lists_merged() {
        let mut gtid_set = GtidSet::parse(format!("{}:3-5", SERVER_UUID1)).unwrap();

        gtid_set.add_gtid(Gtid::new(create_uuid1(), 2)).unwrap();
        gtid_set.add_gtid(Gtid::new(create_uuid1(), 4)).unwrap();
        gtid_set.add_gtid(Gtid::new(create_uuid1(), 5)).unwrap();
        gtid_set.add_gtid(Gtid::new(create_uuid1(), 7)).unwrap();
        gtid_set.add_gtid(Gtid::new(create_uuid2(), 9)).unwrap();
        gtid_set.add_gtid(Gtid::new(create_uuid1(), 0)).unwrap();

        assert_eq!(
            format!("{}:0:2-5:7,{}:9", SERVER_UUID1, SERVER_UUID2),
            gtid_set.to_string()
        );
    }

    #[test]
    fn add_gtid_in_gap_intervals_joined() {
        let mut gtid_set = GtidSet::parse(format!("{}:3-4:6-7", SERVER_UUID1)).unwrap();

        gtid_set.add_gtid(Gtid::new(create_uuid1(), 5)).unwrap();

        assert_eq!(format!("{}:3-7", SERVER_UUID1), gtid_set.to_string());
    }

    #[test]
    fn raw_gtid_sets_equals_correctly() {
        let list_1 = GtidSet::parse(format!("{}:1-191:192-199", SERVER_UUID1)).unwrap();
        let list_2 = GtidSet::parse(format!("{}:1-199", SERVER_UUID1)).unwrap();
        assert_eq!(list_1.to_string(), list_2.to_string());

        let list_1 = GtidSet::parse(format!("{}:1-191:193-199", SERVER_UUID1)).unwrap();
        let list_2 = GtidSet::parse(format!("{}:1-199", SERVER_UUID1)).unwrap();
        assert_ne!(list_1.to_string(), list_2.to_string());
    }

    #[test]
    fn simple_gtid_set_has_single_interval() {
        let gtid_set = GtidSet::parse(format!("{}:1-191", SERVER_UUID1)).unwrap();
        let uuid_set = gtid_set.uuid_sets.get(&create_uuid1().to_string()).unwrap();

        assert_eq!(1, uuid_set.intervals.len());
        assert_eq!(
            Interval::new(1, 191).to_string(),
            uuid_set.intervals[0].to_string()
        );
        assert_eq!(format!("{}:1-191", SERVER_UUID1), gtid_set.to_string());
    }

    #[test]
    fn continuous_intervals_collapsed() {
        let gtid_set = GtidSet::parse(format!("{}:1-191:192-199", SERVER_UUID1)).unwrap();
        let uuid_set = gtid_set.uuid_sets.get(&create_uuid1().to_string()).unwrap();

        assert_eq!(1, uuid_set.intervals.len());
        assert_eq!(
            Interval::new(1, 199).to_string(),
            uuid_set.intervals[0].to_string()
        );
        assert_eq!(format!("{}:1-199", SERVER_UUID1), gtid_set.to_string());
    }

    #[test]
    fn non_continuous_intervals_not_collapsed() {
        let gtid_set = GtidSet::parse(format!("{}:1-191:193-199", SERVER_UUID1)).unwrap();
        let uuid_set = gtid_set.uuid_sets.get(&create_uuid1().to_string()).unwrap();

        assert_eq!(2, uuid_set.intervals.len());
        assert_eq!(
            Interval::new(1, 191).to_string(),
            uuid_set.intervals[0].to_string()
        );
        assert_eq!(
            Interval::new(193, 199).to_string(),
            uuid_set.intervals[1].to_string()
        );
        assert_eq!(
            format!("{}:1-191:193-199", SERVER_UUID1),
            gtid_set.to_string()
        );
    }

    #[test]
    fn multiple_intervals_not_collapsed() {
        let gtid_set =
            GtidSet::parse(format!("{}:1-191:193-199:1000-1033", SERVER_UUID1)).unwrap();
        let uuid_set = gtid_set.uuid_sets.get(&create_uuid1().to_string()).unwrap();

        assert_eq!(3, uuid_set.intervals.len());
        assert_eq!(
            Interval::new(1, 191).to_string(),
            uuid_set.intervals[0].to_string()
        );
        assert_eq!(
            Interval::new(193, 199).to_string(),
            uuid_set.intervals[1].to_string()
        );
        assert_eq!(
            Interval::new(1000, 1033).to_string(),
            uuid_set.intervals[2].to_string()
        );
        assert_eq!(
            format!("{}:1-191:193-199:1000-1033", SERVER_UUID1),
            gtid_set.to_string()
        );
    }

    #[test]
    fn multiple_intervals_some_collapsed() {
        let gtid_set = GtidSet::parse(format!(
            "{}:1-191:192-199:1000-1033:1035-1036:1038-1039",
            SERVER_UUID1
        ))
            .unwrap();
        let uuid_set = gtid_set.uuid_sets.get(&create_uuid1().to_string()).unwrap();

        assert_eq!(4, uuid_set.intervals.len());
        assert_eq!(
            Interval::new(1, 199).to_string(),
            uuid_set.intervals[0].to_string()
        );
        assert_eq!(
            Interval::new(1000, 1033).to_string(),
            uuid_set.intervals[1].to_string()
        );
        assert_eq!(
            Interval::new(1035, 1036).to_string(),
            uuid_set.intervals[2].to_string()
        );
        assert_eq!(
            Interval::new(1038, 1039).to_string(),
            uuid_set.intervals[3].to_string()
        );
        assert_eq!(
            format!("{}:1-199:1000-1033:1035-1036:1038-1039", SERVER_UUID1),
            gtid_set.to_string()
        );
    }

    #[test]
    fn multi_server_setup_has_single_intervals_trims_spaces() {
        let gtid_set = GtidSet::parse(format!(
            "{}:1-3:11:47-49, {}:1-19:55:56-100",
            SERVER_UUID1, SERVER_UUID2
        ))
            .unwrap();

        assert_eq!(2, gtid_set.uuid_sets.len());

        let uuid_set_1 = gtid_set.uuid_sets.get(&create_uuid1().to_string()).unwrap();
        let uuid_set_2 = gtid_set.uuid_sets.get(&create_uuid2().to_string()).unwrap();

        assert_eq!(3, uuid_set_1.intervals.len());
        assert_eq!(
            Interval::new(1, 3).to_string(),
            uuid_set_1.intervals[0].to_string()
        );
        assert_eq!(
            Interval::new(11, 11).to_string(),
            uuid_set_1.intervals[1].to_string()
        );
        assert_eq!(
            Interval::new(47, 49).to_string(),
            uuid_set_1.intervals[2].to_string()
        );

        assert_eq!(2, uuid_set_2.intervals.len());
        assert_eq!(
            Interval::new(1, 19).to_string(),
            uuid_set_2.intervals[0].to_string()
        );
        assert_eq!(
            Interval::new(55, 100).to_string(),
            uuid_set_2.intervals[1].to_string()
        );

        assert_eq!(
            format!("{}:1-3:11:47-49,{}:1-19:55-100", SERVER_UUID1, SERVER_UUID2),
            gtid_set.to_string()
        );
    }
}