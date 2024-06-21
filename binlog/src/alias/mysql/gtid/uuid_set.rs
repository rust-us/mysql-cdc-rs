use std::fmt::{Display, Formatter};
use std::io;
use std::str::FromStr;
use serde::Serialize;
use common::err::decode_error::ReError;
use crate::alias::mysql::gtid::gtid::Gtid;
use crate::alias::mysql::gtid::interval::Interval;
use crate::alias::mysql::gtid::uuid::Uuid;

#[derive(Debug, Serialize, Clone)]
pub struct UuidSet {
    /// Gets server uuid of the UuidSet.
    pub source_id: Uuid,

    /// Gets a list of intervals of the UuidSet.
    /// Vec<(start, stop)>
    pub intervals: Vec<Interval>,
}

impl<'a> FromStr for UuidSet {
    type Err = io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (uuid, ranges) = s
            .split_once(':')
            .ok_or_else(|| UuidSet::wrap_err(format!("invalid sid format: {}", s)))?;

        let uuid = Uuid::parse(String::from(uuid));

        let intervals = ranges
            .split(':')
            .map(|token| {
                let range = token.split('-').collect::<Vec<_>>();

                // let interval = match range.len() {
                //     1 => {
                //         let interval = Interval::new(range[0].parse().unwrap(), range[0].parse().unwrap());
                //         // let start = UuidSet::parse_interval_num(range[0], s)?;
                //         // let interval = Interval::check_and_new(start, start + 1)?;
                //         interval
                //     },
                //     2 => {
                //         let interval = Interval::new(range[0].parse().unwrap(), range[1].parse().unwrap());
                //
                //         // let start = UuidSet::parse_interval_num(range[0], s)?;
                //         // let end = UuidSet::parse_interval_num(range[1], s)?;
                //         // let interval = Interval::check_and_new(start, end + 1)?;
                //         interval
                //     },
                //     _ => Err(UuidSet::wrap_err(format!("invalid GnoInterval format: {}", s))),
                // };

                if range.len() != 1 && range.len() != 2 {
                    return Err(UuidSet::wrap_err(format!("invalid GnoInterval format: {}", s)));
                }
                if range.len() == 1 {
                    let start = UuidSet::parse_interval_num(range[0], s)?;
                    let interval = Interval::check_and_new(start, start)?;
                    Ok(interval)
                } else {
                    let start = UuidSet::parse_interval_num(range[0], s)?;
                    let end = UuidSet::parse_interval_num(range[1], s)?;
                    let interval = Interval::check_and_new(start, end)?;
                    Ok(interval)
                }
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            source_id: uuid.unwrap(),
            intervals,
        })
    }
}


impl UuidSet {
    pub fn new(source_id: Uuid, mut intervals: Vec<Interval>) -> UuidSet {
        if intervals.len() > 1 {
            collapse_intervals(&mut intervals);
        }

        UuidSet {
            source_id,
            intervals,
        }
    }


    /// Adds a gtid value to the UuidSet.
    pub fn add_gtid(&mut self, gtid: Gtid) -> Result<bool, ReError> {
        if self.source_id.data != gtid.source_id.data {
            return Err(ReError::String(
                "SourceId of the passed gtid doesn't belong to the UuidSet".to_string(),
            ));
        }

        let index = find_interval_index(&self.intervals, gtid.transaction_id);
        let mut added = false;
        if index < self.intervals.len() {
            let interval = &mut self.intervals[index];
            if interval.get_start() == gtid.transaction_id + 1 {
                interval.set_start(gtid.transaction_id);
                added = true;
            } else if interval.get_end() + 1 == gtid.transaction_id {
                interval.set_end(gtid.transaction_id);
                added = true;
            } else if interval.get_start() <= gtid.transaction_id && gtid.transaction_id <= interval.get_end() {
                return Ok(false);
            }
        }
        if !added {
            let interval = Interval::new(gtid.transaction_id, gtid.transaction_id);
            self.intervals.insert(index, interval);
        }
        if self.intervals.len() > 1 {
            collapse_intervals(&mut self.intervals);
        }
        Ok(true)
    }

    fn wrap_err(msg: String) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidInput, msg)
    }

    fn parse_interval_num(to_parse: &str, full: &str) -> Result<u64, io::Error> {
        let n: u64 = to_parse.parse().map_err(|e| {
            UuidSet::wrap_err(format!(
                "invalid GnoInterval format: {}, error: {}",
                full, e
            ))
        })?;
        Ok(n)
    }

    fn _combine(intervals: Vec<Interval>) -> Vec<Interval> {
        // todo
        // todo
        // todo

        intervals
    }
}

impl UuidSet {
    pub fn get_source_id(&self) -> Uuid {
        self.source_id.clone()
    }

    pub fn get_intervals_len(&self) -> usize {
        self.intervals.len()
    }

    pub fn push_interval(&mut self, interval: Interval) -> bool {
        self.intervals.push(interval);

        true
    }

    /// 把{start,stop}连续的合并掉: [{start:1, stop:4},{start:4, stop:5}] => [{start:1, stop:5}]
    pub fn combine(&mut self) {
        let new_intervals = UuidSet::_combine(self.intervals.clone());

        self.intervals = new_intervals;
    }

    pub fn intervals(&self) -> &Vec<Interval> {
        &self.intervals
    }

}

impl Display for UuidSet {
    /// Returns string representation of an UuidSet part of a GtidSet.
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let intervals = self
            .intervals
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<String>>()
            .join(":");

        write!(f, "{}:{}", self.source_id, intervals)
    }
}

pub fn find_interval_index(intervals: &Vec<Interval>, transaction_id: u64) -> usize {
    let mut result_index = 0;
    let mut left_index = 0;
    let mut right_index = intervals.len();

    while left_index < right_index {
        result_index = (left_index + right_index) / 2;
        let interval = &intervals[result_index];
        if interval.get_end() < transaction_id {
            left_index = result_index + 1;
        } else if transaction_id < interval.get_start() {
            right_index = result_index;
        } else {
            return result_index;
        }
    }
    if !intervals.is_empty() && intervals[result_index].get_end() < transaction_id {
        result_index += 1;
    }
    result_index
}

pub fn collapse_intervals(intervals: &mut Vec<Interval>) {
    let mut index = 0;

    while index < intervals.len() - 1 {
        let right_start = intervals[index + 1].get_start();
        let right_end = intervals[index + 1].get_end();

        let mut left = &mut intervals[index];
        if left.get_end() + 1 == right_start {
            left.set_end(right_end);
            intervals.remove(index + 1);
        } else {
            index += 1;
        }
    }
}
