use std::{fmt, io};
use std::fmt::Display;
use serde::Serialize;

#[derive(Debug, Serialize, Clone)]
pub struct Interval {
    /// Gets first transaction id in the interval.
    start: u64,
    /// Gets last transaction id in the interval.
    end: u64,
}

impl Display for Interval {
    /// Returns string representation of an UuidSet interval.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.start == self.end {
            write!(f, "{}", self.start)
        } else {
            write!(f, "{}-{}", self.start, self.end)
        }
    }
}

impl Interval {
    pub fn new(start: u64, end: u64,) -> Interval {
        Interval {
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

    pub fn get_start(&self) -> u64 {
        self.start
    }

    pub fn set_start(&mut self, start:u64) {
        self.start = start;
    }

    pub fn get_end(&self) -> u64 {
        self.end
    }

    pub fn set_end(&mut self, end:u64) {
        self.end = end;
    }
}
