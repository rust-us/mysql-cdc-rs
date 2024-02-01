use std::alloc::AllocError;
use bigdecimal::BigDecimal;
use memory::Buffer;
use crate::decimal_util::serialize;

use crate::schema::data_type::Value;

/// data_type Value序列化
pub trait WriteValue {

    fn write_value(&mut self, v: &Value, extra_not_null_flag: bool) -> Result<usize, AllocError>;

    fn write_raw_value(&mut self, v: &Value) -> Result<usize, AllocError>;

}

impl WriteValue for Buffer {

    fn write_value(&mut self, value: &Value, extra_not_null_flag: bool) -> Result<usize, AllocError> {
        let mut size = 0;

        //head： isNull
        match value {
            Value::Null => {
                self.write_byte(1)?;
                size += 1;
            }
            _ => {
                //isNull
                self.write_byte(0)?;
                if extra_not_null_flag {
                    //isNull
                    self.write_byte(0)?;
                }

                self.write_bytes(&value.get_data_type_code().to_le_bytes())?;
                //valid
                self.write_byte(0)?;
                //reused
                self.write_byte(0)?;
                size += 1 + 1 + 4 + 1 + 1;
            }
        }

        size += self.write_raw_value(value)?;
        Ok(size)
    }

    fn write_raw_value(&mut self, value: &Value) -> Result<usize, AllocError> {
        let mut size = 0;
        match value {
            Value::Null => {
            }
            Value::Boolean(v) => {
                self.write_byte(if *v {1} else {0})?;
                size += 1;
            }
            Value::Byte(v) => {
                self.write_byte(*v)?;
                size += 1;
            }
            Value::Short(v) => {
                self.write_bytes(&v.to_le_bytes())?;
                size += 2;
            }
            Value::Int(v) => {
                self.write_bytes(&v.to_le_bytes())?;
                size += 4;
            }
            Value::Long(v) | Value::Date(v) | Value::Time(v) | Value::DateTime(v) | Value::Timestamp(v) => {
                self.write_bytes(&v.to_le_bytes())?;
                size += 8;
            }
            Value::String(v) | Value::JSON(v) => {
                let bytes = v.as_bytes();
                //length: 4 bytes
                self.write_bytes(&(v.len() as i32).to_le_bytes())?;
                //utf8 bytes
                if v.len() > 0 {
                    self.write_bytes(bytes)?;
                }
                size += 4 + bytes.len();
            }
            Value::Float(v) => {
                self.write_bytes(&v.to_le_bytes())?;
                size += 4;
            }
            Value::Double(v) => {
                self.write_bytes(&v.to_le_bytes())?;
                size += 8;
            }
            //Value::Decimal(v, precision, scale) => {
            Value::Decimal(v) => {
                let decimal: BigDecimal = v.parse().unwrap();
                //todo
                let precision = 12u16;
                let scale = 2u16;
                self.write_bytes(&precision.to_le_bytes())?;
                //scale
                self.write_bytes(&scale.to_le_bytes())?;
                //data.len
                let vec = serialize(&decimal, precision.clone() as usize, scale.clone() as usize);

                let data = &vec[..];
                self.write_bytes(&(data.len() as i32).to_le_bytes())?;
                //data
                self.write_bytes(data)?;
                size += 2 + 2 + 4 + data.len();
            }
            Value::Binary(v) | Value::Blob(v) | Value::Bytes(v) => {
                //length: 4 bytes
                self.write_bytes(&(v.len() as i32).to_le_bytes())?;
                if v.len() > 0 {
                    self.write_bytes(&v[..])?;
                }
                size += 4 + v.len();
            }
        }

        Ok(size)
    }

}


#[cfg(test)]
mod test {
    use memory::Buffer;

    use crate::memory_ext::WriteValue;
    use crate::schema::data_type::Value;

    #[test]
    fn test() -> () {
        let mut buffer = Buffer::new().unwrap();
        let int = Value::Int(111);
        buffer.write_value(&int, true).unwrap();
    }

}
