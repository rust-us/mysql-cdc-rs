use std::io::{Read, Write};
use std::net::TcpStream;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use common::err::CResult;
use crate::{PACKET_HEADER_SIZE, TIMEOUT_LATENCY_DELTA};
use crate::conn::connection_options::ConnectionOptions;

pub struct PacketChannel {
    stream: TcpStream,
}

impl PacketChannel {
    pub fn new(options: &ConnectionOptions) -> CResult<Self> {
        let address: String = format!("{}:{}", options.hostname, options.port.to_string());
        let stream = TcpStream::connect(address)?;
        let read_timeout = options.heartbeat_interval + TIMEOUT_LATENCY_DELTA;
        stream.set_read_timeout(Some(read_timeout))?;
        Ok(Self { stream })
    }

    pub fn read_packet(&mut self) -> CResult<(Vec<u8>, u8)> {
        let mut header_buffer = [0; PACKET_HEADER_SIZE];

        self.stream.read_exact(&mut header_buffer)?;
        let packet_size = (&header_buffer[0..3]).read_u24::<LittleEndian>()?;
        let seq_num = header_buffer[3];

        let mut packet: Vec<u8> = vec![0; packet_size as usize];
        self.stream.read_exact(&mut packet)?;

        Ok((packet, seq_num))
    }

    pub fn write_packet(&mut self, packet: &[u8], seq_num: u8) -> CResult<()> {
        let packet_len = packet.len() as u32;
        self.stream.write_u24::<LittleEndian>(packet_len)?;
        self.stream.write_u8(seq_num)?;
        self.stream.write(packet)?;
        Ok(())
    }

    pub fn upgrade_to_ssl(&mut self) {
        // todo
    }

}

impl Default for PacketChannel {
    fn default() -> Self {
        PacketChannel::new(&ConnectionOptions::default()).unwrap()
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test() {
        assert_eq!(1, 1);
        println!("binlog lib test:{}", 0x21);
    }
}

