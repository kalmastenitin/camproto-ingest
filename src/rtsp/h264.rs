use bytes::{Bytes, BytesMut, BufMut};

pub struct H264Depackatizer {
    fu_buf: BytesMut, // FU-A reassembly buffer
    started: bool, // have we seen FU-A start
}

impl H264Depackatizer {
    pub fn new() -> Self {
        Self {
            fu_buf: BytesMut::with_capacity(256 * 1024),
            started: false,
        }
    }

    pub fn push(&mut self, payload: &[u8], marker: bool) -> Option<Bytes> {
        if payload.is_empty() {
            return None;
        }

        let nal_type = payload[0] & 0x1F;

        match nal_type {
            1..=23 => self.handle_single(payload),
            24 => self.handle_stap_a(payload),
            28 => self.handle_fu_a(payload, marker),
            _ => None,
        }
    }

    handle_single(&self, payload: &[u8]) -> Option<Bytes>{
        
    }
}

