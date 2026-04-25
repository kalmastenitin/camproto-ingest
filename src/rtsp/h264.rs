use bytes::{BufMut, Bytes, BytesMut};

pub struct H264Depackatizer {
    fu_buf: BytesMut, // FU-A reassembly buffer
    started: bool,    // have we seen FU-A start
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

    fn handle_single(&self, payload: &[u8]) -> Option<Bytes> {
        let mut buf = BytesMut::with_capacity(4 + payload.len());
        to_avcc(&mut buf, payload);
        Some(buf.freeze())
    }

    fn handle_stap_a(&self, payload: &[u8]) -> Option<Bytes> {
        let mut buf = BytesMut::with_capacity(payload.len()); // we are skipping initial 4 bytes as offset hence only len
        let mut offset = 1; // skip STAP-A header byte

        while offset + 2 <= payload.len() {
            let nal_size = u16::from_be_bytes([payload[offset], payload[offset + 1]]) as usize;
            offset += 2;

            if offset + nal_size > payload.len() {
                break; // this is malformed
            }

            let nal = &payload[offset..offset + nal_size];
            to_avcc(&mut buf, nal);
            offset += nal_size;
        }
        if buf.is_empty() {
            None
        } else {
            Some(buf.freeze())
        }
    }

    fn handle_fu_a(&mut self, payload: &[u8], marker: bool) -> Option<Bytes> {
        if payload.len() < 2 {
            return None;
        }

        let fu_indicator = payload[0];
        let fu_header = payload[1];

        let start = (fu_header & 0x80) != 0;
        let end = (fu_header & 0x40) != 0;
        let nal_type = fu_header & 0x1F;

        if start {
            self.fu_buf.clear();
            self.started = true;

            let nal_header = (fu_indicator & 0xE0) | nal_type;
            self.fu_buf.put_u8(nal_header);
            self.fu_buf.put_slice(&payload[2..]);
        } else if self.started {
            self.fu_buf.put_slice(&payload[2..]);
        } else {
            return None;
        }

        if end || marker {
            self.started = false;
            let nal = self.fu_buf.split().freeze();
            let mut out = BytesMut::with_capacity(4 + nal.len());
            to_avcc(&mut out, &nal);
            return Some(out.freeze());
        }
        None
    }
}

fn to_avcc(buf: &mut BytesMut, nal: &[u8]) {
    buf.put_u32(nal.len() as u32);
    buf.put_slice(nal);
}
