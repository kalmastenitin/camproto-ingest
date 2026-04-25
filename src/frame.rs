use bytes::Bytes;

#[derive(Clone)]
pub enum Codec {
    H265 { vps: Bytes, sps: Bytes, pps: Bytes },
    H264 { sps: Bytes, pps: Bytes },
}

#[derive(Clone)]
pub struct MediaFrame {
    pub camera_id: String,
    pub codec: Codec,
    pub pts: u64, // microseconds
    pub is_keyframe: bool,
    pub data: Bytes,
}
