use bytes::Bytes;

#[derive(Clone)]
pub enum Codec {
    H265,
    H264,
}

#[derive(Clone)]
pub struct MediaFrame {
    pub camera_id: String,
    pub codec: Codec,
    pub pts: u64, // microseconds
    pub is_keyframe: bool,
    pub data: Bytes,
}
