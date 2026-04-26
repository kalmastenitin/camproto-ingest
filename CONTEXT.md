# camproto-ingest — Session Context
 
Part of the CamProto VMS stack.
Spec: github.com/kalmastenitin/camproto-spec
 
## What this repo does
RTSP client + RTP depacketizer → MediaFrame on tokio::broadcast channel.
Foundation for camproto-store (recording) and camproto-egress (streaming).
 
## File Structure
```
src/
├── lib.rs                  — pub mod declarations, pub use re-exports
├── frame.rs                — MediaFrame, Codec enum (H264/H265/G711Pcma/G711Pcmu/Aac)
├── rtsp/
│    ├── mod.rs
│    ├── client.rs          — RtspClient, RtspConfig, Auth enum, all RTSP methods,
│    │                        rtp_loop (H.264+H.265+audio), probe(), TEARDOWN
│    └── sdp.rs             — parse_sdp, SdpInfo, StreamInfo, AudioTrack,
│                             CodecParams, AudioCodec, parse_audio_track
└── rtp/
     ├── mod.rs
     └── h264.rs            — H264Depackatizer (FU-A, STAP-A, single NAL → AVCC)
 
src/bin/
└── dev.rs                  — dev runner: probe() then stream, prints all frames
```
 
Note: H.265 depacketization lives inside `rtp_loop` in `client.rs` (not a separate file).
H.264 is a separate struct in `rtp/h264.rs` because it needs stateful FU-A reassembly across packets.
 
## Key Types
 
```rust
// frame.rs
#[derive(Clone)]
pub enum Codec {
    H265 { vps: Bytes, sps: Bytes, pps: Bytes },
    H264 { sps: Bytes, pps: Bytes },
    G711Pcma,
    G711Pcmu,
    Aac { config: Bytes },
}
 
#[derive(Clone)]
pub struct MediaFrame {
    pub camera_id:   String,
    pub codec:       Codec,       // carries VPS/SPS/PPS — fMP4 writer needs these
    pub pts:         u64,         // microseconds, derived from real clock_rate
    pub is_keyframe: bool,        // audio frames: always true
    pub data:        Bytes,       // HVCC/AVCC for video, raw bytes for audio, zero copy
}
 
// rtsp/sdp.rs
pub struct SdpInfo {
    pub codec:        CodecParams,
    pub control_url:  String,
    pub clock_rate:   u32,
    pub width:        u32,
    pub height:       u32,
    pub framerate:    f32,
    pub bitrate:      Option<u32>,
    pub audio:        Option<AudioTrack>,
    pub session_name: String,
}
 
pub struct AudioTrack {
    pub codec:       AudioCodec,
    pub sample_rate: u32,
    pub channels:    u32,
    pub clock_rate:  u32,
    pub control_url: String,
}
 
pub enum AudioCodec { Pcma, Pcmu, Aac { config: Bytes } }
 
pub enum CodecParams {
    H265 { vps: Bytes, sps: Bytes, pps: Bytes },
    H264 { sps: Bytes, pps: Bytes },
}
 
pub struct StreamInfo {
    pub camera_id:    String,
    pub camera_ip:    String,
    pub session_name: String,
    pub video:        VideoInfo,
    pub audio:        Option<AudioInfo>,
}
 
// rtsp/client.rs
pub struct RtspConfig {
    pub url:       String,
    pub camera_id: String,
}
 
enum Auth { None, Basic(String), Digest { realm: String, nonce: String } }
```
 
## Crates
```toml
tokio   = { features = ["full"] }
bytes   = "1"
url     = "2"
base64  = "0.22"
md5     = "0.7"
```
 
## What Works
- [x] TCP connect with 10s timeout
- [x] OPTIONS / DESCRIBE / SETUP / PLAY / TEARDOWN
- [x] Auth: None / Basic / Digest (RFC 2617 HA1/HA2/response)
- [x] TEARDOWN on disconnect (prevents 453 on reconnect)
- [x] Reconnect loop + exponential backoff (1s → 2s → 4s → ... → 60s max)
- [x] SDP parsing — video + audio, codec, VPS/SPS/PPS, control URL,
        clock_rate, framerate, bitrate, dimensions, session name
- [x] Audio SETUP (interleaved=2-3) — only if camera has audio track
- [x] H.265: FU (type 49), single NAL (type 1-47), AP (type 48) → HVCC
- [x] H.264: FU-A (type 28), STAP-A (type 24), single NAL (type 1-23) → AVCC
- [x] H264+ / H265+ / HEVC / AVC codec name aliases
- [x] MJPEG: clear "not yet supported" error before fmtp lookup
- [x] Audio frames: PCMA / PCMU / AAC (channel 2, raw payload passthrough)
- [x] Codec from SDP (not hardcoded)
- [x] clock_rate from SDP (not hardcoded)
- [x] Keyframe detection: H.264 from AVCC NAL type, H.265 from fu_type/nal_type
- [x] MediaFrame with full codec params (VPS/SPS/PPS inside Codec enum)
- [x] Zero copy — BytesMut → split().freeze() → Bytes
- [x] tokio::broadcast fan-out (capacity 128)
- [x] probe() → StreamInfo (DESCRIBE only, no stream started)
- [x] Tested: H.264 1080p, H.265 1080p, H.265 4K 3840x2160, PCMA audio
## Channel Layout (TCP interleaved)
```
Channel 0 → video RTP
Channel 1 → video RTCP (skipped)
Channel 2 → audio RTP (if audio SETUP succeeded)
Channel 3 → audio RTCP (skipped)
```
 
## What's NOT done (future work)
- [ ] H.264 in-band SPS/PPS when camera omits from SDP
- [ ] RTCP SR parsing (for audio/video sync, jitter measurement)
- [ ] ONVIF metadata track (bounding boxes, object detection)
- [ ] SEI NAL parsing (speed camera embedded metadata)
- [ ] UDP RTP mode (TCP interleaved is sufficient for LAN CCTV)
- [ ] MJPEG depacketizer (RFC 2435)
- [ ] AV1 (no cameras ship it yet — reserved in CamProto spec)
## Dev runner
```bash
# stream from camera
cargo run --bin dev
 
# change URL in src/bin/dev.rs:
url: "rtsp://admin:admin@192.168.1.240:554/rtsp/streaming?channel=1&subtype=0".into()
```
 
## Tested cameras
- Sparsh VC-2MP — H.265 1080p 25fps, PCMA audio ✓
- Sparsh VC-2MP — H.264 1080p 25fps ✓
- Sparsh VC-8MP — H.265 4K (3840x2160) 25fps ✓
## Next repo
camproto-store — fMP4 writer + .vlog/.vidx seek index + PostgreSQL segment registry