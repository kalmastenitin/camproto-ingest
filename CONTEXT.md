# camproto-ingest — Session Context

Part of the CamProto VMS stack.
Spec: github.com/yourname/camproto-spec

## What this repo does
RTSP client + RTP depacketizer → MediaFrame on tokio::broadcast channel.
Foundation for camproto-store (recording) and camproto-egress (streaming).

## Architecture
```
Camera RTSP/RTP
    │
    ▼
RtspClient::run()                     src/rtsp/client.rs
    ├── DESCRIBE → parse_sdp()        src/rtsp/sdp.rs
    ├── Auth (Basic/Digest)           src/rtsp/auth.rs
    ├── SETUP + PLAY
    └── TCP interleaved RTP loop
            │
            ▼
        RtpDepacketizer::push()       src/rtp/depack.rs
            ├── H264Depacketizer      src/rtp/h264.rs
            └── H265Depacketizer      src/rtp/h265.rs
                        │
                        ▼
                 MediaFrame            src/frame.rs
                        │
                 tokio::broadcast
```

## Key Types
```rust
// frame.rs
#[derive(Clone)]
pub enum Codec { H265, H264 }

#[derive(Clone)]
pub struct MediaFrame {
    pub camera_id:   String,
    pub codec:       Codec,
    pub pts:         u64,     // microseconds
    pub is_keyframe: bool,
    pub data:        Bytes,   // zero copy — split().freeze() from BytesMut
}

// client.rs
enum Auth {
    None,
    Basic(String),                        // stores base64(user:pass)
    Digest { realm: String, nonce: String },
}
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
- [x] OPTIONS
- [x] DESCRIBE — 200 direct or 401 + retry
- [x] Basic auth
- [x] Digest auth (RFC 2617 HA1/HA2/response)
- [x] Auth::None (cameras that need no auth)
- [x] SDP parsing — control URL extraction
- [x] SETUP — session ID extraction
- [x] PLAY
- [x] TCP interleaved RTP receive loop
- [x] H.265 FU reassembly (NAL type 49)
- [x] MediaFrame production (zero copy Bytes)
- [x] tokio::broadcast fan-out (channel capacity 128)
- [x] Reconnect loop with exponential backoff
- [x] Tested on real Sparsh camera — 25fps H.265, keyframe every ~2s

## What's Missing
- [ ] H.264 depacketizer (FU-A, STAP-A, single NAL)
- [ ] H.265 single NAL (type 1-47) and AP (type 48)
- [ ] UDP RTP mode (TCP interleaved only)
- [ ] SPS/PPS in MediaFrame (needed by camproto-store for fMP4 moov)
- [ ] Codec extracted from SDP (currently hardcoded H265)
- [ ] Resolution from SDP (currently not parsed)

## Biggest Known Gap
Codec is hardcoded as H265 in rtp_loop. Need to:
1. Parse codec from SDP a=rtpmap line
2. Pass codec + SPS/PPS into rtp_loop
3. Depacketize based on codec type

## Dev runner
cargo run --bin dev

## Next step
H.264 depacketizer OR camproto-store (fMP4 writer)
```