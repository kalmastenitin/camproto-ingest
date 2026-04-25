# camproto-ingest — Session Context

Part of the CamProto VMS stack.
Spec: github.com/kalmastenitin/camproto-spec

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
pub enum Codec {
    H265 { vps: Bytes, sps: Bytes, pps: Bytes },
    H264 { sps: Bytes, pps: Bytes },
}

#[derive(Clone)]
pub struct MediaFrame {
    pub camera_id:   String,
    pub codec:       Codec,   // carries VPS/SPS/PPS — fMP4 writer needs these
    pub pts:         u64,     // microseconds, from real clock_rate
    pub is_keyframe: bool,
    pub data:        Bytes,   // HVCC/AVCC format, zero copy
}

// rtsp/sdp.rs
pub struct SdpInfo {
    pub codec:       CodecParams,
    pub control_url: String,
    pub clock_rate:  u32,
    pub width:       u32,
    pub height:      u32,
}

pub enum CodecParams {
    H265 { vps: Bytes, sps: Bytes, pps: Bytes },
    H264 { sps: Bytes, pps: Bytes },
}

// rtsp/client.rs
enum Auth { None, Basic(String), Digest { realm: String, nonce: String } }
```

## What Works
- [x] TCP connect + 10s timeout
- [x] OPTIONS / DESCRIBE / SETUP / PLAY
- [x] Basic + Digest + None auth
- [x] SDP parsing — codec, VPS/SPS/PPS, control URL, clock_rate, dimensions
- [x] H.265 FU reassembly (NAL type 49)
- [x] MediaFrame with real codec params (VPS/SPS/PPS inside Codec enum)
- [x] clock_rate from SDP (not hardcoded)
- [x] codec from SDP (not hardcoded)
- [x] Zero copy — Bytes, split().freeze()
- [x] tokio::broadcast fan-out (capacity 128)
- [x] Reconnect loop + exponential backoff (1s→2s→4s→...→60s)
- [x] Tested on real Sparsh camera — H.265 1080p 25fps

## What's Missing
- [ ] H.264 depacketizer (FU-A, STAP-A, single NAL)
- [ ] H.265 single NAL (type 1-47) and AP (type 48)
- [ ] UDP RTP mode

## Dev runner
cargo run --bin dev

## Next repo
camproto-store — fMP4 writer + .vlog/.vidx index + PostgreSQL registry