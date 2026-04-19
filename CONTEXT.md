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
MediaFrame {
    camera_id:        String,
    codec:            Codec,     // H264(params) | H265(params) | Aac | ...
    pts:              u64,       // microseconds
    dts:              u64,       // microseconds
    is_keyframe:      bool,
    is_discontinuity: bool,
    data:             Bytes,     // AVCC/HVCC format
}
```

## Crates used
- tokio (full), bytes, url, base64, md5, nom, thiserror

## Status
- [x] Project structure + Cargo.toml
- [ ] MediaFrame + Codec types (src/frame.rs)
- [ ] Error types (src/error.rs)
- [ ] RTSP state machine (OPTIONS/DESCRIBE/SETUP/PLAY) (src/rtsp/client.rs)
- [ ] Basic + Digest auth (src/rtsp/auth.rs)
- [ ] SDP parser — codec, control URL, resolution (src/rtsp/sdp.rs)
- [ ] RTP packet parser (src/rtp/packet.rs)
- [ ] H.264 depacketizer — FU-A, STAP-A, single NAL (src/rtp/h264.rs)
- [ ] H.265 depacketizer — FU, AP, single NAL (src/rtp/h265.rs)
- [ ] RTP dispatcher + PTS clock (src/rtp/depack.rs)
- [ ] Dev runner binary (src/bin/dev.rs)
- [ ] Test on real CP Plus camera
- [ ] Test on real Sparsh camera
- [ ] Handle H.264 in-band SPS/PPS (camera doesn't send in SDP)
- [ ] Reconnect loop with backoff (currently run() returns on disconnect)
- [ ] UDP RTP mode (TCP interleaved only for now)

## Known gaps / next tasks
1. Reconnect loop — RtspClient::run() needs outer retry loop with backoff
2. Real camera testing — buy CP Plus + Sparsh, run ingest-dev binary
3. H.264 in-band SPS/PPS — some cameras omit from SDP, send only in-stream

## Test cameras
- [ ] CP Plus CP-UNC-DA21PL3C
- [ ] Sparsh VC-2MP-FA20MS

## Dev runner
cargo run --bin ingest-dev -- rtsp://admin:admin@192.168.1.50:554/stream1 cam_001
```