# camproto-ingest
 
RTSP ingest + RTP depacketizer for the [CamProto](https://github.com/kalmastenitin/camproto-spec) VMS stack.
 
Connects to IP cameras via RTSP, depacketizes H.264/H.265 RTP streams,
produces `MediaFrame` structs on a `tokio::broadcast` channel, and optionally
streams audio frames alongside video.
 
---
 
## What it does
 
```
Camera (RTSP/RTP)
    │
    ▼
RtspClient
    ├── probe()                  → StreamInfo (codec, resolution, fps, audio)
    └── run()
         ├── OPTIONS
         ├── DESCRIBE + auth     → SDP (codec params, VPS/SPS/PPS, clock rate)
         ├── SETUP video         → interleaved=0-1
         ├── SETUP audio         → interleaved=2-3 (if camera has audio)
         ├── PLAY
         ├── TCP interleaved RTP loop
         │    ├── channel 0: video RTP → depacketize → MediaFrame
         │    └── channel 2: audio RTP → passthrough → MediaFrame
         └── TEARDOWN (always, even on error)
 
Depacketizers:
    H.265: FU (type 49), single NAL (type 1-47), AP (type 48) → HVCC
    H.264: FU-A (type 28), STAP-A (type 24), single NAL (type 1-23) → AVCC
 
Output:
    tokio::broadcast::Sender<MediaFrame>
        ├── camproto-store  (fMP4 recording)
        └── camproto-egress (MSE / WebRTC live streaming)
```
 
---
 
## Quick start
 
```bash
# Edit src/bin/dev.rs with your camera URL, then:
cargo run --bin dev
```
 
```
=== Stream Info ===
  camera:    cam_001
  ip:        192.168.1.240
  video:     H265 1920x1080 @ 25fps
  audio:     PCMA 8000Hz 1ch
==================
connected to 192.168.1.240:554
streaming camera_id=cam_001
FRAME pts=0.256s keyframe=true  size=146270B
FRAME pts=0.296s keyframe=false size=4812B
FRAME pts=6.872s keyframe=true  size=384B    ← audio
...
```
 
---
 
## Usage as a library
 
```rust
use camproto_ingest::{RtspClient, RtspConfig};
 
#[tokio::main]
async fn main() {
    let config = RtspConfig {
        url:       "rtsp://admin:admin@192.168.1.50:554/stream1".into(),
        camera_id: "cam_001".into(),
    };
 
    let (client, mut rx) = RtspClient::new(config);
 
    // probe without streaming
    if let Ok(info) = client.probe().await {
        println!("{} {}x{} @ {}fps",
            info.video.codec, info.video.width,
            info.video.height, info.video.framerate);
    }
 
    // spawn ingest
    tokio::spawn(async move {
        client.run().await.unwrap();
    });
 
    // consume frames
    loop {
        match rx.recv().await {
            Ok(frame) => {
                println!("pts={:.3}s keyframe={} size={}B codec={}",
                    frame.pts as f64 / 1_000_000.0,
                    frame.is_keyframe,
                    frame.data.len(),
                    match frame.codec {
                        camproto_ingest::frame::Codec::H265 { .. } => "H265",
                        camproto_ingest::frame::Codec::H264 { .. } => "H264",
                        camproto_ingest::frame::Codec::G711Pcma    => "PCMA",
                        camproto_ingest::frame::Codec::G711Pcmu    => "PCMU",
                        camproto_ingest::frame::Codec::Aac { .. }  => "AAC",
                    }
                );
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                eprintln!("dropped {} frames", n);
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
        }
    }
}
```
 
---
 
## MediaFrame
 
```rust
#[derive(Clone)]
pub struct MediaFrame {
    pub camera_id:   String,
    pub codec:       Codec,    // H265/H264 carry VPS/SPS/PPS for fMP4 muxer
    pub pts:         u64,      // microseconds (from real RTP clock_rate)
    pub is_keyframe: bool,     // audio frames: always true
    pub data:        Bytes,    // HVCC (H.265) or AVCC (H.264) or raw audio bytes
}
 
#[derive(Clone)]
pub enum Codec {
    H265 { vps: Bytes, sps: Bytes, pps: Bytes },
    H264 { sps: Bytes, pps: Bytes },
    G711Pcma,
    G711Pcmu,
    Aac { config: Bytes },
}
```
 
Video data is always in **AVCC/HVCC format** (4-byte length prefix per NAL unit).
This is what fMP4 expects — no conversion needed at the muxer.
 
Audio data is raw codec bytes from the RTP payload — no reassembly needed for G.711.
 
---
 
## Supported cameras
 
| Codec | RTP packaging | Status |
|---|---|---|
| H.265 / HEVC / H.265+ | FU (49), single NAL (1-47), AP (48) | ✅ |
| H.264 / AVC / H.264+ | FU-A (28), STAP-A (24), single NAL (1-23) | ✅ |
| PCMA (G.711 A-law) | raw passthrough | ✅ |
| PCMU (G.711 μ-law) | raw passthrough | ✅ |
| AAC | raw passthrough | ✅ |
| MJPEG | — | ❌ not yet supported |
| AV1 | — | ❌ reserved for future |
 
---
 
## Authentication
 
| Method | Status |
|---|---|
| None (camera returns 200 on first DESCRIBE) | ✅ |
| Basic (RFC 2068) | ✅ |
| Digest (RFC 2617, HA1/HA2/response) | ✅ |
 
Credentials are parsed from the RTSP URL:
`rtsp://username:password@192.168.1.50:554/stream`
 
---
 
## Reconnect behaviour
 
On disconnect (network loss, camera reboot, error), `run()` automatically:
 
1. Sends TEARDOWN (best effort, prevents 453 on reconnect)
2. Waits with exponential backoff: 1s → 2s → 4s → 8s → 16s → 32s → 60s (max)
3. Reconnects and resumes from OPTIONS
---
 
## probe() — stream info without streaming
 
```rust
let info: StreamInfo = client.probe().await?;
 
// info.video: codec, width, height, framerate, bitrate, has_sps, has_vps
// info.audio: codec, sample_rate, channels (None if no audio track)
```
 
Used by `camproto-control` (Go) to validate a camera URL before saving it,
and by the NVR client (egui) to show camera properties.
 
---
 
## Zero copy
 
Video frames use `BytesMut::split().freeze()` at every GOP boundary.
Broadcasting a `MediaFrame` to N subscribers = N reference count increments,
not N copies of the video data. `Bytes::clone()` is O(1).
 
---
 
## Cargo.toml
 
```toml
[dependencies]
tokio  = { version = "1", features = ["full"] }
bytes  = "1"
url    = "2"
base64 = "0.22"
md5    = "0.7"
```
 
---
 
## Tested cameras
 
| Camera | Codec | Resolution | FPS | Audio |
|---|---|---|---|---|
| Sparsh VC-2MP | H.265 | 1920×1080 | 25 | PCMA 8kHz |
| Sparsh VC-2MP | H.264 | 1920×1080 | 25 | PCMA 8kHz |
| Sparsh VC-8MP | H.265 | 3840×2160 | 25 | PCMA 8kHz |
 
---
 
## Part of CamProto
 
| Repo | Description |
|---|---|
| [camproto-spec](https://github.com/kalmastenitin/camproto-spec) | Protocol spec + .proto files |
| **camproto-ingest** | RTSP ingest + RTP depacketizer ← you are here |
| camproto-store | fMP4 recording + .vlog/.vidx storage |
| camproto-egress | MSE + WebRTC live streaming |
| camproto-transport | QUIC + NAT traversal |
| camproto-control | Go control plane + HTTP API |
| camproto-nvr | egui desktop NVR client |
 
---
 
## License
 
Apache 2.0