# camproto-ingest

Part of CamProto VMS stack.
Spec: github.com/yourname/camproto-spec

## What this does
RTSP client + RTP depacketizer → MediaFrame on tokio broadcast channel.

## Status
- [ ] RTSP DESCRIBE/SETUP/PLAY
- [ ] SDP parser
- [ ] H.264 depacketizer
- [ ] H.265 depacketizer
- [ ] Fan-out bus

## MediaFrame
pts, dts (microseconds), is_keyframe, codec, data: Bytes

## Tested cameras
- CP Plus CP-UNC-DA21PL3C (firmware 2.x)
- Sparsh VC-2MP