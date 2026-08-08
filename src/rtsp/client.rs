use crate::frame::{Codec, MediaFrame};
use crate::rtp::h264::H264Depackatizer;
use crate::rtsp::sdp::{parse_sdp, CodecParams, SdpInfo, StreamInfo};

use base64::{engine::general_purpose::STANDARD as B64, Engine};
use bytes::{BufMut, BytesMut};
use md5;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio::time::timeout;
use url::Url;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub struct RtspConfig {
    pub url:       String,
    pub camera_id: String,
}

pub struct RtspClient {
    config: RtspConfig,
    cseq:   u32,
    tx:     broadcast::Sender<MediaFrame>,
}

enum Auth {
    None,
    Basic(String),
    Digest { realm: String, nonce: String },
}

impl Auth {
    fn header(&self, method: &str, uri: &str, username: &str, password: &str) -> Option<String> {
        match self {
            Auth::None => None,
            Auth::Basic(encoded) => Some(format!("Basic {}", encoded)),
            Auth::Digest { realm, nonce } => {
                Some(digest_auth(method, uri, username, password, realm, nonce))
            }
        }
    }
}

fn digest_auth(method: &str, uri: &str, username: &str, password: &str, realm: &str, nonce: &str) -> String {
    let ha1  = md5_hex(&format!("{}:{}:{}", username, realm, password));
    let ha2  = md5_hex(&format!("{}:{}", method, uri));
    let resp = md5_hex(&format!("{}:{}:{}", ha1, nonce, ha2));
    format!(
        "Digest username=\"{}\", realm=\"{}\", nonce=\"{}\", uri=\"{}\", response=\"{}\"",
        username, realm, nonce, uri, resp
    )
}

fn md5_hex(input: &str) -> String {
    format!("{:x}", md5::compute(input.as_bytes()))
}

async fn read_response(stream: &mut TcpStream) -> Result<String, BoxError> {
    let mut buf = [0u8; 1];
    let mut response = Vec::with_capacity(512);
    loop {
        stream.read_exact(&mut buf).await?;
        response.push(buf[0]);
        if response.ends_with(b"\r\n\r\n") { break; }
    }
    Ok(String::from_utf8_lossy(&response).into_owned())
}

async fn read_body(stream: &mut TcpStream, response: &str) -> Result<String, BoxError> {
    let len = parse_header(response, "content-length")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body).await?;
    Ok(String::from_utf8_lossy(&body).into_owned())
}

fn parse_header<'a>(response: &'a str, key: &str) -> Option<&'a str> {
    let key_lower = key.to_lowercase();
    response.lines().find_map(|line| {
        if line.to_lowercase().starts_with(&key_lower) {
            line.split_once(':').map(|(_, v)| v.trim())
        } else {
            None
        }
    })
}

fn parse_status(response: &str) -> Option<u16> {
    response.lines().next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
}

fn digest_param(header: &str, key: &str) -> Option<String> {
    let search = format!("{}=\"", key);
    let start  = header.find(&search)? + search.len();
    let end    = header[start..].find('"')? + start;
    Some(header[start..end].to_string())
}

fn backoff(attempt: u32) -> u64 {
    1u64.checked_shl(attempt.min(5)).unwrap_or(32).min(60)
}

// ── RTCP helpers ─────────────────────────────────────────────────────────────

/// RTCP Receiver Report with zero report blocks — used as keepalive.
/// Cameras close the connection if they don't receive RTCP feedback.
fn make_rtcp_rr(our_ssrc: u32) -> [u8; 8] {
    let mut pkt = [0u8; 8];
    pkt[0] = 0x80; // V=2, P=0, RC=0
    pkt[1] = 0xC9; // PT=201 (RR)
    pkt[2] = 0x00; // length high
    pkt[3] = 0x01; // length = 1 (2 × 32-bit words − 1)
    pkt[4..8].copy_from_slice(&our_ssrc.to_be_bytes());
    pkt
}

/// RTCP PLI (Picture Loss Indication) — asks the camera for an immediate IDR keyframe.
/// Sent once right after PLAY so the first frame arrives in <1 GOP interval.
fn make_rtcp_pli(our_ssrc: u32, media_ssrc: u32) -> [u8; 12] {
    let mut pkt = [0u8; 12];
    pkt[0] = 0x81; // V=2, P=0, FMT=1 (PLI)
    pkt[1] = 0xCE; // PT=206 (PSFB)
    pkt[2] = 0x00; // length high
    pkt[3] = 0x02; // length = 2 (3 × 32-bit words − 1)
    pkt[4..8].copy_from_slice(&our_ssrc.to_be_bytes());
    pkt[8..12].copy_from_slice(&media_ssrc.to_be_bytes());
    pkt
}

/// Wrap RTCP bytes in an RTSP/TCP interleaved frame and write to the stream.
async fn send_rtcp<W: AsyncWriteExt + Unpin>(
    writer:  &mut W,
    channel: u8,
    payload: &[u8],
) -> Result<(), BoxError> {
    let len = payload.len() as u16;
    let hdr = [b'$', channel, (len >> 8) as u8, len as u8];
    writer.write_all(&hdr).await?;
    writer.write_all(payload).await?;
    Ok(())
}

// ── RTSP methods ──────────────────────────────────────────────────────────────

impl RtspClient {
    pub fn new(config: RtspConfig) -> (Self, broadcast::Receiver<MediaFrame>) {
        let (tx, rx) = broadcast::channel(512);
        (Self { config, cseq: 1, tx }, rx)
    }

    async fn connect(addr: &str) -> Result<TcpStream, BoxError> {
        let stream = timeout(Duration::from_secs(10), TcpStream::connect(addr))
            .await
            .map_err(|_| "connection timed out")?
            .map_err(|e| e.to_string())?;
        stream.set_nodelay(true)?;
        Ok(stream)
    }

    async fn do_options(stream: &mut TcpStream, url: &str, cseq: &mut u32) -> Result<(), BoxError> {
        let req = format!("OPTIONS {} RTSP/1.0\r\nCSeq: {}\r\n\r\n", url, cseq);
        *cseq += 1;
        stream.write_all(req.as_bytes()).await?;
        let resp = read_response(stream).await?;
        match parse_status(&resp) {
            Some(200) => {}
            Some(401) | Some(403) => {
                // Some servers (certain Dahua firmware included) require auth on
                // OPTIONS; others don't ask for it at all. OPTIONS is only a
                // capability probe, not a prerequisite for DESCRIBE — rather than
                // duplicate the full digest challenge/retry here too, treat this
                // as non-fatal and let DESCRIBE (which already authenticates
                // correctly) handle the real handshake.
                eprintln!("OPTIONS requires auth — skipping, DESCRIBE will authenticate");
            }
            Some(s) => return Err(format!("OPTIONS failed: {}", s).into()),
            None => return Err("OPTIONS: could not parse status".into()),
        }
        Ok(())
    }

    async fn do_describe(
        stream:   &mut TcpStream,
        url:      &str,
        cseq:     &mut u32,
        username: &str,
        password: &str,
    ) -> Result<(Auth, String), BoxError> {
        let req = format!(
            "DESCRIBE {} RTSP/1.0\r\nCSeq: {}\r\nAccept: application/sdp\r\n\r\n",
            url, cseq
        );
        *cseq += 1;
        stream.write_all(req.as_bytes()).await?;
        let resp = read_response(stream).await?;

        match parse_status(&resp) {
            Some(200) => { let sdp = read_body(stream, &resp).await?; return Ok((Auth::None, sdp)); }
            Some(401) => {}
            Some(s)   => return Err(format!("DESCRIBE failed: {}", s).into()),
            None      => return Err("DESCRIBE: could not parse status".into()),
        }

        let auth_line = parse_header(&resp, "www-authenticate")
            .ok_or("DESCRIBE 401 missing WWW-Authenticate")?;
        let auth = if auth_line.to_lowercase().starts_with("basic") {
            Auth::Basic(B64.encode(format!("{}:{}", username, password)))
        } else {
            let realm = digest_param(auth_line, "realm").unwrap_or_default();
            let nonce = digest_param(auth_line, "nonce").unwrap_or_default();
            Auth::Digest { realm, nonce }
        };

        let auth_header = auth.header("DESCRIBE", url, username, password)
            .ok_or("auth produced no header")?;
        let req2 = format!(
            "DESCRIBE {} RTSP/1.0\r\nCSeq: {}\r\nAuthorization: {}\r\nAccept: application/sdp\r\n\r\n",
            url, cseq, auth_header
        );
        *cseq += 1;
        stream.write_all(req2.as_bytes()).await?;
        let resp2 = read_response(stream).await?;
        match parse_status(&resp2) {
            Some(200) => {}
            Some(s)   => return Err(format!("DESCRIBE auth retry failed: {}", s).into()),
            None      => return Err("DESCRIBE auth retry: could not parse status".into()),
        }
        let sdp = read_body(stream, &resp2).await?;
        Ok((auth, sdp))
    }

    async fn do_setup(
        stream:      &mut TcpStream,
        setup_url:   &str,
        cseq:        &mut u32,
        username:    &str,
        password:    &str,
        auth_method: &Auth,
    ) -> Result<String, BoxError> {
        let mut req = format!("SETUP {} RTSP/1.0\r\nCSeq: {}\r\n", setup_url, cseq);
        if let Some(auth) = auth_method.header("SETUP", setup_url, username, password) {
            req.push_str(&format!("Authorization: {}\r\n", auth));
        }
        req.push_str("Transport: RTP/AVP/TCP;unicast;interleaved=0-1\r\n\r\n");
        *cseq += 1;
        stream.write_all(req.as_bytes()).await?;
        let resp = read_response(stream).await?;
        match parse_status(&resp) {
            Some(200) => {}
            Some(s)   => return Err(format!("SETUP failed: {}", s).into()),
            None      => return Err("SETUP: could not parse status".into()),
        }
        let session = parse_header(&resp, "session")
            .ok_or("SETUP response missing Session")?
            .split(';').next().unwrap().trim().to_string();
        Ok(session)
    }

    async fn do_setup_audio(
        stream:      &mut TcpStream,
        audio_url:   &str,
        cseq:        &mut u32,
        username:    &str,
        password:    &str,
        auth_method: &Auth,
    ) -> Result<(), BoxError> {
        let mut req = format!("SETUP {} RTSP/1.0\r\nCSeq: {}\r\n", audio_url, cseq);
        if let Some(auth) = auth_method.header("SETUP", audio_url, username, password) {
            req.push_str(&format!("Authorization: {}\r\n", auth));
        }
        req.push_str("Transport: RTP/AVP/TCP;unicast;interleaved=2-3\r\n\r\n");
        *cseq += 1;
        stream.write_all(req.as_bytes()).await?;
        let resp = read_response(stream).await?;
        match parse_status(&resp) {
            Some(200) => Ok(()),
            Some(s)   => Err(format!("SETUP audio failed: {}", s).into()),
            None      => Err("SETUP audio: could not parse status".into()),
        }
    }

    async fn do_play(
        stream:      &mut TcpStream,
        url:         &str,
        cseq:        &mut u32,
        session:     &str,
        username:    &str,
        password:    &str,
        auth_method: &Auth,
    ) -> Result<(), BoxError> {
        let mut req = format!("PLAY {} RTSP/1.0\r\nCSeq: {}\r\nSession: {}\r\n", url, cseq, session);
        if let Some(auth) = auth_method.header("PLAY", url, username, password) {
            req.push_str(&format!("Authorization: {}\r\n", auth));
        }
        req.push_str("Range: npt=0.000-\r\n\r\n");
        *cseq += 1;
        stream.write_all(req.as_bytes()).await?;
        let resp = read_response(stream).await?;
        match parse_status(&resp) {
            Some(200) => Ok(()),
            Some(s)   => Err(format!("PLAY failed: {}", s).into()),
            None      => Err("PLAY: could not parse status".into()),
        }
    }

    async fn do_teardown(
        stream:      &mut TcpStream,
        url:         &str,
        cseq:        &mut u32,
        session:     &str,
        username:    &str,
        password:    &str,
        auth_method: &Auth,
    ) -> Result<(), BoxError> {
        let mut req = format!(
            "TEARDOWN {} RTSP/1.0\r\nCSeq: {}\r\nSession: {}\r\n",
            url, cseq, session
        );
        if let Some(auth) = auth_method.header("TEARDOWN", url, username, password) {
            req.push_str(&format!("Authorization: {}\r\n", auth));
        }
        req.push_str("\r\n");
        *cseq += 1;
        let _ = stream.write_all(req.as_bytes()).await;
        Ok(())
    }

    // ── RTP/RTCP loop ─────────────────────────────────────────────────────────

    async fn rtp_loop(
        stream:    &mut TcpStream,
        camera_id: &str,
        sdp_info:  &SdpInfo,
        tx:        &broadcast::Sender<MediaFrame>,
    ) -> Result<(), BoxError> {
        const OUR_SSRC: u32 = 0x63616D70; // "camp" — arbitrary fixed SSRC for our side

        let (mut reader, mut writer) = stream.split();

        // Send PLI immediately — camera sends IDR keyframe within ~1 frame interval
        // instead of waiting for the next GOP boundary (2-10 seconds on many cameras)
        let pli = make_rtcp_pli(OUR_SSRC, 0);
        let _ = send_rtcp(&mut writer, 1, &pli).await;

        // RTCP RR keepalive — cameras drop the connection after ~30-60s without feedback
        let mut keepalive = tokio::time::interval(Duration::from_secs(5));
        keepalive.tick().await; // skip the first immediate tick

        let mut fu_buf = BytesMut::with_capacity(256 * 1024);
        let frame_codec = match &sdp_info.codec {
            CodecParams::H265 { vps, sps, pps } => Codec::H265 {
                vps: vps.clone(), pps: pps.clone(), sps: sps.clone(),
            },
            CodecParams::H264 { sps, pps } => Codec::H264 {
                sps: sps.clone(), pps: pps.clone(),
            },
        };
        let mut h264 = H264Depackatizer::new();

        loop {
            // Interleaved frame header: $ channel(1B) length(2B BE)
            let mut hdr = [0u8; 4];

            tokio::select! {
                biased;

                // Keepalive fires every 5s — send RTCP RR on RTCP channel (1)
                _ = keepalive.tick() => {
                    let rr = make_rtcp_rr(OUR_SSRC);
                    let _ = send_rtcp(&mut writer, 1, &rr).await;
                    continue;
                }

                result = reader.read_exact(&mut hdr) => {
                    result?;
                }
            }

            if hdr[0] != b'$' { continue; }

            let channel = hdr[1];
            let length  = u16::from_be_bytes([hdr[2], hdr[3]]) as usize;
            let mut pkt = vec![0u8; length];
            reader.read_exact(&mut pkt).await?;

            match channel {
                0 => {
                    // RTP video
                    if pkt.len() < 12 { continue; }
                    let marker    = (pkt[1] & 0x80) != 0;
                    let timestamp = u32::from_be_bytes([pkt[4], pkt[5], pkt[6], pkt[7]]);
                    let payload   = &pkt[12..];
                    if payload.is_empty() { continue; }
                    let pts = (timestamp as u64) * 1_000_000 / sdp_info.clock_rate as u64;

                    match &frame_codec {
                        Codec::H264 { .. } => {
                            if let Some(data) = h264.push(payload, marker) {
                                let is_keyframe = data.len() > 4 && (data[4] & 0x1F) == 5;
                                let _ = tx.send(MediaFrame {
                                    camera_id: camera_id.to_string(),
                                    codec: frame_codec.clone(),
                                    pts, is_keyframe, data,
                                });
                            }
                        }
                        Codec::H265 { .. } => {
                            let nal_type = (payload[0] >> 1) & 0x3F;
                            match nal_type {
                                1..=47 => {
                                    // Single NAL
                                    let mut out = BytesMut::with_capacity(4 + payload.len());
                                    out.put_u32(payload.len() as u32);
                                    out.put_slice(payload);
                                    let is_keyframe = matches!(nal_type, 19 | 20 | 21);
                                    let _ = tx.send(MediaFrame {
                                        camera_id: camera_id.to_string(),
                                        codec: frame_codec.clone(),
                                        pts, is_keyframe,
                                        data: out.freeze(),
                                    });
                                }
                                48 => {
                                    // Aggregation packet
                                    let mut out    = BytesMut::with_capacity(payload.len());
                                    let mut offset = 2;
                                    while offset + 2 <= payload.len() {
                                        let nal_size = u16::from_be_bytes([payload[offset], payload[offset+1]]) as usize;
                                        offset += 2;
                                        if offset + nal_size > payload.len() { break; }
                                        let nal = &payload[offset..offset + nal_size];
                                        out.put_u32(nal.len() as u32);
                                        out.put_slice(nal);
                                        offset += nal_size;
                                    }
                                    if !out.is_empty() {
                                        let _ = tx.send(MediaFrame {
                                            camera_id: camera_id.to_string(),
                                            codec: frame_codec.clone(),
                                            pts, is_keyframe: false,
                                            data: out.freeze(),
                                        });
                                    }
                                }
                                49 => {
                                    // FU (fragmentation)
                                    let fu_hdr  = payload[2];
                                    let start   = (fu_hdr & 0x80) != 0;
                                    let end     = (fu_hdr & 0x40) != 0;
                                    let fu_type = fu_hdr & 0x3F;
                                    if start {
                                        fu_buf.clear();
                                        fu_buf.put_u8((payload[0] & 0x81) | (fu_type << 1));
                                        fu_buf.put_u8(payload[1]);
                                        fu_buf.put_slice(&payload[3..]);
                                    } else {
                                        fu_buf.put_slice(&payload[3..]);
                                    }
                                    if end || marker {
                                        let nal = fu_buf.split().freeze();
                                        let mut out = BytesMut::with_capacity(4 + nal.len());
                                        out.put_u32(nal.len() as u32);
                                        out.put_slice(&nal);
                                        let is_keyframe = matches!(fu_type, 19 | 20);
                                        let _ = tx.send(MediaFrame {
                                            camera_id: camera_id.to_string(),
                                            codec: frame_codec.clone(),
                                            pts, is_keyframe,
                                            data: out.freeze(),
                                        });
                                    }
                                }
                                _ => {}
                            }
                        }
                        _ => {}
                    }
                }
                2 => {
                    // RTP audio
                    if pkt.len() < 12 { continue; }
                    let timestamp  = u32::from_be_bytes([pkt[4], pkt[5], pkt[6], pkt[7]]);
                    let payload    = &pkt[12..];
                    if payload.is_empty() { continue; }
                    let audio_clock = sdp_info.audio.as_ref().map(|a| a.clock_rate).unwrap_or(8000);
                    let pts = (timestamp as u64) * 1_000_000 / audio_clock as u64;
                    let audio_codec = sdp_info.audio.as_ref().map(|a| match &a.codec {
                        crate::rtsp::sdp::AudioCodec::Pcma      => Codec::G711Pcma,
                        crate::rtsp::sdp::AudioCodec::Pcmu      => Codec::G711Pcmu,
                        crate::rtsp::sdp::AudioCodec::Aac { config } => Codec::Aac { config: config.clone() },
                    });
                    if let Some(codec) = audio_codec {
                        let _ = tx.send(MediaFrame {
                            camera_id: camera_id.to_string(),
                            codec, pts, is_keyframe: true,
                            data: bytes::Bytes::copy_from_slice(payload),
                        });
                    }
                }
                1 | 3 => { /* incoming RTCP from camera — ignore, we send our own */ }
                _     => {}
            }
        }
    }

    // ── Entry point ───────────────────────────────────────────────────────────

    pub async fn run(mut self) -> Result<(), BoxError> {
        let mut attempt = 0u32;
        loop {
            match self.connect_and_stream().await {
                Ok(())  => break,
                Err(e)  => {
                    let secs = backoff(attempt);
                    println!("disconnected: {} - retrying in {}s", e, secs);
                    tokio::time::sleep(Duration::from_secs(secs)).await;
                    self.cseq = 1;
                    attempt  += 1;
                }
            }
        }
        Ok(())
    }

    async fn connect_and_stream(&mut self) -> Result<(), BoxError> {
        let parsed   = Url::parse(&self.config.url)?;
        let host     = parsed.host_str().ok_or("missing host")?;
        let port     = parsed.port().unwrap_or(554);
        let addr     = format!("{}:{}", host, port);
        let username = parsed.username().to_string();
        let password = parsed.password().unwrap_or("").to_string();

        let mut stream = Self::connect(&addr).await?;
        println!("connected to {}", addr);

        Self::do_options(&mut stream, &self.config.url, &mut self.cseq).await?;

        let (auth, sdp) = Self::do_describe(
            &mut stream, &self.config.url, &mut self.cseq, &username, &password,
        ).await?;

        let sdp_info  = parse_sdp(&sdp)?;
        let setup_url = sdp_info.control_url.clone();

        let session = Self::do_setup(
            &mut stream, &setup_url, &mut self.cseq, &username, &password, &auth,
        ).await?;

        if let Some(ref audio) = sdp_info.audio {
            Self::do_setup_audio(
                &mut stream, &audio.control_url, &mut self.cseq, &username, &password, &auth,
            ).await?;
        }

        let result = self.play_and_stream(&mut stream, &sdp_info, &session, &auth, &username, &password).await;

        Self::do_teardown(
            &mut stream, &self.config.url, &mut self.cseq, &session, &username, &password, &auth,
        ).await.ok();

        result
    }

    async fn play_and_stream(
        &mut self,
        stream:   &mut TcpStream,
        sdp_info: &SdpInfo,
        session:  &str,
        auth:     &Auth,
        username: &str,
        password: &str,
    ) -> Result<(), BoxError> {
        Self::do_play(stream, &self.config.url, &mut self.cseq, session, username, password, auth).await?;
        println!("streaming camera_id={}", self.config.camera_id);
        Self::rtp_loop(stream, &self.config.camera_id, sdp_info, &self.tx).await
    }

    pub async fn probe(&self) -> Result<StreamInfo, BoxError> {
        let parsed   = Url::parse(&self.config.url)?;
        let host     = parsed.host_str().ok_or("missing host")?;
        let port     = parsed.port().unwrap_or(554);
        let addr     = format!("{}:{}", host, port);
        let username = parsed.username().to_string();
        let password = parsed.password().unwrap_or("").to_string();
        let camera_ip = host.to_string();

        let mut stream = Self::connect(&addr).await?;
        let mut cseq = 1u32;

        Self::do_options(&mut stream, &self.config.url, &mut cseq).await?;
        let (_, sdp) = Self::do_describe(&mut stream, &self.config.url, &mut cseq, &username, &password).await?;
        drop(stream);

        let sdp_info = parse_sdp(&sdp)?;
        Ok(sdp_info.to_stream_info(&self.config.camera_id, &camera_ip, &sdp_info.session_name))
    }
}