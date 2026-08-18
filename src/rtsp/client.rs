use crate::frame::{Codec, MediaFrame};
use crate::rtp::h264::H264Depackatizer;
use crate::rtsp::sdp::{parse_sdp, CodecParams, SdpInfo, StreamInfo};

use base64::{engine::general_purpose::STANDARD as B64, Engine};
use bytes::{BufMut, Bytes, BytesMut};
use md5;
use percent_encoding::percent_decode_str;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio::time::timeout;
use url::Url;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Default)]
pub struct RtspConfig {
    pub url: String,
    pub camera_id: String,
    pub playback_start: Option<String>,
    pub playback_end: Option<String>,
    pub playback_scale: Option<f32>,
}

pub struct RtspClient {
    config: RtspConfig,
    cseq: u32,
    tx: broadcast::Sender<MediaFrame>,
}

enum Auth {
    None,
    Basic(String),
    Digest {
        realm: String,
        nonce: String,
        qop: Option<String>,
    },
}

// ── RTCP helpers ─────
fn h265_nal_type(nal: &[u8]) -> Option<u8> {
    if nal.len() < 2 {
        return None;
    }

    Some((nal[0] >> 1) & 0x3F)
}

fn is_h265_keyframe(nal_type: u8) -> bool {
    matches!(nal_type, 16..=21)
}

fn append_h265_nal(out: &mut BytesMut, nal: &[u8]) {
    out.put_u32(nal.len() as u32);
    out.put_slice(nal);
}

fn update_h265_parameter_sets(nal: &[u8], vps: &mut Bytes, sps: &mut Bytes, pps: &mut Bytes) {
    let Some(nal_type) = h265_nal_type(nal) else {
        return;
    };

    match nal_type {
        32 => {
            *vps = Bytes::copy_from_slice(nal);
        }

        33 => {
            *sps = Bytes::copy_from_slice(nal);
        }

        34 => {
            *pps = Bytes::copy_from_slice(nal);
        }

        _ => {}
    }
}

fn publish_h265_access_unit(
    tx: &broadcast::Sender<MediaFrame>,
    camera_id: &str,
    data: Bytes,
    pts: u64,
    is_keyframe: bool,
    vps: &Bytes,
    sps: &Bytes,
    pps: &Bytes,
) {
    if data.is_empty() {
        return;
    }

    let _ = tx.send(MediaFrame {
        camera_id: camera_id.to_string(),

        codec: Codec::H265 {
            vps: vps.clone(),
            sps: sps.clone(),
            pps: pps.clone(),
        },

        pts,

        is_keyframe,

        data,
    });
}

impl Auth {
    fn header(&self, method: &str, uri: &str, username: &str, password: &str) -> Option<String> {
        match self {
            Auth::None => None,
            Auth::Basic(encoded) => Some(format!("Basic {}", encoded)),
            Auth::Digest { realm, nonce, qop } => Some(digest_auth(
                method, uri, username, password, realm, nonce, qop,
            )),
        }
    }
}

fn digest_auth(
    method: &str,
    uri: &str,
    username: &str,
    password: &str,
    realm: &str,
    nonce: &str,
    qop: &Option<String>,
) -> String {
    let ha1 = md5_hex(&format!("{}:{}:{}", username, realm, password));
    let ha2 = md5_hex(&format!("{}:{}", method, uri));

    match qop {
        Some(q) if q.split(',').any(|s| s.trim() == "auth") => {
            let cnonce = format!(
                "{:x}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            );
            let nc = "00000001";
            let resp = md5_hex(&format!("{}:{}:{}:{}:auth:{}", ha1, nonce, nc, cnonce, ha2));
            format!(
                "Digest username=\"{}\", realm=\"{}\", nonce=\"{}\", uri=\"{}\", \
                 response=\"{}\", qop=auth, nc={}, cnonce=\"{}\"",
                username, realm, nonce, uri, resp, nc, cnonce
            )
        }
        _ => {
            // legacy RFC 2069, no qop
            let resp = md5_hex(&format!("{}:{}:{}", ha1, nonce, ha2));
            format!(
                "Digest username=\"{}\", realm=\"{}\", nonce=\"{}\", uri=\"{}\", response=\"{}\"",
                username, realm, nonce, uri, resp
            )
        }
    }
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
        if response.ends_with(b"\r\n\r\n") {
            break;
        }
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
    response
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
}

fn digest_param(header: &str, key: &str) -> Option<String> {
    let search = format!("{}=\"", key);
    let start = header.find(&search)? + search.len();
    let end = header[start..].find('"')? + start;
    Some(header[start..end].to_string())
}

fn resolve_control_url(base: &str, control: &str) -> String {
    if control == "*" {
        return base.to_string();
    }
    if control.starts_with("rtsp://") || control.starts_with("rtsps://") {
        return control.to_string();
    }
    if base.ends_with('/') {
        format!("{base}{control}")
    } else {
        format!("{base}/{control}")
    }
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
    writer: &mut W,
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
        (
            Self {
                config,
                cseq: 1,
                tx,
            },
            rx,
        )
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
        stream: &mut TcpStream,
        url: &str,
        cseq: &mut u32,
        username: &str,
        password: &str,
    ) -> Result<(Auth, String, String), BoxError> {
        // added: base_url
        let req = format!(
            "DESCRIBE {} RTSP/1.0\r\nCSeq: {}\r\nAccept: application/sdp\r\n\r\n",
            url, cseq
        );
        *cseq += 1;
        stream.write_all(req.as_bytes()).await?;
        let resp = read_response(stream).await?;

        match parse_status(&resp) {
            Some(200) => {
                let sdp = read_body(stream, &resp).await?;
                // Content-Base is the correct RFC-2326 base for resolving
                // relative a=control values; fall back to the request URL
                // if the server doesn't send one.
                let base_url = parse_header(&resp, "content-base")
                    .map(|s| s.trim().to_string())
                    .unwrap_or_else(|| url.to_string());
                return Ok((Auth::None, sdp, base_url));
            }
            Some(401) => {}
            Some(s) => return Err(format!("DESCRIBE failed: {}", s).into()),
            None => return Err("DESCRIBE: could not parse status".into()),
        }

        let auth_line = parse_header(&resp, "www-authenticate")
            .ok_or("DESCRIBE 401 missing WWW-Authenticate")?;
        let auth = if auth_line.to_lowercase().starts_with("basic") {
            Auth::Basic(B64.encode(format!("{}:{}", username, password)))
        } else {
            let realm = digest_param(auth_line, "realm").unwrap_or_default();
            let nonce = digest_param(auth_line, "nonce").unwrap_or_default();
            let qop = digest_param(auth_line, "qop");
            Auth::Digest { realm, nonce, qop }
        };

        let auth_header = auth
            .header("DESCRIBE", url, username, password)
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
            Some(s) => return Err(format!("DESCRIBE auth retry failed: {}", s).into()),
            None => return Err("DESCRIBE auth retry: could not parse status".into()),
        }
        let sdp = read_body(stream, &resp2).await?;
        let base_url = parse_header(&resp2, "content-base")
            .map(|s| s.trim().to_string())
            .unwrap_or_else(|| url.to_string());
        Ok((auth, sdp, base_url))
    }

    async fn do_setup(
        stream: &mut TcpStream,
        setup_url: &str,
        cseq: &mut u32,
        username: &str,
        password: &str,
        auth_method: &Auth,
        is_playback: bool,
    ) -> Result<String, BoxError> {
        let mut req = format!("SETUP {} RTSP/1.0\r\nCSeq: {}\r\n", setup_url, cseq);
        if let Some(auth) = auth_method.header("SETUP", setup_url, username, password) {
            req.push_str(&format!("Authorization: {}\r\n", auth));
        }

        if is_playback {
            req.push_str("Require: onvif-replay\r\n");
        }

        req.push_str("Transport: RTP/AVP/TCP;unicast;interleaved=0-1\r\n\r\n");
        *cseq += 1;
        stream.write_all(req.as_bytes()).await?;
        let resp = read_response(stream).await?;
        match parse_status(&resp) {
            Some(200) => {}
            Some(s) => return Err(format!("SETUP failed: {}", s).into()),
            None => return Err("SETUP: could not parse status".into()),
        }
        let session = parse_header(&resp, "session")
            .ok_or("SETUP response missing Session")?
            .split(';')
            .next()
            .unwrap()
            .trim()
            .to_string();
        Ok(session)
    }

    async fn do_setup_audio(
        stream: &mut TcpStream,
        audio_url: &str,
        cseq: &mut u32,
        username: &str,
        password: &str,
        auth_method: &Auth,
        session: &str,
        is_playback: bool,
    ) -> Result<(), BoxError> {
        let mut req = format!(
            "SETUP {} RTSP/1.0\r\nCSeq: {}\r\nSession: {}\r\n",
            audio_url, cseq, session
        );

        if let Some(auth) = auth_method.header("SETUP", audio_url, username, password) {
            req.push_str(&format!("Authorization: {}\r\n", auth));
        }

        if is_playback {
            req.push_str("Require: onvif-replay\r\n");
        }
        req.push_str("Transport: RTP/AVP/TCP;unicast;interleaved=2-3\r\n\r\n");
        *cseq += 1;
        stream.write_all(req.as_bytes()).await?;

        let resp = read_response(stream).await?;
        match parse_status(&resp) {
            Some(200) => Ok(()),
            Some(s) => Err(format!("SETUP audio failed: {}", s).into()),
            None => Err("SETUP audio: could not parse status".into()),
        }
    }

    async fn do_play(
        stream: &mut TcpStream,
        url: &str,
        cseq: &mut u32,
        session: &str,
        username: &str,
        password: &str,
        auth_method: &Auth,
        playback_start: Option<&str>,
        playback_end: Option<&str>,
        playback_scale: Option<f32>,
    ) -> Result<(), BoxError> {
        let is_playback = playback_start.is_some();

        let mut req = format!(
            "PLAY {} RTSP/1.0\r\nCSeq: {}\r\nSession: {}\r\n",
            url, cseq, session
        );

        if let Some(auth) = auth_method.header("PLAY", url, username, password) {
            req.push_str(&format!("Authorization: {}\r\n", auth));
        }

        if is_playback {
            // Required ONVIF replay feature tag.
            req.push_str("Require: onvif-replay\r\n");

            match (playback_start, playback_end) {
                (Some(start), Some(end)) => {
                    let start = rtsp_clock(start);
                    let end = rtsp_clock(end);

                    req.push_str(&format!("Range: clock={start}-{end}\r\n"));
                }

                (Some(start), None) => {
                    let start = rtsp_clock(start);

                    req.push_str(&format!("Range: clock={start}-\r\n"));
                }

                _ => {}
            }

            // For now leave rate control enabled so the NVR sends approximately
            // real-time playback. Absence of this header means "yes" per ONVIF.
            //
            // Later, for fast export/download:
            // req.push_str("Rate-Control: no\r\n");
        } else {
            // Normal live stream.
            req.push_str("Range: npt=0.000-\r\n");
        }

        if let Some(scale) = playback_scale {
            if (scale - 1.0).abs() > f32::EPSILON {
                req.push_str(&format!("Scale: {scale}\r\n"));
            }
        }

        req.push_str("\r\n");

        *cseq += 1;

        eprintln!(
            "DEBUG PLAY request:\n{}",
            req.lines()
                .filter(|line| !line.starts_with("Authorization:"))
                .collect::<Vec<_>>()
                .join("\n")
        );

        stream.write_all(req.as_bytes()).await?;

        let resp = read_response(stream).await?;

        eprintln!("DEBUG PLAY response:\n{resp}");

        match parse_status(&resp) {
            Some(200) => Ok(()),
            Some(status) => Err(format!("PLAY failed: status={status}\n{resp}").into()),
            None => Err(format!("PLAY: could not parse status\n{resp}").into()),
        }
    }

    async fn do_teardown(
        stream: &mut TcpStream,
        url: &str,
        cseq: &mut u32,
        session: &str,
        username: &str,
        password: &str,
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
        stream: &mut TcpStream,
        camera_id: &str,
        sdp_info: &SdpInfo,
        tx: &broadcast::Sender<MediaFrame>,
    ) -> Result<(), BoxError> {
        const OUR_SSRC: u32 = 0x63616D70;

        let (mut reader, mut writer) = stream.split();

        // Request an IDR/keyframe.
        let pli = make_rtcp_pli(OUR_SSRC, 0);
        let _ = send_rtcp(&mut writer, 1, &pli).await;

        // RTCP keepalive.
        let mut keepalive = tokio::time::interval(Duration::from_secs(5));

        keepalive.tick().await;

        // ---------------------------------------------------------
        // RTP timestamp normalization
        // ---------------------------------------------------------

        let mut video_base_timestamp: Option<u32> = None;
        let mut audio_base_timestamp: Option<u32> = None;

        // ---------------------------------------------------------
        // H264 state
        // ---------------------------------------------------------

        let mut h264 = H264Depackatizer::new();

        // ---------------------------------------------------------
        // H265 FU reconstruction
        // ---------------------------------------------------------

        let mut fu_buf = BytesMut::with_capacity(256 * 1024);

        // ---------------------------------------------------------
        // H265 Access Unit aggregation
        // ---------------------------------------------------------

        let mut h265_au = BytesMut::with_capacity(512 * 1024);

        let mut h265_au_timestamp: Option<u32> = None;

        let mut h265_au_pts: u64 = 0;

        let mut h265_au_keyframe = false;

        let mut h265_au_has_vcl = false;

        // ---------------------------------------------------------
        // H265 parameter-set cache
        // ---------------------------------------------------------

        let (mut h265_vps, mut h265_sps, mut h265_pps) = match &sdp_info.codec {
            CodecParams::H265 { vps, sps, pps } => (vps.clone(), sps.clone(), pps.clone()),

            _ => (Bytes::new(), Bytes::new(), Bytes::new()),
        };

        // ---------------------------------------------------------
        // Base codec
        // ---------------------------------------------------------

        let frame_codec = match &sdp_info.codec {
            CodecParams::H265 { vps, sps, pps } => Codec::H265 {
                vps: vps.clone(),
                sps: sps.clone(),
                pps: pps.clone(),
            },

            CodecParams::H264 { sps, pps } => Codec::H264 {
                sps: sps.clone(),
                pps: pps.clone(),
            },
        };

        loop {
            let mut hdr = [0u8; 4];

            tokio::select! {
                biased;

                _ = keepalive.tick() => {
                    let rr = make_rtcp_rr(OUR_SSRC);

                    let _ =
                        send_rtcp(&mut writer, 1, &rr).await;

                    continue;
                }

                result = reader.read_exact(&mut hdr) => {
                    result?;
                }
            }

            // RTSP/TCP interleaved RTP begins with '$'
            if hdr[0] != b'$' {
                continue;
            }

            let channel = hdr[1];

            let length = u16::from_be_bytes([hdr[2], hdr[3]]) as usize;

            let mut pkt = vec![0u8; length];

            reader.read_exact(&mut pkt).await?;

            match channel {
                // =================================================
                // VIDEO RTP
                // =================================================
                0 => {
                    let Some(off) = rtp_payload_offset(&pkt) else {
                        continue;
                    };

                    let marker = (pkt[1] & 0x80) != 0;

                    let timestamp = u32::from_be_bytes([pkt[4], pkt[5], pkt[6], pkt[7]]);

                    let payload = &pkt[off..];

                    if payload.is_empty() {
                        continue;
                    }

                    // ---------------------------------------------
                    // Normalize RTP timestamp so stream starts at 0
                    // ---------------------------------------------

                    let base_timestamp = *video_base_timestamp.get_or_insert(timestamp);

                    let relative_timestamp = timestamp.wrapping_sub(base_timestamp);

                    let pts = relative_timestamp as u64 * 1_000_000 / sdp_info.clock_rate as u64;

                    match &frame_codec {
                        // =========================================
                        // H264
                        // =========================================
                        Codec::H264 { .. } => {
                            if let Some(data) = h264.push(payload, marker) {
                                let is_keyframe = data.len() > 4 && (data[4] & 0x1F) == 5;

                                let _ = tx.send(MediaFrame {
                                    camera_id: camera_id.to_string(),

                                    codec: frame_codec.clone(),

                                    pts,

                                    is_keyframe,

                                    data,
                                });
                            }
                        }

                        // =========================================
                        // H265
                        // =========================================
                        Codec::H265 { .. } => {
                            if payload.len() < 2 {
                                continue;
                            }

                            let nal_type = (payload[0] >> 1) & 0x3F;

                            // -------------------------------------
                            // New timestamp means new access unit.
                            //
                            // Normally marker=true flushes the AU.
                            // This is a safety fallback for cameras
                            // with unreliable marker behaviour.
                            // -------------------------------------

                            if let Some(old_timestamp) = h265_au_timestamp {
                                if old_timestamp != timestamp {
                                    if !h265_au.is_empty() && h265_au_has_vcl {
                                        let data = h265_au.split().freeze();

                                        publish_h265_access_unit(
                                            tx,
                                            camera_id,
                                            data,
                                            h265_au_pts,
                                            h265_au_keyframe,
                                            &h265_vps,
                                            &h265_sps,
                                            &h265_pps,
                                        );
                                    } else {
                                        h265_au.clear();
                                    }

                                    h265_au_keyframe = false;

                                    h265_au_has_vcl = false;

                                    fu_buf.clear();
                                }
                            }

                            if h265_au_timestamp != Some(timestamp) {
                                h265_au_timestamp = Some(timestamp);

                                h265_au_pts = pts;

                                h265_au_keyframe = false;

                                h265_au_has_vcl = false;
                            }

                            // -------------------------------------
                            // A packet can result in one or more
                            // complete NAL units.
                            // -------------------------------------

                            let mut complete_nals: Vec<Vec<u8>> = Vec::new();

                            match nal_type {
                                // =================================
                                // Single NAL Unit
                                // =================================
                                0..=47 => {
                                    complete_nals.push(payload.to_vec());
                                }

                                // =================================
                                // Aggregation Packet (AP)
                                // =================================
                                48 => {
                                    let mut offset = 2;

                                    while offset + 2 <= payload.len() {
                                        let nal_size = u16::from_be_bytes([
                                            payload[offset],
                                            payload[offset + 1],
                                        ])
                                            as usize;

                                        offset += 2;

                                        if offset + nal_size > payload.len() {
                                            break;
                                        }

                                        complete_nals
                                            .push(payload[offset..offset + nal_size].to_vec());

                                        offset += nal_size;
                                    }
                                }

                                // =================================
                                // Fragmentation Unit (FU)
                                // =================================
                                49 => {
                                    if payload.len() < 3 {
                                        continue;
                                    }

                                    let fu_header = payload[2];

                                    let start = (fu_header & 0x80) != 0;

                                    let end = (fu_header & 0x40) != 0;

                                    let fu_type = fu_header & 0x3F;

                                    if start {
                                        fu_buf.clear();

                                        // Reconstruct original
                                        // H265 two-byte NAL header.
                                        fu_buf.put_u8((payload[0] & 0x81) | (fu_type << 1));

                                        fu_buf.put_u8(payload[1]);

                                        fu_buf.put_slice(&payload[3..]);
                                    } else {
                                        if fu_buf.is_empty() {
                                            // We lost the FU start.
                                            continue;
                                        }

                                        fu_buf.put_slice(&payload[3..]);
                                    }

                                    if end && !fu_buf.is_empty() {
                                        complete_nals.push(fu_buf.split().to_vec());
                                    }
                                }

                                _ => {}
                            }

                            // -------------------------------------
                            // Process completed H265 NALs
                            // -------------------------------------

                            for nal in complete_nals {
                                let Some(current_type) = h265_nal_type(&nal) else {
                                    continue;
                                };

                                // Capture VPS/SPS/PPS arriving
                                // inside RTP.
                                update_h265_parameter_sets(
                                    &nal,
                                    &mut h265_vps,
                                    &mut h265_sps,
                                    &mut h265_pps,
                                );

                                // H265 VCL range is 0..31.
                                if current_type <= 31 {
                                    h265_au_has_vcl = true;
                                }

                                if is_h265_keyframe(current_type) {
                                    h265_au_keyframe = true;
                                }

                                append_h265_nal(&mut h265_au, &nal);
                            }

                            // -------------------------------------
                            // RTP marker = end of picture/AU
                            // -------------------------------------

                            if marker {
                                if !h265_au.is_empty() && h265_au_has_vcl {
                                    let data = h265_au.split().freeze();

                                    publish_h265_access_unit(
                                        tx,
                                        camera_id,
                                        data,
                                        h265_au_pts,
                                        h265_au_keyframe,
                                        &h265_vps,
                                        &h265_sps,
                                        &h265_pps,
                                    );
                                } else {
                                    h265_au.clear();
                                }

                                h265_au_timestamp = None;

                                h265_au_keyframe = false;

                                h265_au_has_vcl = false;
                            }
                        }

                        _ => {}
                    }
                }

                // =================================================
                // AUDIO RTP
                // =================================================
                2 => {
                    let Some(off) = rtp_payload_offset(&pkt) else {
                        continue;
                    };
                    let timestamp = u32::from_be_bytes([pkt[4], pkt[5], pkt[6], pkt[7]]);
                    let payload = &pkt[off..];
                    if payload.is_empty() {
                        continue;
                    }

                    let audio_clock = sdp_info
                        .audio
                        .as_ref()
                        .map(|audio| audio.clock_rate)
                        .unwrap_or(8000);

                    // Normalize audio PTS also.
                    let base_timestamp = *audio_base_timestamp.get_or_insert(timestamp);

                    let relative_timestamp = timestamp.wrapping_sub(base_timestamp);

                    let pts = relative_timestamp as u64 * 1_000_000 / audio_clock as u64;

                    let audio_codec = sdp_info.audio.as_ref().map(|audio| match &audio.codec {
                        crate::rtsp::sdp::AudioCodec::Pcma => Codec::G711Pcma,

                        crate::rtsp::sdp::AudioCodec::Pcmu => Codec::G711Pcmu,

                        crate::rtsp::sdp::AudioCodec::Aac { config } => Codec::Aac {
                            config: config.clone(),
                        },
                    });

                    if let Some(codec) = audio_codec {
                        let _ = tx.send(MediaFrame {
                            camera_id: camera_id.to_string(),

                            codec,

                            pts,

                            is_keyframe: true,

                            data: Bytes::copy_from_slice(payload),
                        });
                    }
                }

                // RTCP
                1 | 3 => {}

                _ => {}
            }
        }
    }

    // ── Entry point ───────────────────────────────────────────────────────────

    pub async fn run(mut self) -> Result<(), BoxError> {
        let mut attempt = 0u32;
        loop {
            match self.connect_and_stream().await {
                Ok(()) => break,
                Err(e) => {
                    eprintln!("[RTSP {}] stream failed: {}", self.config.camera_id, e);

                    let secs = backoff(attempt);
                    eprintln!("[RTSP {}] reconnecting in {}s", self.config.camera_id, secs);

                    tokio::time::sleep(Duration::from_secs(secs)).await;
                    self.cseq = 1;
                    attempt += 1;
                }
            }
        }
        Ok(())
    }

    async fn connect_and_stream(&mut self) -> Result<(), BoxError> {
        let parsed = Url::parse(&self.config.url)?;
        let host = parsed.host_str().ok_or("missing host")?;
        let port = parsed.port().unwrap_or(554);
        let addr = format!("{}:{}", host, port);
        let username = percent_decode_str(parsed.username())
            .decode_utf8()
            .map(|s| s.into_owned())
            .unwrap_or_default();
        let password = percent_decode_str(parsed.password().unwrap_or(""))
            .decode_utf8()
            .map(|s| s.into_owned())
            .unwrap_or_default();
        let is_playback = self.config.playback_start.is_some();

        // The RTSP request-line URI (and therefore the digest `uri=` value) must
        // NOT contain embedded credentials — only the Authorization header does.
        // self.config.url has them (that's how we pass creds in); strip before
        // using it as a wire-level URI anywhere.
        let mut clean = parsed.clone();
        let _ = clean.set_username("");
        let _ = clean.set_password(None);
        let clean_url = strip_userinfo(&self.config.url);

        let mut stream = Self::connect(&addr).await?;

        Self::do_options(&mut stream, &clean_url, &mut self.cseq).await?;

        let (auth, sdp, base_url) = Self::do_describe(
            &mut stream,
            &clean_url,
            &mut self.cseq,
            &username,
            &password,
        )
        .await?;

        let sdp_info = parse_sdp(&sdp)?;
        let setup_url = resolve_control_url(&base_url, &sdp_info.control_url);

        let session = Self::do_setup(
            &mut stream,
            &setup_url,
            &mut self.cseq,
            &username,
            &password,
            &auth,
            is_playback,
        )
        .await?;

        if let Some(ref audio) = sdp_info.audio {
            let audio_setup_url = resolve_control_url(&base_url, &audio.control_url);
            match Self::do_setup_audio(
                &mut stream,
                &audio_setup_url,
                &mut self.cseq,
                &username,
                &password,
                &auth,
                &session,
                is_playback,
            )
            .await
            {
                Ok(()) => {
                    eprintln!("DEBUG audio SETUP successful");
                }

                Err(e) => {
                    eprintln!("DEBUG audio SETUP failed: {} — continuing video-only", e);
                }
            };
        }

        eprintln!("DEBUG VIDEO SETUP OK session={}", session);

        if sdp_info.audio.is_some() {
            eprintln!("DEBUG AUDIO TRACK PRESENT");
        }

        let result = self
            .play_and_stream(
                &mut stream,
                &clean_url,
                &sdp_info,
                &session,
                &auth,
                &username,
                &password,
            )
            .await;

        Self::do_teardown(
            &mut stream,
            &clean_url,
            &mut self.cseq,
            &session,
            &username,
            &password,
            &auth,
        )
        .await
        .ok();

        result
    }

    async fn play_and_stream(
        &mut self,
        stream: &mut TcpStream,
        url: &str,
        sdp_info: &SdpInfo,
        session: &str,
        auth: &Auth,
        username: &str,
        password: &str,
    ) -> Result<(), BoxError> {
        Self::do_play(
            stream,
            url,
            &mut self.cseq,
            session,
            username,
            password,
            auth,
            self.config.playback_start.as_deref(),
            self.config.playback_end.as_deref(),
            self.config.playback_scale,
        )
        .await?;
        eprintln!("DEBUG streaming camera_id={}", self.config.camera_id);
        Self::rtp_loop(stream, &self.config.camera_id, sdp_info, &self.tx).await
    }

    pub async fn probe(&self) -> Result<StreamInfo, BoxError> {
        let parsed = Url::parse(&self.config.url)?;
        let host = parsed.host_str().ok_or("missing host")?;
        let port = parsed.port().unwrap_or(554);
        let addr = format!("{}:{}", host, port);
        let username = percent_decode_str(parsed.username())
            .decode_utf8()
            .map(|s| s.into_owned())
            .unwrap_or_default();
        let password = percent_decode_str(parsed.password().unwrap_or(""))
            .decode_utf8()
            .map(|s| s.into_owned())
            .unwrap_or_default();
        let camera_ip = host.to_string();

        let mut clean = parsed.clone();
        let _ = clean.set_username("");
        let _ = clean.set_password(None);

        let clean_url = strip_userinfo(&self.config.url);

        let mut stream = Self::connect(&addr).await?;
        let mut cseq = 1u32;

        Self::do_options(&mut stream, &clean_url, &mut cseq).await?;
        let (_, sdp, _base_url) =
            Self::do_describe(&mut stream, &clean_url, &mut cseq, &username, &password).await?;
        drop(stream);

        let sdp_info = parse_sdp(&sdp)?;
        Ok(sdp_info.to_stream_info(&self.config.camera_id, &camera_ip, &sdp_info.session_name))
    }
}

fn strip_userinfo(url: &str) -> String {
    let Some(scheme_end) = url.find("://") else {
        return url.to_string();
    };
    let after_scheme = scheme_end + 3;
    let Some(at_pos) = url[after_scheme..].find('@') else {
        return url.to_string();
    };
    // Only treat this '@' as userinfo if it comes before the first '/' —
    // otherwise it's inside the path/query, not part of the authority.
    let slash_pos = url[after_scheme..].find('/').unwrap_or(usize::MAX);
    if at_pos >= slash_pos {
        return url.to_string();
    }
    let mut out = String::with_capacity(url.len());
    out.push_str(&url[..after_scheme]);
    out.push_str(&url[after_scheme + at_pos + 1..]);
    out
}

fn rtsp_clock(value: &str) -> String {
    value.trim().replace('-', "").replace(':', "")
}

fn rtp_payload_offset(pkt: &[u8]) -> Option<usize> {
    if pkt.len() < 12 {
        return None;
    }
    let cc = (pkt[0] & 0x0F) as usize; // CSRC count
    let has_ext = (pkt[0] & 0x10) != 0; // X bit
    let mut offset = 12 + cc * 4;
    if has_ext {
        // 2 bytes profile id, 2 bytes length (in 32-bit words), then the words.
        if pkt.len() < offset + 4 {
            return None;
        }
        let ext_words = u16::from_be_bytes([pkt[offset + 2], pkt[offset + 3]]) as usize;
        offset += 4 + ext_words * 4;
    }
    (pkt.len() >= offset).then_some(offset)
}
