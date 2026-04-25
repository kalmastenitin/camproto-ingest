use crate::frame::{Codec, MediaFrame};
use crate::rtsp::sdp::{CodecParams, SdpInfo, parse_sdp};

use base64::{engine::general_purpose::STANDARD as B64, Engine};
use bytes::{BufMut, BytesMut};
use md5;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio::time::timeout;
use url::Url;

// Type Aliases
type BoxError = Box<dyn std::error::Error + Send + Sync>;

// ─────────────────────────────────────────────────────────────────────────────i
// Public types
// ─────────────────────────────────────────────────────────────────────────────

pub struct RtspConfig {
    pub url: String,
    pub camera_id: String,
}

pub struct RtspClient {
    config: RtspConfig,
    cseq: u32,
    tx: broadcast::Sender<MediaFrame>,
}

enum Auth {
    None,
    Basic(String), // the full "Basic xxxx" header value
    Digest { realm: String, nonce: String },
}

// ─────────────────────────────────────────────────────────────────────────────
// Auth
// ─────────────────────────────────────────────────────────────────────────────

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

fn digest_auth(
    method: &str,
    uri: &str,
    username: &str,
    password: &str,
    realm: &str,
    nonce: &str,
) -> String {
    let ha1 = md5_hex(&format!("{}:{}:{}", username, realm, password));
    let ha2 = md5_hex(&format!("{}:{}", method, uri));
    let resp = md5_hex(&format!("{}:{}:{}", ha1, nonce, ha2));
    format!(
        "Digest username=\"{}\", realm=\"{}\", nonce=\"{}\", uri=\"{}\", response=\"{}\"",
        username, realm, nonce, uri, resp
    )
}

fn md5_hex(input: &str) -> String {
    format!("{:x}", md5::compute(input.as_bytes()))
}

// ─────────────────────────────────────────────────────────────────────────────
// Response helpers
// ─────────────────────────────────────────────────────────────────────────────

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

fn backoff(attempt: u32) -> u64 {
    let secs = 1u64 << attempt.min(5);
    secs.min(60)
}

// ─────────────────────────────────────────────────────────────────────────────
// RtspClient
// ─────────────────────────────────────────────────────────────────────────────

impl RtspClient {
    pub fn new(config: RtspConfig) -> (Self, broadcast::Receiver<MediaFrame>) {
        let (tx, rx) = broadcast::channel(128);

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
            Some(200) => Ok(()),
            Some(s) => Err(format!("OPTIONS failed: {}", s).into()),
            None => Err("OPTIONS: could not parse status".into()),
        }
    }

    // Returns (Auth, sdp_body)
    async fn do_describe(
        stream: &mut TcpStream,
        url: &str,
        cseq: &mut u32,
        username: &str,
        password: &str,
    ) -> Result<(Auth, String), BoxError> {
        // First attempt — no auth
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
                return Ok((Auth::None, sdp));
            }
            Some(401) => {} // fall through to auth
            Some(s) => return Err(format!("DESCRIBE failed: {}", s).into()),
            None => return Err("DESCRIBE: could not parse status".into()),
        }

        // Extract realm + nonce from 401
        let auth_line = parse_header(&resp, "www-authenticate")
            .ok_or("DESCRIBE 401 missing WWW-Authenticate")?;
        let auth = if auth_line.to_lowercase().starts_with("basic") {
            Auth::Basic(B64.encode(format!("{}:{}", username, password)))
        } else {
            // Retry with Digest
            let realm = digest_param(auth_line, "realm").unwrap_or_default();
            let nonce = digest_param(auth_line, "nonce").unwrap_or_default();

            Auth::Digest { realm, nonce }
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
        Ok((auth, sdp))
    }

    // Returns session ID
    async fn do_setup(
        stream: &mut TcpStream,
        setup_url: &str,
        cseq: &mut u32,
        username: &str,
        password: &str,
        auth_method: &Auth,
    ) -> Result<String, BoxError> {
        let mut req = format!("SETUP {} RTSP/1.0\r\nCSeq: {}\r\n", setup_url, cseq);

        if let Some(auth) = auth_method.header("SETUP", setup_url, username, password) {
            req.push_str(&format!("Authorization: {}\r\n", auth));
        };

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

    async fn do_play(
        stream: &mut TcpStream,
        url: &str,
        cseq: &mut u32,
        session: &str,
        username: &str,
        password: &str,
        auth_method: &Auth,
    ) -> Result<(), BoxError> {
        let mut req = format!(
            "PLAY {} RTSP/1.0\r\nCSeq: {}\r\nSession: {}\r\n",
            url, cseq, session
        );
        if let Some(auth) = auth_method.header("PLAY", url, username, password) {
            req.push_str(&format!("Authorization: {}\r\n", auth));
        }
        req.push_str("Range: npt=0.000-\r\n\r\n");

        *cseq += 1;
        stream.write_all(req.as_bytes()).await?;
        let resp = read_response(stream).await?;
        match parse_status(&resp) {
            Some(200) => Ok(()),
            Some(s) => Err(format!("PLAY failed: {}", s).into()),
            None => Err("PLAY: could not parse status".into()),
        }
    }

    async fn rtp_loop(
        stream: &mut TcpStream,
        camera_id: &str,
        sdp_info: &SdpInfo,
        tx: &broadcast::Sender<MediaFrame>,
    ) -> Result<(), BoxError> {
        // Pre-allocated reassembly buffer — split().freeze() = zero copy at frame boundary
        let mut fu_buf = BytesMut::with_capacity(256 * 1024);
        let frame_codec = match &sdp_info.codec {
            CodecParams::H265 { vps,sps, pps } => Codec::H265 {
                vps: vps.clone(),
                pps: pps.clone(),
                sps: sps.clone(),
            },
            CodecParams::H264 { sps, pps } => Codec::H264 {
                sps: sps.clone(),
                pps: pps.clone(),
            },
        };
        
        loop {
            // Interleaved frame: $ channel(1B) length(2B BE) data(N bytes)
            let mut hdr = [0u8; 4];
            stream.read_exact(&mut hdr).await?;

            if hdr[0] != b'$' {
                continue;
            } // re-sync

            let channel = hdr[1];
            let length = u16::from_be_bytes([hdr[2], hdr[3]]) as usize;

            

            let mut pkt = vec![0u8; length];
            stream.read_exact(&mut pkt).await?;

            if channel % 2 != 0 {
                continue;
            } // odd = RTCP, skip
            if pkt.len() < 12 {
                continue;
            } // too short for RTP header

            let marker = (pkt[1] & 0x80) != 0;
            let timestamp = u32::from_be_bytes([pkt[4], pkt[5], pkt[6], pkt[7]]);
            let payload = &pkt[12..];

            if payload.is_empty() {
                continue;
            }

            // H.265 NAL type = bits 1-6 of byte 0
            let nal_type = (payload[0] >> 1) & 0x3F;

            // Type 49 = FU (Fragmentation Unit) — large NAL split across packets
            if nal_type == 49 && payload.len() >= 3 {
                let fu_hdr = payload[2];
                let start = (fu_hdr & 0x80) != 0;
                let end = (fu_hdr & 0x40) != 0;
                let fu_type = fu_hdr & 0x3F;

                if start {
                    fu_buf.clear();
                    // Reconstruct original 2-byte NAL header:
                    // keep forbidden+layer bits from PayloadHdr, replace nal_unit_type
                    fu_buf.put_u8((payload[0] & 0x81) | (fu_type << 1));
                    fu_buf.put_u8(payload[1]);
                    fu_buf.put_slice(&payload[3..]);
                } else {
                    fu_buf.put_slice(&payload[3..]);
                }

                if end || marker {
                    let data = fu_buf.split().freeze(); // zero copy
                    let pts = (timestamp as u64) * 1_000_000 / sdp_info.clock_rate as u64;
                    let is_keyframe = fu_type == 19 || fu_type == 20;

                    let frame = MediaFrame {
                        camera_id: camera_id.to_string(),
                        codec: frame_codec.clone(),
                        pts,
                        is_keyframe,
                        data,
                    };

                    let _ = tx.send(frame);
                }
            }
            // TODO: single NAL (type 1-47), AP (type 48)
        }
    }

    // ── Entry point ───────────────────────────────────────────────────────────

    pub async fn run(mut self) -> Result<(), BoxError> {
        let mut attempt = 0u32;
        loop {
            match self.connect_and_stream().await {
                Ok(()) => break,
                Err(e) => {
                    let secs = backoff(attempt);
                    println!("disconnected: {} - retrying in {}s", e, secs);
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
        let username = parsed.username().to_string();
        let password = parsed.password().unwrap_or("").to_string();

        let mut stream = Self::connect(&addr).await?;
        println!("connected to {}", addr);

        Self::do_options(&mut stream, &self.config.url, &mut self.cseq).await?;

        let (auth, sdp) = Self::do_describe(
            &mut stream,
            &self.config.url,
            &mut self.cseq,
            &username,
            &password,
        )
        .await?;
        
        let sdp_info = parse_sdp(&sdp)?;

        let setup_url = sdp_info.control_url.clone();
        
        let session = Self::do_setup(
            &mut stream,
            &setup_url,
            &mut self.cseq,
            &username,
            &password,
            &auth,
        )
        .await?;

        Self::do_play(
            &mut stream,
            &self.config.url,
            &mut self.cseq,
            &session,
            &username,
            &password,
            &auth,
        )
        .await?;

        println!("streaming camera_id={}", self.config.camera_id);

        Self::rtp_loop(&mut stream, &self.config.camera_id, &sdp_info, &self.tx).await
    }
}
