use core::f32;

use base64::{engine::general_purpose::STANDARD as B64, Engine};
use bytes::Bytes;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub struct SdpInfo {
    pub codec: CodecParams,
    pub control_url: String,
    pub clock_rate: u32,
    pub width: u32,
    pub height: u32,
    pub framerate: f32,
    pub bitrate: Option<u32>,
    pub audio: Option<AudioTrack>,
    pub session_name: String,
}

pub struct AudioTrack {
    pub codec: AudioCodec,
    pub sample_rate: u32,
    pub channels: u32,
    pub clock_rate: u32,
    pub control_url: String,
}

pub enum AudioCodec {
    Pcma,                  // G.711 A-law
    Pcmu,                  // G.711 μ-law
    Aac { config: Bytes }, // AAC with audio specific config
}

pub enum CodecParams {
    H265 { vps: Bytes, sps: Bytes, pps: Bytes },
    H264 { sps: Bytes, pps: Bytes },
}

pub struct StreamInfo {
    pub camera_id: String,
    pub camera_ip: String,
    pub session_name: String,
    pub video: VideoInfo,
    pub audio: Option<AudioInfo>,
}

pub struct VideoInfo {
    pub codec: String,
    pub width: u32,
    pub height: u32,
    pub framerate: f32,
    pub bitrate: Option<u32>,
    pub clock_rate: u32,
    pub has_vps: bool,
    pub has_sps: bool,
    pub has_pps: bool,
}

pub struct AudioInfo {
    pub codec: String,
    pub sample_rate: u32,
    pub channels: u32,
}

fn parse_fmtp_param(fmtp_line: &str, key: &str) -> Result<Bytes, BoxError> {
    let search = format!("{}=", key);
    let start = fmtp_line
        .find(&search)
        .ok_or_else(|| format!("fmtp: missing {}", key))?
        + search.len();
    let value = fmtp_line[start..].split(';').next().unwrap_or("").trim();
    Ok(Bytes::from(B64.decode(value)?))
}

fn parse_audio_track(sdp: &str) -> Option<AudioTrack> {
    // find m=audio line
    let audio_line = sdp.lines().find(|l| l.starts_with("m=audio"))?;

    // extract payload type — same as video
    let pt = audio_line.split_whitespace().nth(3)?;

    // find a=rtpmap for this payload type
    let rtpmap_prefix = format!("a=rtpmap:{}", pt);
    let rtpmap_line = sdp.lines().find(|l| l.starts_with(&rtpmap_prefix))?;
    let encoding = rtpmap_line.strip_prefix(&rtpmap_prefix)?;

    // "PCMA/8000/1" or "PCMU/8000" or "mpeg4-generic/48000/2"
    let mut parts = encoding.trim().split('/');
    let codec_str = parts.next()?.trim().to_uppercase();
    let sample_rate: u32 = parts.next()?.trim().parse().ok()?;
    let channels: u32 = parts
        .next()
        .and_then(|c| c.trim().parse().ok())
        .unwrap_or(1);
    let clock_rate = sample_rate; // audio clock_rate == sample_rate

    let codec = match codec_str.as_str() {
        "PCMA" => AudioCodec::Pcma,
        "PCMU" => AudioCodec::Pcmu,
        "MPEG4-GENERIC" | "AAC" => {
            // AAC config from fmtp config= parameter
            let fmtp_prefix = format!("a=fmtp:{}", pt);
            let config = sdp
                .lines()
                .find(|l| l.starts_with(&fmtp_prefix))
                .and_then(|line| {
                    let params = line.strip_prefix(&fmtp_prefix)?;
                    params.split(';').find_map(|p| {
                        let p = p.trim();
                        p.strip_prefix("config=").map(|v| {
                            // hex decode
                            (0..v.len())
                                .step_by(2)
                                .filter_map(|i| u8::from_str_radix(&v[i..i + 2], 16).ok())
                                .collect::<Vec<u8>>()
                        })
                    })
                })
                .map(Bytes::from)
                .unwrap_or_default();
            AudioCodec::Aac { config }
        }
        _ => return None, // unsupported audio codec — skip
    };

    // find a=control for audio track
    // audio control comes after m=audio in the SDP
    // find position of m=audio then look for a=control after it
    let audio_section_start = sdp.find("m=audio")?;
    let audio_section = &sdp[audio_section_start..];
    let control_url = audio_section
        .lines()
        .find(|l| l.starts_with("a=control:"))
        .and_then(|l| l.strip_prefix("a=control:"))
        .unwrap_or("")
        .trim()
        .to_string();

    Some(AudioTrack {
        codec,
        sample_rate,
        channels,
        clock_rate,
        control_url,
    })
}

pub fn parse_sdp(sdp: &str) -> Result<SdpInfo, BoxError> {
    let video_line = sdp
        .lines()
        .find(|l| l.starts_with("m=video"))
        .ok_or("no video m in sdp")?;

    let payload_type = video_line
        .split_whitespace()
        .nth(3)
        .ok_or("m=video missing payload_type")?;

    let rtpmap_prefix = format!("a=rtpmap:{}", payload_type);
    let rtpmap_line = sdp
        .lines()
        .find(|l| l.starts_with(&rtpmap_prefix))
        .ok_or("no a=rtpmap in sdp")?;

    let encoding = rtpmap_line.strip_prefix(&rtpmap_prefix).unwrap();
    let mut parts = encoding.split('/');
    let codec_name = parts.next().unwrap_or("").trim();
    let clock_rate: u32 = parts.next().unwrap_or("90000").parse()?;

    println!("codec_name: {}", codec_name);
    // ── Check codec name FIRST — before requiring fmtp ──────────────────────
    match codec_name {
        "JPEG" | "MJPEG" => return Err("MJPEG not yet supported".into()),
        "H264" | "H264+" | "AVC" | "H265" | "H265+" | "HEVC" => {}
        other => return Err(format!("unsupported codec: {}", other).into()),
    }

    eprintln!("DEBUG full SDP body:\n {sdp}");

    // ── Now safe to require fmtp ─────────────────────────────────────────────
    let fmtp_prefix = format!("a=fmtp:{}", payload_type);
    let fmtp_line = sdp.lines().find(|l| l.starts_with(&fmtp_prefix));

    let codec = match codec_name {
        "H265" | "H265+" | "HEVC" => {
            if let Some(line) = fmtp_line {
                let vps = parse_fmtp_param(line, "sprop-vps").unwrap_or_default();
                let sps = parse_fmtp_param(line, "sprop-sps").unwrap_or_default();
                let pps = parse_fmtp_param(line, "sprop-pps").unwrap_or_default();
                CodecParams::H265 { vps, sps, pps }
            } else {
                CodecParams::H265 {
                    vps: Bytes::new(),
                    sps: Bytes::new(),
                    pps: Bytes::new(),
                }
            }
        }
        "H264" | "H264+" | "AVC" => {
            if let Some(line) = fmtp_line {
                if let Some(i) = line.find("sprop-parameter-sets=") {
                    let params = &line[i + "sprop-parameter-sets=".len()..];
                    let mut it = params.split(',');
                    let sps = Bytes::from(B64.decode(it.next().unwrap_or("").trim())?);
                    let pps = Bytes::from(
                        B64.decode(
                            it.next()
                                .unwrap_or("")
                                .trim()
                                .split(';')
                                .next()
                                .unwrap_or(""),
                        )
                        .unwrap_or_default(),
                    );
                    CodecParams::H264 { sps, pps }
                } else {
                    CodecParams::H264 {
                        sps: Bytes::new(),
                        pps: Bytes::new(),
                    }
                }
            } else {
                CodecParams::H264 {
                    sps: Bytes::new(),
                    pps: Bytes::new(),
                }
            }
        }

        _ => unreachable!(), // already handled above
    };

    let video_section_start = sdp.find("m=video").ok_or("no video m in sdp")?;
    let control_url = sdp[video_section_start..]
        .lines()
        .find(|l| l.starts_with("a=control:"))
        .and_then(|l| l.strip_prefix("a=control:"))
        .ok_or("no a=control in SDP video section")?
        .trim()
        .to_string();

    let (width, height) = sdp
        .lines()
        .find(|l| l.starts_with("a=x-dimensions:"))
        .and_then(|l| l.strip_prefix("a=x-dimensions:"))
        .and_then(|s| {
            let mut p = s.trim().split(',');
            let w = p.next()?.parse().ok()?;
            let h = p.next()?.parse().ok()?;
            Some((w, h))
        })
        .unwrap_or((0, 0));

    let framerate = sdp
        .lines()
        .find(|l| l.starts_with("a=framerate:") || l.starts_with("a=x-framerate:"))
        .and_then(|l| l.split_once(':').map(|(_, v)| v.trim()))
        .and_then(|v| v.parse::<f32>().ok())
        .unwrap_or(0.0);

    let bitrate = sdp
        .lines()
        .find(|l| l.to_lowercase().starts_with("a=bitrate:"))
        .and_then(|l| l.split_once(':').map(|(_, v)| v.trim()))
        .and_then(|v| v.parse::<u32>().ok());

    let audio = parse_audio_track(sdp);

    let session_name = sdp
        .lines()
        .find(|l| l.starts_with("s="))
        .and_then(|l| l.strip_prefix("s="))
        .unwrap_or("RTSP Session")
        .trim()
        .to_string();

    Ok(SdpInfo {
        codec,
        control_url,
        clock_rate,
        width,
        height,
        framerate,
        bitrate,
        audio,
        session_name,
    })
}

impl SdpInfo {
    pub fn to_stream_info(
        &self,
        camera_id: &str,
        camera_ip: &str,
        session_name: &str,
    ) -> StreamInfo {
        let (codec_name, has_vps, has_sps, has_pps) = match &self.codec {
            CodecParams::H265 { vps, sps, pps } => (
                "H265".to_string(),
                !vps.is_empty(),
                !sps.is_empty(),
                !pps.is_empty(),
            ),
            CodecParams::H264 { sps, pps } => {
                ("H264".to_string(), false, !sps.is_empty(), !pps.is_empty())
            }
        };

        let audio = self.audio.as_ref().map(|a| AudioInfo {
            codec: match &a.codec {
                AudioCodec::Pcma => "PCMA".to_string(),
                AudioCodec::Pcmu => "PCMU".to_string(),
                AudioCodec::Aac { .. } => "AAC".to_string(),
            },
            sample_rate: a.sample_rate,
            channels: a.channels,
        });

        StreamInfo {
            camera_id: camera_id.to_string(),
            camera_ip: camera_ip.to_string(),
            session_name: session_name.to_string(),
            video: VideoInfo {
                codec: codec_name,
                width: self.width,
                height: self.height,
                framerate: self.framerate,
                bitrate: self.bitrate,
                clock_rate: self.clock_rate,
                has_vps,
                has_sps,
                has_pps,
            },
            audio,
        }
    }
}
