use base64::{engine::general_purpose::STANDARD as B64, Engine};
use bytes::Bytes;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub struct SdpInfo {
    pub codec: CodecParams,
    pub control_url: String,
    pub clock_rate: u32,
    pub width: u32,
    pub height: u32,
}

pub enum CodecParams {
    H265 { vps: Bytes, sps: Bytes, pps: Bytes },
    H264 { sps: Bytes, pps: Bytes },
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

pub fn parse_sdp(sdp: &str) -> Result<SdpInfo, BoxError> {
    // m=video 0 RTP/AVP 96          → payload type = 96
    // a=rtpmap:96 H265/90000        → codec = H265, clock_rate = 90000
    // a=fmtp:96 sprop-vps=...       → vps, sps, pps (base64 encoded)
    // a=control:rtsp://...trackID=1 → control_url
    // a=x-dimensions:1920,1080      → width=1920, height=1080

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

    let mut parts = encoding.split("/");
    let codec_name = parts.next().unwrap_or("").trim();
    let clock_rate: u32 = parts.next().unwrap_or("90000").parse()?;

    let fmtp_prefix = format!("a=fmtp:{}", payload_type);
    let fmtp_line = sdp
        .lines()
        .find(|l| l.starts_with(&fmtp_prefix))
        .ok_or("no a=fmtp present in sdp")?;

    let codec = match codec_name {
        "H265" => {
            // parse sprop-vps, sprop-sps, sprop-pps
            let vps = parse_fmtp_param(fmtp_line, "sprop-vps")?;
            let sps = parse_fmtp_param(fmtp_line, "sprop-sps")?;
            let pps = parse_fmtp_param(fmtp_line, "sprop-pps")?;
            CodecParams::H265 { vps, sps, pps }
        }
        "H264" => {
            // parse sprop-parameter-sets=<sps_b64>,<pps_b64>
            let params = fmtp_line
                .find("sprop-parameter-sets=")
                .map(|i| &fmtp_line[i + "sprop-parameter-sets=".len()..])
                .ok_or("H264: no sprop-parameter-sets")?;
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
                )?,
            );
            CodecParams::H264 { sps, pps }
        }
        other => return Err(format!("unsupported codec: {}", other).into()),
    };

    let control_url = sdp
        .lines()
        .find(|l| l.starts_with("a=control:"))
        .and_then(|l| l.strip_prefix("a=control:"))
        .ok_or("no a=control in SDP")?
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

    Ok(SdpInfo {
        codec,
        control_url,
        clock_rate,
        width,
        height,
    })
}
