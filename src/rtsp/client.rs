use md5;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use url::Url;

pub struct RtspConfig {
    pub url: String,
    pub camera_id: String,
}

pub struct RtspClient {
    config: RtspConfig,
    cseq: u32,
}

fn parse_digest_param(header: &str, key: &str) -> Option<String> {
    // find key="value" in the header string
    let search = format!("{}=\"", key);
    let start = header.find(&search)? + search.len();
    let end = header[start..].find('"')? + start;
    Some(header[start..end].to_string())
}

fn parse_header<'a>(response: &'a str, key: &str) -> Option<&'a str> {
    for line in response.lines() {
        if line.to_lowercase().starts_with(&key.to_lowercase()) {
            return line.split_once(':').map(|(_, v)| v.trim());
        }
    }
    None
}



async fn read_response(
    stream: &mut TcpStream,
    buf: &mut [u8; 1],
) -> Result<String, Box<dyn std::error::Error>> {
    let mut response = Vec::new();
    loop {
        stream.read_exact(buf).await?;
        response.push(buf[0]);
        if response.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    Ok(String::from_utf8_lossy(&response).to_string())
}

impl RtspClient {
    pub fn new(config: RtspConfig) -> Self {
        RtspClient { config, cseq: 1 }
    }

    pub async fn run(mut self) -> Result<(), Box<dyn std::error::Error>> {
        let parsed = Url::parse(&self.config.url)?;
        let host = parsed.host_str().ok_or("missing host")?;
        let port = parsed.port().unwrap_or(554);
        let addr = format!("{}:{}", host, port);

        let mut stream = TcpStream::connect(&addr).await?;

        println!("connected to {}", addr);

        let request = format!(
            "OPTIONS {} RTSP/1.0\r\nCSeq: {}\r\nRequire: implicit-play\r\nProxy-Require: gzipped-messages\r\n\r\n",
            self.config.url, self.cseq
        );
        self.cseq += 1;

        stream.write_all(request.as_bytes()).await?;

        let mut buf = [0u8; 1];

        let mut response_str = read_response(&mut stream, &mut buf).await?;

        println!("options response: {}", response_str);

        let describe_req = format!(
            "DESCRIBE {} RTSP/1.0\r\nCSeq: {}\r\nAccept: application/sdp, application/rtsl, application/mheg\r\n\r\n",
            self.config.url, self.cseq
        );
        self.cseq += 1;
        stream.write_all(describe_req.as_bytes()).await?;

        response_str = read_response(&mut stream, &mut buf).await?;

        let mut body_len = 0usize;

        let mut realm = String::new();
        let mut nonce = String::new();

        body_len = parse_header(&response_str, "content-length")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);

        if let Some(auth_line) = parse_header(&response_str, "www-authenticate") {
            realm = parse_digest_param(auth_line, "realm").unwrap_or_default();
            nonce = parse_digest_param(auth_line, "nonce").unwrap_or_default();
        }

        let username = parsed.username().to_string();
        let password = parsed.password().unwrap_or("").to_string();

        let ha1 = format!(
            "{:x}",
            md5::compute(format!("{}:{}:{}", username, realm, password))
        );
        let ha2 = format!(
            "{:x}",
            md5::compute(format!("DESCRIBE:{}", self.config.url))
        );
        let resp_hash = format!("{:x}", md5::compute(format!("{}:{}:{}", ha1, nonce, ha2)));

        let auth_header = format!(
            "Digest username=\"{}\", realm=\"{}\", nonce=\"{}\", uri=\"{}\", response=\"{}\"",
            username, realm, nonce, self.config.url, resp_hash
        );

        let describe_req2 = format!(
            "DESCRIBE {} RTSP/1.0\r\nCSeq: {}\r\nAuthorization: {}\r\nAccept: application/sdp, application/rtsl, application/mheg\r\n\r\n",
            self.config.url, self.cseq, auth_header
        );
        self.cseq += 1;
        stream.write_all(describe_req2.as_bytes()).await?;

        response_str = read_response(&mut stream, &mut buf).await?;

        body_len = parse_header(&response_str, "content-length")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);

        if let Some(auth_line) = parse_header(&response_str, "www-authenticate") {
            realm = parse_digest_param(auth_line, "realm").unwrap_or_default();
            nonce = parse_digest_param(auth_line, "nonce").unwrap_or_default();
        }

        println!("describe response: {}", response_str);

        let mut body = vec![0u8; body_len];
        stream.read_exact(&mut body).await?;
        let sdp = String::from_utf8_lossy(&body).to_string();
        println!("sdp response: {}", sdp);

        Ok(())
    }
}
