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

impl RtspClient {
    pub fn new(config: RtspConfig) -> Self {
        RtspClient { config, cseq: 1 }
    }

    pub async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        let parsed = Url::parse(&self.config.url)?;
        let host = parsed.host_str().ok_or("missing host")?;
        let port = parsed.port().unwrap_or(554);
        let addr = format!("{}:{}", host, port);

        let mut stream = TcpStream::connect(&addr).await?;

        println!("connected to {}",addr);

        
        Ok(())
    }
}
