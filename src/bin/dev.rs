use camproto_ingest::rtsp::{RtspClient, RtspConfig};

#[tokio::main]
async fn main() {
    let config = RtspConfig {
        url:       "rtsp://admin:admin@192.168.1.50:554/stream1".into(),
        camera_id: "cam_001".into(),
    };

    let client = RtspClient::new(config);
    
    if let Err(e) = client.run().await {
        eprintln!("error: {}", e);
    }
}