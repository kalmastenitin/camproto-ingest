use camproto_ingest::rtsp::{RtspClient, RtspConfig};

#[tokio::main]
async fn main() {
    let config = RtspConfig {
        url: "rtsp://admin:admin@192.168.1.240:554/rtsp/streaming?channel=5&subtype=01".into(),
        camera_id: "cam_001".into(),
    };

    let client = RtspClient::new(config);

    if let Err(e) = client.run().await {
        eprintln!("error: {}", e);
    }
}
