use camproto_ingest::rtsp::{RtspClient, RtspConfig};

#[tokio::main]
async fn main() {
    let config = RtspConfig {
        url: "rtsp://admin:admin@192.168.1.240:554/rtsp/streaming?channel=5&subtype=01".into(),
        camera_id: "cam_001".into(),
    };

    let (client, mut rx) = RtspClient::new(config);

    tokio::spawn(async move {
        if let Err(e) = client.run().await {
            eprintln!("ingest error :{}", e);
        }
    });

    loop {
        match rx.recv().await {
            Ok(frame) => {
                println!(
                    "FRAME pts={:.3}s keyframe={} size={}B",
                    frame.pts as f64 / 1_000_000.0,
                    frame.is_keyframe,
                    frame.data.len(),
                );
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                eprintln!("WARNING: dropped {} frames", n);
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                eprintln!("stream ended")
            }
        }
    }
}
