use camproto_ingest::rtsp::{RtspClient, RtspConfig};

#[tokio::main]
async fn main() {
    let config = RtspConfig {
        url: "rtsp://admin:sachin1997@192.168.1.241:554/Streaming/Unicast/channels/101".into(),
        camera_id: "cam_001".into(),
        ..Default::default()
    };

    let (client, mut rx) = RtspClient::new(config);

    match client.probe().await {
        Ok(info) => {
            println!("=== Stream Info ===");
            println!("  camera:    {}", info.camera_id);
            println!("  ip:        {}", info.camera_ip);
            println!("  session:   {}", info.session_name);
            println!(
                "  video:     {} {}x{} @ {}fps",
                info.video.codec, info.video.width, info.video.height, info.video.framerate
            );
            println!("  clock:     {}Hz", info.video.clock_rate);
            if let Some(br) = info.video.bitrate {
                println!("  bitrate:   {}kbps", br);
            }
            println!("  has_sps:   {}", info.video.has_sps);
            println!("  has_vps:   {}", info.video.has_vps);
            if let Some(audio) = &info.audio {
                println!(
                    "  audio:     {} {}Hz {}ch",
                    audio.codec, audio.sample_rate, audio.channels
                );
            }
            println!("==================");
        }
        Err(e) => eprintln!("probe failed: {}", e),
    }
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
