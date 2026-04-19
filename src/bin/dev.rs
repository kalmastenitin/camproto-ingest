/// Development runner.
///
/// Connects to a real camera and prints frame stats to stdout.
/// Used for testing the ingest pipeline during development.
///
/// Usage:
///   RUST_LOG=debug cargo run --bin ingest-dev -- rtsp://admin:admin@192.168.1.50:554/stream1 cam_001
///
/// Environment variables:
///   RUST_LOG=debug    — enable debug logging (tracing)
use camproto_ingest::{RtspClient, RtspConfig};
use tracing_subscriber::EnvFilter;
 
#[tokio::main]
async fn main() {
    // ── Logging ───────────────────────────────────────────────────────────────
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(true)
        .with_thread_ids(false)
        .compact()
        .init();
 
    // ── Args ──────────────────────────────────────────────────────────────────
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: ingest-dev <rtsp_url> <camera_id>");
        eprintln!("Example: ingest-dev rtsp://admin:admin@192.168.1.50:554/stream1 cam_001");
        std::process::exit(1);
    }
 
    let url       = args[1].clone();
    let camera_id = args[2].clone();
 
    println!("CamProto ingest dev runner");
    println!("  URL:       {}", url);
    println!("  Camera ID: {}", camera_id);
    println!("  Press Ctrl+C to stop\n");
 
    // ── Client ────────────────────────────────────────────────────────────────
    let config = RtspConfig {
        url: url.clone(),
        camera_id: camera_id.clone(),
        ..Default::default()
    };
 
    let (client, mut rx) = RtspClient::new(config);
 
    // Spawn the ingest loop
    let ingest_handle = tokio::spawn(async move {
        if let Err(e) = client.run().await {
            eprintln!("Ingest error: {}", e);
        }
    });
 
    // ── Stats ─────────────────────────────────────────────────────────────────
    let mut frame_count:    u64 = 0;
    let mut keyframe_count: u64 = 0;
    let mut bytes_total:    u64 = 0;
    let start = std::time::Instant::now();
 
    loop {
        tokio::select! {
            result = rx.recv() => {
                match result {
                    Ok(frame) => {
                        frame_count    += 1;
                        bytes_total    += frame.size() as u64;
                        if frame.is_keyframe {
                            keyframe_count += 1;
                        }
 
                        // Print a line every keyframe
                        if frame.is_keyframe {
                            let elapsed = start.elapsed().as_secs_f64();
                            let fps     = frame_count as f64 / elapsed.max(0.001);
                            let kbps    = (bytes_total * 8) as f64 / elapsed.max(0.001) / 1000.0;
                            println!(
                                "[{:.1}s] codec={} pts={:.3}s frames={} keyframes={} fps={:.1} bitrate={:.0}kbps size={}B",
                                elapsed,
                                frame.codec.name(),
                                frame.pts as f64 / 1_000_000.0,
                                frame_count,
                                keyframe_count,
                                fps,
                                kbps,
                                frame.size(),
                            );
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        eprintln!("WARNING: dropped {} frames (consumer too slow)", n);
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        println!("Stream ended.");
                        break;
                    }
                }
            }
 
            _ = tokio::signal::ctrl_c() => {
                println!("\nStopping...");
                break;
            }
        }
    }
 
    // ── Summary ───────────────────────────────────────────────────────────────
    let elapsed = start.elapsed().as_secs_f64();
    println!("\n── Summary ──────────────────────────────────");
    println!("  Duration:   {:.1}s", elapsed);
    println!("  Frames:     {}", frame_count);
    println!("  Keyframes:  {}", keyframe_count);
    println!("  Total data: {:.2} MB", bytes_total as f64 / 1_000_000.0);
    println!("  Avg fps:    {:.1}", frame_count as f64 / elapsed.max(0.001));
    println!("  Avg kbps:   {:.0}", (bytes_total * 8) as f64 / elapsed.max(0.001) / 1000.0);
 
    ingest_handle.abort();
}