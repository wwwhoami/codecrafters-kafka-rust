use codecrafters_kafka::ServerAsync;

#[tokio::main]
async fn main() {
    let server = ServerAsync::new("127.0.0.1:9092");

    server.run().await.unwrap_or_else(|e| {
        eprintln!("server failed: {}", e);
        std::process::exit(1);
    });
}
