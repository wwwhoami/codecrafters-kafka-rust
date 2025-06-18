use codecrafters_kafka::Server;

fn main() {
    let server = Server::new("127.0.0.1:9092").unwrap_or_else(|e| {
        eprintln!("failed to create server: {}", e);
        std::process::exit(1);
    });

    server.run();
}
