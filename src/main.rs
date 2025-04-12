use std::net::TcpListener;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:9092").expect("unable to bind to port");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!(
                    "accepted new connection from {}",
                    stream.peer_addr().unwrap()
                );
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
