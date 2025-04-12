use std::{io::Write, net::TcpListener};

#[derive(Debug)]
struct ResponseHeaderV0 {
    correlation_id: i32,
}

impl ResponseHeaderV0 {
    fn to_bytes(&self) -> Vec<u8> {
        Vec::from(&self.correlation_id.to_be_bytes())
    }
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:9092").expect("unable to bind to port");

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let msg_size: i32 = 0;
                let response_header = ResponseHeaderV0 { correlation_id: 7 };

                stream
                    .write_all(&msg_size.to_be_bytes())
                    .expect("unable to write to stream");

                stream
                    .write_all(&response_header.to_bytes())
                    .expect("unable to write to stream");

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
