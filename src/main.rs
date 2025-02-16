use anyhow::Result;
use chrono::Local;
use fastwebsockets::{FragmentCollector, Frame, OpCode};
use http_body_util::Empty;
use hyper::body::Bytes;
use hyper::Request;
use serde_json::json;
use std::future::Future;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::ClientConfig;
use tokio_rustls::TlsConnector;

const DOMAIN: &str = "broken-ultra-diagram.solana-mainnet.quiknode.pro";
const WS_URI: &str = "/9ca044d1b1177bde02e2ed6b17f08c3d6c9c567b";
const SOLANA_PUMP_FUN_PROGRAM_ID: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";

struct SpawnExecutor;

impl<Fut> hyper::rt::Executor<Fut> for SpawnExecutor
where
    Fut: Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    fn execute(&self, fut: Fut) {
        tokio::spawn(fut);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut addr = String::from(DOMAIN);
    addr.push_str(":443");
    println!("TcpStream::connect({})", &addr);
    let tcp_stream = TcpStream::connect(&addr).await.expect("failed to connect");
    let root_store = tokio_rustls::rustls::RootCertStore::from_iter(
        webpki_roots::TLS_SERVER_ROOTS.iter().cloned(),
    );
    let config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    let tls_stream = TlsConnector::from(Arc::new(config));
    let domain = ServerName::try_from(DOMAIN)?;
    let tls_stream = tls_stream.connect(domain, tcp_stream).await?;

    let subscription_request = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "logsSubscribe",
        "params": [{ "mentions": [SOLANA_PUMP_FUN_PROGRAM_ID] }, { "commitment": "finalized" }]
    });

    let subscription_request = serde_json::to_string(&subscription_request)?;
    let subscription_requestb = subscription_request.as_bytes();

    let req = Request::builder()
        .method("GET")
        .uri(format!("wss://{}{}", &addr, WS_URI))
        .header("Host", &addr)
        .header("Upgrade", "websocket")
        .header("Connection", "Upgrade")
        .header(
            "Sec-WebSocket-Key",
            fastwebsockets::handshake::generate_key(),
        )
        .header("Sec-WebSocket-Version", "13")
        .body(Empty::<Bytes>::new())
        .expect("Failed to build req");

    let (mut ws, _) = fastwebsockets::handshake::client(&SpawnExecutor, req, tls_stream).await?;

    let frame = Frame::text(subscription_requestb.into());

    ws.write_frame(frame).await.expect("Failed to write frame");

    let mut ws = FragmentCollector::new(ws);
    loop {
        let msg = match ws.read_frame().await {
            Ok(msg) => msg,
            Err(e) => {
                eprintln!("Error reading frame: {:?}", e);
                break;
            }
        };

        match msg.opcode {
            OpCode::Text => {
                let text = String::from_utf8_lossy(&msg.payload);
                println!("{}: Text: {}", Local::now().timestamp_millis(), text);
            }
            OpCode::Binary => {
                println!("Binary: {:?}", msg.payload);
            }
            OpCode::Close => {
                println!("Close: {:?}", msg.payload);
                break;
            }
            OpCode::Ping => {
                println!("Ping: {:?}", msg.payload);
            }
            OpCode::Pong => {
                println!("Pong: {:?}", msg.payload);
            }
            OpCode::Continuation => {
                println!("Continuation: {:?}", msg.payload);
            }
        }
    }
    Ok(())
}
