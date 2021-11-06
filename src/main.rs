extern crate differential_dataflow;
extern crate timely;

use std::thread::spawn;

use tungstenite::{connect, Message};

// use differential_dataflow::operators::iterate::Variable;
use differential_dataflow::operators::Count;

use rust_lib_aggs::ws::{self, Trade};

#[derive(thiserror::Error, Debug)]
pub enum MainError {
    #[error("couldn't parse json")]
    Parse(#[from] serde_json::Error),
    #[error("couldn't send to channel")]
    Write(tungstenite::Error),
    #[error("couldn't read from channel")]
    Read(tungstenite::Error),
}

type MyTrade = ws::CryptoTrade;

fn main() -> Result<(), MainError> {
    let url =
        url::Url::parse("wss://socket.polygon.io/crypto").expect("hardcoded url should be valid");

    let (mut socket, _) = connect(url).expect("Failed to connect");
    println!("WebSocket handshake has been successfully completed");

    try_send_payload(
        &mut socket,
        &ws::Action {
            action: ws::ActionType::Auth,
            params: env!("API_KEY").to_string(),
        },
    )?;
    try_send_payload(
        &mut socket,
        &ws::Action {
            action: ws::ActionType::Subscribe,
            params: "XT.*".to_string(),
        },
    )?;

    // Create an unbounded channel.
    let (tx, rx) = crossbeam::channel::bounded(1000);

    spawn(move || loop {
        if let Message::Text(data) = socket.read_message().unwrap() {
            let messages: Vec<ws::Message<MyTrade>> = serde_json::from_str(data.as_str()).unwrap();
            for message in messages.iter() {
                if let ws::Message::Trade(trade) = message {
                    tx.send(*trade).unwrap();
                } else {
                    println!(
                        "{}",
                        serde_json::to_string(message).expect("failed to serialize Message")
                    )
                }
            }
        }
    });

    timely::execute_from_args(std::env::args(), move |worker| {
        let mut input = differential_dataflow::input::InputSession::new();
        let mut probe = timely::dataflow::ProbeHandle::new();

        // Build a dataflow to present most recent values for keys.
        worker.dataflow(|scope| {
            // Determine the most recent inputs for each key.
            input
                .to_collection(scope)
                .map(|trade: MyTrade| trade.x)
                .count()
                .probe_with(&mut probe)
                .inspect(|x| println!("{:?}", x));
        });

        for trade in rx.iter() {
            if trade.timestamp() > *input.time() {
                input.advance_to(trade.timestamp());
            }
            input.insert(trade);

            input.flush();
            worker.step();
        }
    })
    .expect("Computation terminated abnormally");

    Ok(())
}

fn try_send_payload<T: std::io::Write + std::io::Read>(
    socket: &mut tungstenite::WebSocket<T>,
    payload: &impl serde::Serialize,
) -> Result<(), MainError> {
    let msg = Message::Text(serde_json::to_string(payload)?);
    socket.write_message(msg).map_err(MainError::Write)
}
