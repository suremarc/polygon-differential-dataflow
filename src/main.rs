extern crate differential_dataflow;
extern crate timely;

use std::{thread::spawn, time::Duration};

use tungstenite::{connect, Message};

// use differential_dataflow::operators::iterate::Variable;
use differential_dataflow::operators::{iterate::Variable, Count};

use rust_lib_aggs::ws::{self, Trade};

use std::time::{SystemTime, UNIX_EPOCH};

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
            // Prepare some delayed feedback from the output.
            // Explanation of `delay` deferred for the moment.
            const RETENTION: Duration = Duration::from_secs(1);
            const BAR_LENGTH: Duration = Duration::from_secs(30);
            let retractions = Variable::new(scope, RETENTION.as_millis().try_into().unwrap());

            let trades = input.to_collection(scope);
            let trades_recent = trades.filter(|trade: &MyTrade| {
                let since_the_epoch = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Time went backwards");
                Duration::from_millis(trade.timestamp()) + RETENTION + BAR_LENGTH > since_the_epoch
            });

            // trades_recent
            //     .probe_with(&mut probe)
            //     .inspect(|trade| println!("{:?}", trade));

            let trade_buckets = trades.map(|trade: MyTrade| {
                let agg_timestamp =
                    trade.timestamp() as u128 / BAR_LENGTH.as_millis() * BAR_LENGTH.as_millis();
                ((trade.ticker(), agg_timestamp), trade)
            });

            let aggs = trade_buckets.map(|tup| tup.0).count();

            aggs.probe_with(&mut probe).inspect(|x| {
                if x.2 == 1 {
                    println!("{:?}", x)
                }
            });

            retractions.set(&trades.concat(&trades_recent.negate()));
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
