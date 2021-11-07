extern crate differential_dataflow;
extern crate timely;

use std::{thread::spawn, time::Duration};

use tungstenite::{connect, Message};

use differential_dataflow::operators::{iterate::SemigroupVariable, Consolidate, Count};

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
            params: format!("XT.{}", std::env::args().nth(1).unwrap()),
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

    timely::execute_from_args(std::env::args().skip(2), move |worker| {
        let mut input = differential_dataflow::input::InputSession::new();
        let mut probe = timely::dataflow::ProbeHandle::new();

        worker.dataflow(|scope| {
            const RETENTION: Duration = Duration::from_secs(15);
            const BAR_LENGTH: Duration = Duration::from_secs(1);

            let trades = input.to_collection(scope);
            // .probe_with(&mut probe)
            // .inspect(|x| println!("{:?}", x));

            let frontier_timestamp =
                probe.with_frontier(|frontier| *frontier.first().unwrap_or(&Duration::default()));

            let trades_by_window = trades
                .map(|trade: MyTrade| {
                    let agg_timestamp =
                        trade.timestamp() - trade.timestamp() % (BAR_LENGTH.as_millis() as i64);
                    (agg_timestamp, trade)
                })
                .filter(move |&(agg_timestamp, _)| {
                    Duration::from_millis(agg_timestamp as u64) + RETENTION > frontier_timestamp
                });

            let _count_per_window_per_ticker = trades_by_window
                .map(|(agg_timestamp, trade)| (agg_timestamp, trade.ticker()))
                .consolidate()
                .count()
                .probe_with(&mut probe)
                .inspect(move |(((agg_timestamp, ticker), count), ts, diff)| {
                    if *diff > 0 && Duration::from_millis(*agg_timestamp as u64) + RETENTION > *ts {
                        println!("{:?}", (((agg_timestamp, ticker), count), ts, diff));
                    }
                });
        });

        loop {
            input.flush();
            worker.step();

            for trade in rx.try_iter() {
                let ts_unix = Duration::from_millis(trade.timestamp() as u64);
                if ts_unix > *input.time() {
                    input.advance_to(ts_unix);
                }

                // println!("{:?}", trade);
                input.insert(trade);
            }
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
