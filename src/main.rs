extern crate differential_dataflow;
extern crate timely;

use std::{
    sync::{Arc, Mutex},
    thread::spawn,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tungstenite::{connect, Message};

use differential_dataflow::operators::{iterate::SemigroupVariable, Consolidate, Reduce};

use rust_lib_aggs::ws::{self, Decimal, Trade};

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

    const BAR_LENGTH: Duration = Duration::from_secs(60);

    let (aggs_tx, aggs_rx) = std::sync::mpsc::channel();
    let mux = Arc::new(Mutex::new(aggs_tx));

    let mut aggs = std::collections::BTreeMap::new();

    spawn(move || loop {
        aggs.extend(aggs_rx.try_iter());

        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");

        aggs.retain(|(agg_timestamp, ticker), stats| {
            let expired =
                Duration::from_millis(*agg_timestamp as u64) + BAR_LENGTH < since_the_epoch;
            if expired {
                println!("{:?}", (ticker, agg_timestamp, stats));
            }

            !expired
        });
    });

    timely::execute_from_args(std::env::args().skip(2), move |worker| {
        let mut input = differential_dataflow::input::InputSession::new();
        let mut probe = timely::dataflow::ProbeHandle::new();

        let tx;
        {
            let guard = mux.lock();
            tx = guard.unwrap().clone();
        }

        worker.dataflow(|scope| {
            const RETENTION: Duration = Duration::from_secs(15 * 60);
            let retractions = SemigroupVariable::new(scope, RETENTION);

            let trades = input
                .to_collection(scope)
                .concat(&retractions)
                .consolidate();

            let trades_by_window = trades.map(|trade: MyTrade| {
                let agg_timestamp =
                    trade.timestamp() - trade.timestamp() % (BAR_LENGTH.as_millis() as i64);
                (agg_timestamp, trade)
            });

            let trades_recent = trades_by_window.map(|(_, trade)| trade);
            retractions.set(&trades_recent.negate());

            let trades_by_window_by_ticker = trades_by_window
                .map(|(agg_timestamp, trade)| ((agg_timestamp, trade.ticker()), trade));

            trades_by_window_by_ticker
                .reduce(|_key, input, output: &mut Vec<((Decimal, Decimal), i32)>| {
                    let value_total: Decimal = input
                        .iter()
                        .map(|&(trade, _)| trade.price() * trade.volume())
                        .sum();
                    let volume_total: Decimal =
                        input.iter().map(|&(trade, _)| trade.volume()).sum();

                    output.push(((value_total / volume_total, volume_total), 1))
                })
                .probe_with(&mut probe)
                .inspect(move |(((agg_timestamp, ticker), data), ts, diff)| {
                    if *diff > 0 && Duration::from_millis(*agg_timestamp as u64) + RETENTION > *ts {
                        tx.send(((*agg_timestamp, ticker.clone()), *data))
                            .expect("couldn't send");
                    }
                });
        });

        loop {
            for trade in rx.try_iter() {
                let ts_unix = Duration::from_millis(trade.timestamp() as u64);
                if ts_unix > *input.time() {
                    input.advance_to(ts_unix);
                }

                // println!("{:?}", trade);
                input.insert(trade);
            }

            input.flush();
            while probe.less_than(input.time()) {
                worker.step();
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
