extern crate differential_dataflow;
extern crate timely;

use std::{
    sync::{Arc, Mutex},
    thread::spawn,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crossbeam::channel::Receiver;
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

type AggKey = (String, i64);
type Stats = (Decimal, Decimal, isize);

const BAR_LENGTH: Duration = Duration::from_secs(30);
const RETENTION: Duration = Duration::from_secs(900);

fn truncate(dur: Duration, inc: Duration) -> Duration {
    Duration::from_nanos((dur.as_nanos() / inc.as_nanos() * inc.as_nanos()) as u64)
}

fn main() -> Result<(), MainError> {
    let rx = trades_feed()?;

    let (aggs_tx, aggs_rx) = std::sync::mpsc::sync_channel(10_000);
    let mux = Arc::new(Mutex::new(aggs_tx));

    spawn(aggs_loop(aggs_rx));

    timely::execute_from_args(std::env::args().skip(2), move |worker| {
        let mut input = differential_dataflow::input::InputSession::<_, _, isize>::new();
        let mut probe = timely::dataflow::ProbeHandle::new();

        let aggs_tx;
        {
            let guard = mux.lock();
            aggs_tx = guard.unwrap().clone();
        }

        worker.dataflow(|scope| {
            let trades_old = SemigroupVariable::new(scope, RETENTION + BAR_LENGTH);

            let trades = input.to_collection(scope);
            let trades_recent = trades.concat(&trades_old.negate()).consolidate();

            // Feed input trades forward so that they get retracted once RETENTION has passed
            trades_old.set(&trades);

            let trades_by_window = trades_recent.map(|trade: MyTrade| {
                let agg_timestamp = truncate(trade.timestamp(), BAR_LENGTH).as_millis() as i64;
                (agg_timestamp, trade)
            });

            let trades_by_window_by_ticker = trades_by_window
                .map(|(agg_timestamp, trade)| ((trade.ticker(), agg_timestamp), trade));

            let stats = trades_by_window_by_ticker.reduce(|_key, input, output| {
                let value = input
                    .iter()
                    .map(|&(trade, num)| trade.volume() * trade.price() * num)
                    .sum();
                let volume = input.iter().map(|&(trade, num)| trade.volume() * num).sum();
                let count = input.len() as isize;

                output.push(((value, volume, count), 1_isize));
            });

            stats.probe_with(&mut probe).inspect(
                move |(((ticker, agg_timestamp), (value, volume, count)), ts, diff)| {
                    if *diff > 0 && Duration::from_millis(*agg_timestamp as u64) + RETENTION > *ts {
                        aggs_tx
                            .send(((ticker.clone(), *agg_timestamp), (*value, *volume, *count)))
                            .expect("couldn't send");
                    }
                },
            );
        });

        let mut last_flush = Instant::now();

        for trade in rx.iter() {
            let ts_unix = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time went backwards");
            input.advance_to(ts_unix);
            // println!("{:#?}", input.time());

            input.insert(trade);

            if Instant::now().duration_since(last_flush) > Duration::from_millis(25) {
                input.flush();
                last_flush = Instant::now();

                while probe.less_than(input.time()) {
                    worker.step();
                }
            }
        }
    })
    .expect("Computation terminated abnormally");

    Ok(())
}

fn trades_feed() -> Result<Receiver<MyTrade>, MainError> {
    let url = url::Url::parse(format!("wss://socket.polygon.io/{}", MyTrade::SOCKET_PATH).as_str())
        .expect("hardcoded url should be valid");

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
            params: format!(
                "{}.{}",
                MyTrade::FEED_PREFIX,
                std::env::args().nth(1).unwrap()
            ),
        },
    )?;

    let (tx, rx) = crossbeam::channel::bounded(10_000);

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

    Ok(rx)
}

fn try_send_payload<T: std::io::Write + std::io::Read>(
    socket: &mut tungstenite::WebSocket<T>,
    payload: &impl serde::Serialize,
) -> Result<(), MainError> {
    let msg = Message::Text(serde_json::to_string(payload)?);
    socket.write_message(msg).map_err(MainError::Write)
}

fn aggs_loop(rx: std::sync::mpsc::Receiver<(AggKey, Stats)>) -> impl FnOnce() {
    let mut aggs = std::collections::BTreeMap::new();

    move || loop {
        aggs.extend(rx.try_iter());

        std::thread::sleep(Duration::from_millis(50));

        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");

        aggs.retain(|(ticker, agg_timestamp), (value, volume, count)| {
            let expired =
                Duration::from_millis(*agg_timestamp as u64) + BAR_LENGTH < since_the_epoch;
            if expired {
                println!(
                    "{} - {}: {}, {}, {}",
                    agg_timestamp,
                    ticker,
                    *value / *volume,
                    *volume,
                    count
                );
            }

            !expired
        });
    }
}
