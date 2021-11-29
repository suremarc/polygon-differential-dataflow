extern crate differential_dataflow;
extern crate timely;

use std::{
    sync::{Arc, Mutex},
    thread::spawn,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crossbeam::channel::Receiver;
use rust_decimal::prelude::ToPrimitive;
use tungstenite::{connect, Message};

use differential_dataflow::operators::{iterate::SemigroupVariable, Consolidate, Count, Join};
use differential_dataflow::{difference::DiffPair, operators::Reduce};

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

type MyTrade = ws::StockTrade;

type AggKey = (String, i64);
type Stats = (
    Decimal,
    Decimal,
    Decimal,
    Decimal,
    Decimal,
    Decimal,
    isize,
    Duration,
);

const BAR_LENGTH: Duration = Duration::from_secs(30);
const RETENTION: Duration = Duration::from_secs(900);

fn truncate(dur: Duration, inc: Duration) -> Duration {
    Duration::from_nanos((dur.as_nanos() / inc.as_nanos() * inc.as_nanos()) as u64)
}

fn main() -> Result<(), MainError> {
    let rx = trades_feed()?;

    let (aggs_tx, aggs_rx) = std::sync::mpsc::sync_channel(100_000);
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

            let prices_by_timestamp = trades_by_window_by_ticker
                .map(|(key, trade)| (key, (trade.timestamp(), trade.price())));
            let prices = trades_by_window_by_ticker.map(|(key, trade)| (key, trade.price()));

            let open_close = prices_by_timestamp.reduce(|_key, input, output| {
                let values = input.iter().map(|&((_ts, price), _num)| *price);
                output.push((
                    (
                        values.clone().next().unwrap_or_else(|| Decimal::from(0)),
                        values.clone().last().unwrap_or_else(|| Decimal::from(0)),
                    ),
                    1_isize,
                ));
            });

            let low_high = prices.reduce(|_key, input, output| {
                let values = input.iter().map(|&(price, _num)| *price);
                output.push((
                    (
                        values.clone().next().unwrap_or_else(|| Decimal::from(0)),
                        values.clone().last().unwrap_or_else(|| Decimal::from(0)),
                    ),
                    1_isize,
                ));
            });

            let ohlc = open_close
                .join(&low_high)
                .map(|(key, ((open, close), (low, high)))| (key, (open, high, low, close)));

            let value_and_volume = trades_by_window_by_ticker
                .explode(|(key, trade)| {
                    Some((
                        key,
                        DiffPair::new(trade.price() * trade.volume(), trade.volume()),
                    ))
                })
                .consolidate()
                .map(|data| (data, ()));

            let count = trades_by_window_by_ticker.map(|(key, _trade)| key).count();

            let stats = value_and_volume
                .join(&count)
                .map(|(key, ((), count))| (key, count))
                .join(&ohlc);

            stats.probe_with(&mut probe).inspect(
                move |(
                    ((ticker, agg_timestamp), (count, (open, high, low, close))),
                    ts,
                    DiffPair {
                        element1: value,
                        element2: volume,
                    },
                )| {
                    if *value > Decimal::from(0)
                        && *volume > Decimal::from(0)
                        && Duration::from_millis(*agg_timestamp as u64) + RETENTION > *ts
                    {
                        aggs_tx
                            .send((
                                (ticker.clone(), *agg_timestamp),
                                (*open, *high, *low, *close, *value, *volume, *count, *ts),
                            ))
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

            if Instant::now().duration_since(last_flush) > Duration::from_millis(250) {
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

    println!("url: {}", url);

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

    let (tx, rx) = crossbeam::channel::bounded(1_000_000);

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

        aggs.retain(
            |(ticker, agg_timestamp), (open, high, low, close, value, volume, count, _ts)| {
                let expired =
                    Duration::from_millis(*agg_timestamp as u64) + BAR_LENGTH < since_the_epoch;
                if expired {
                    println!(
                        "{} - {}: open: {:.2}, high: {:.2}, low: {:.2}, close: {:.2}, vwap: {:.2}, vol: {:.3}, trades: {}",
                        agg_timestamp,
                        ticker,
                        open.0.to_f64().unwrap(),
                        high.0.to_f64().unwrap(),
                        low.0.to_f64().unwrap(),
                        close.0.to_f64().unwrap(),
                        (*value / *volume).0.to_f64().unwrap(),
                        volume.0.to_f64().unwrap(),
                        *count
                    );
                }

                !expired
            },
        );
    }
}
