extern crate differential_dataflow;
extern crate timely;

use std::{
    sync::{Arc, Mutex},
    thread::spawn,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crossbeam::channel::Receiver;
use rust_decimal::prelude::ToPrimitive;
use timely::dataflow::Scope;
use tungstenite::{connect, Message};

use differential_dataflow::{difference::DiffPair, input::Input, operators::Reduce};
use differential_dataflow::{
    operators::{iterate::Variable, Consolidate, Join},
    Collection,
};

use rust_lib_aggs::{
    pair::Partition,
    ws::{self, Decimal, Trade},
};

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

const BAR_LENGTH: Duration = Duration::from_secs(1);
const RETENTION: Duration = Duration::from_secs(30);
const GRACE_PERIOD: Duration = Duration::from_millis(25);
const FLUSH_FREQUENCY: Duration = Duration::from_millis(25);

fn truncate(dur: Duration, inc: Duration) -> Duration {
    Duration::from_nanos((dur.as_nanos() / inc.as_nanos() * inc.as_nanos()) as u64)
}

fn main() -> Result<(), MainError> {
    let feed = trades_feed()?;

    let (tx, rx) = std::sync::mpsc::channel::<MyTrade>();
    let tx = Arc::new(Mutex::new(tx));

    timely::execute_from_args(std::env::args().skip(2), move |worker| {
        let mut probe = timely::dataflow::ProbeHandle::<Partition>::new();
        let tx = tx.lock().unwrap().clone();

        worker.dataflow(
            |scope: &mut timely::dataflow::scopes::Child<_, Partition>| {
                let output = scope.scoped::<Duration, _, _>("partition", |scope| {
                    let trades = scope.new_collection::<MyTrade, isize>();
                    let trades_by_window =
                        trades.map(|trade| (truncate(trade.timestamp(), BAR_LENGTH), trade));
                });
            },
        );

        // let mut last_flush = Instant::now();

        // for trade in rx.iter() {
        //     input.advance_to(Partition::new(trade.ticker(), trade.timestamp()));
        //     // println!("{:#?}", input.time());

        //     input.send(trade);

        //     if Instant::now().duration_since(last_flush) > FLUSH_FREQUENCY {
        //         input.flush();
        //         last_flush = Instant::now();

        //         while probe.less_than(input.time()) {
        //             worker.step_or_park(None);
        //         }
        //     }
        // }
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
            let messages: Vec<ws::Message<MyTrade>> =
                serde_json::from_str(data.as_str()).expect(&data);
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
