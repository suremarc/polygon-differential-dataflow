extern crate differential_dataflow;
extern crate timely;

use std::{
    thread::spawn,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crossbeam::channel::Receiver;
use tungstenite::{connect, Message};

use rust_lib_aggs::{
    dataflow,
    ws::{self, WebsocketTrade},
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

const FLUSH_FREQUENCY: Duration = Duration::from_millis(250);

fn main() -> Result<(), MainError> {
    let rx = trades_feed::<ws::CryptoTrade>()?;

    timely::execute_from_args(std::env::args().skip(2), move |worker| {
        let mut input = differential_dataflow::input::InputSession::<_, _, isize>::new();
        let mut probe = timely::dataflow::ProbeHandle::new();

        worker.dataflow(dataflow::aggregate(&mut input, &mut probe, |agg| {
            println!("{}", agg);
        }));

        let mut last_flush = Instant::now();

        for trade in rx.iter() {
            let ts_unix = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time went backwards");
            input.advance_to(ts_unix);
            // println!("{:#?}", input.time());

            input.insert(trade);

            if Instant::now().duration_since(last_flush) > FLUSH_FREQUENCY {
                input.flush();
                last_flush = Instant::now();

                while probe.less_than(input.time()) {
                    worker.step_or_park(None);
                }
            }
        }
    })
    .expect("Computation terminated abnormally");

    Ok(())
}

fn trades_feed<T: 'static + WebsocketTrade + Send>() -> Result<Receiver<T>, MainError> {
    let url = url::Url::parse(format!("wss://socket.polygon.io/{}", T::SOCKET_PATH).as_str())
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
            params: format!("{}.{}", T::FEED_PREFIX, std::env::args().nth(1).unwrap()),
        },
    )?;

    let (tx, rx) = crossbeam::channel::bounded(1_000_000);

    spawn(move || loop {
        if let Message::Text(data) = socket.read_message().unwrap() {
            let messages: Vec<ws::Message<T>> = serde_json::from_str(data.as_str()).expect(&data);
            for message in messages.iter() {
                if let ws::Message::Trade(trade) = message {
                    tx.send(*trade).unwrap();
                } else {
                    println!(
                        "{}",
                        serde_json::to_string(message).expect("failed to serialize Message")
                    );
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
