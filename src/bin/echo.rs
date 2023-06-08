use gossip_glomers_rs::{ClusterState, Handler, Message, Node, IO};
use serde::{Deserialize, Serialize};

use anyhow::Result;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Echo { echo: String },
    EchoOk { echo: String },
}

fn main() -> anyhow::Result<()> {
    let handler = EchoHandler {};
    let mut node = Node::<(), EchoHandler, Payload>::init((), handler)?;
    node.run()
}

struct EchoHandler {}

impl Handler<Payload, ()> for EchoHandler {
    fn step(
        &mut self,
        _: &ClusterState,
        io: &mut IO<Payload>,
        _: &mut (),
        input: Message<Payload>,
    ) -> Result<()> {
        let payload = &input.body.payload;
        match payload {
            Payload::Echo { echo } => {
                let reply = Payload::EchoOk {
                    echo: echo.to_string(),
                };
                io.reply_to(&input, reply)?;
            }
            Payload::EchoOk { .. } => {}
        };

        Ok(())
    }
}
