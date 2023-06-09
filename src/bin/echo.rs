use gossip_glomers_rs::{ClusterState, Event, Server, Node, IO};
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
    let server = EchoServer{};
    let mut node = Node::<(), EchoServer, Payload, ()>::init((), server)?;
    node.run()
}

struct EchoServer {}

impl Server<Payload, ()> for EchoServer {
    fn on_event(
        &mut self,
        _: &ClusterState,
        io: &mut IO<Payload>,
        _: &mut (),
        input: Event<Payload, ()>,
    ) -> Result<()> {
        match input {
            Event::Message(msg) => {
                let payload = &msg.body.payload;
                match payload {
                    Payload::Echo { echo } => {
                        let reply = Payload::EchoOk {
                            echo: echo.to_string(),
                        };
                        io.reply_to(&msg, reply)?;
                    }
                    Payload::EchoOk { .. } => {}
                };
            },
            _ => { },
        }

        Ok(())
    }
}
