use gossip_glomers_rs::{ClusterState, Message, Node, Server, Timers, IO};
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
    let mut node = Node::<EchoServer, Payload, ()>::init()?;
    node.run()
}

struct EchoServer {}

impl Server<Payload, ()> for EchoServer {
    fn init(_: &ClusterState, _: &mut Timers<Payload, ()>) -> Result<EchoServer> {
        Ok(EchoServer {})
    }

    fn on_message(
        &mut self,
        _: &ClusterState,
        io: &mut IO<Payload>,
        input: Message<Payload>,
    ) -> Result<()> {
        let payload = &input.body.payload;
        match payload {
            Payload::Echo { echo } => {
                let reply = Payload::EchoOk {
                    echo: echo.to_string(),
                };
                io.rpc_reply_to(&input, reply)?;
            }
            Payload::EchoOk { .. } => {}
        };

        Ok(())
    }

    fn on_timer(&mut self, _: &ClusterState, _: &mut IO<Payload>, _: ()) -> Result<()>
    where
        Self: Sized,
    {
        Ok(())
    }
}
