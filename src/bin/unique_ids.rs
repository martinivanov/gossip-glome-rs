use gossip_glomers_rs::{ClusterState, Event, Handler, Node, IO};
use serde::{Deserialize, Serialize};

use anyhow::{bail, Result};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Generate,
    GenerateOk { id: String },
}

fn main() -> anyhow::Result<()> {
    let handler = UniqueIdHandler {};
    let mut node = Node::<(), UniqueIdHandler, Payload, ()>::init((), handler)?;
    node.run()
}

struct UniqueIdHandler {}

impl Handler<Payload, ()> for UniqueIdHandler {
    fn step(
        &mut self,
        cluster_state: &ClusterState,
        io: &mut IO<Payload>,
        _: &mut (),
        input: Event<Payload, ()>,
    ) -> Result<()> {
        match input {
            Event::Message(msg) => {
                let payload = &msg.body.payload;
                match payload {
                    Payload::Generate => {
                        let id = format!("{}-{}", cluster_state.node_id, io.seq);
                        let reply = Payload::GenerateOk { id };
                        io.reply_to(&msg, reply)?;
                    }
                    Payload::GenerateOk { .. } => {
                        bail!("received unexpected GenerateOk message");
                    }
                };
            },
            _ =>  { },
        }

        Ok(())
    }
}
