use gossip_glomers_rs::{ClusterState, Handler, Message, Node, IO};
use serde::{Deserialize, Serialize};
use std::io::StdoutLock;

use anyhow::{Result, bail};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Generate,
    GenerateOk { id: String },
}

fn main() -> anyhow::Result<()> {
    let handler = UniqueIdHandler {};
    let mut node = Node::<(), UniqueIdHandler, Payload>::init((), handler)?;
    node.run()
}

struct UniqueIdHandler {}

impl Handler<Payload, ()> for UniqueIdHandler {
    fn step(
        &mut self,
        cluster_state: &ClusterState,
        io: &mut IO<Payload>,
        _: &mut (),
        input: Message<Payload>,
        output: &mut StdoutLock,
    ) -> Result<()> {
        let payload = &input.body.payload;
        match payload {
            Payload::Generate => {
                let id = format!("{}-{}", cluster_state.node_id, io.seq);
                let reply = Payload::GenerateOk { id: id };
                io.reply_to(&input, reply, output)?;
            },
            Payload::GenerateOk { .. } => { bail!("received unexpected GenerateOk message"); }
        };

        Ok(())
    }
}
