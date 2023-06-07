use gossip_glomers_rs::{ClusterState, Handler, Message, Node, IO};
use serde::{Deserialize, Serialize};
use std::{io::StdoutLock, collections::HashSet};

use anyhow::{Result, bail};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Topology,
    TopologyOk,
    Broadcast { message: usize },
    BroadcastOk,
    Read,
    ReadOk { messages: Vec<usize> },
}

fn main() -> anyhow::Result<()> {
    let handler = BroadcastHandler{};
    let state = BroadcastState {
        messages: &mut HashSet::new(),
    };

    let mut node = Node::<BroadcastState, BroadcastHandler, Payload>::init(state, handler)?;
    node.run()
}

struct BroadcastHandler {}

struct BroadcastState<'a> {
    messages: &'a mut HashSet<usize>,
}

impl<'a> Handler<Payload, BroadcastState<'a>> for BroadcastHandler {
    fn step(
        &mut self,
        cluster_state: &ClusterState,
        io: &mut IO<Payload>,
        state: &mut BroadcastState,
        input: Message<Payload>,
        output: &mut StdoutLock,
    ) -> Result<()> {
        let payload = &input.body.payload;
        match payload {
            Payload::Topology => {
                let reply = Payload::TopologyOk;
                io.reply_to(&input, reply, output)?;
            },
            Payload::TopologyOk => bail!("unexpected topology_ok message"),
            Payload::Broadcast { message } => {
                state.messages.insert(message.to_owned());
                let reply = Payload::BroadcastOk;
                io.reply_to(&input, reply, output)?;
            },
            Payload::BroadcastOk => bail!("unexpected broadcast_ok message"),
            Payload::Read => {
                let values = state.messages.to_owned();
                let reply = Payload::ReadOk { messages: values.into_iter().collect()};
                io.reply_to(&input, reply, output)?;
            },
            Payload::ReadOk { .. } => bail!("unexpected read_ok message"),
        };

        Ok(())
    }
}
