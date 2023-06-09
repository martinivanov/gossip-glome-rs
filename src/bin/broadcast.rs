use gossip_glomers_rs::{ClusterState, Event, Handler, Node, IO};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use anyhow::{bail, Result};

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

enum Timer {
    Gossip,
}

fn main() -> anyhow::Result<()> {
    let handler = BroadcastHandler {};
    let state = BroadcastState {
        messages: &mut HashSet::new(),
    };

    let mut node = Node::<BroadcastState, BroadcastHandler, Payload, Timer>::init(state, handler)?;
    node.run()
}

struct BroadcastHandler {}

struct BroadcastState<'a> {
    messages: &'a mut HashSet<usize>,
}

impl<'a> Handler<Payload, Timer, BroadcastState<'a>> for BroadcastHandler {
    fn step(
        &mut self,
        cluster_state: &ClusterState,
        io: &mut IO<Payload>,
        state: &mut BroadcastState,
        input: Event<Payload, Timer>,
    ) -> Result<()> {
        match input {
            Event::Message(msg) => {
                let payload = &msg.body.payload;
                match payload {
                    Payload::Topology => {
                        let reply = Payload::TopologyOk;
                        io.reply_to(&msg, reply)?;
                    }
                    Payload::TopologyOk => bail!("unexpected topology_ok message"),
                    Payload::Broadcast { message } => {
                        state.messages.insert(message.to_owned());
                        let reply = Payload::BroadcastOk;
                        io.reply_to(&msg, reply)?;
                    }
                    Payload::BroadcastOk => bail!("unexpected broadcast_ok message"),
                    Payload::Read => {
                        let values = state.messages.to_owned();
                        let reply = Payload::ReadOk {
                            messages: values.into_iter().collect(),
                        };
                        io.reply_to(&msg, reply)?;
                    }
                    Payload::ReadOk { .. } => bail!("unexpected read_ok message"),
                };
            }
            Event::Timer(t) => {
                match t {
                    Timer::Gossip => todo!(),
                }
            },
            Event::EOF => todo!(),
        }

        Ok(())
    }
}
