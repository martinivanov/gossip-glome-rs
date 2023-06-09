use gossip_glomers_rs::{ClusterState, Event, Node, Server, IO, Timers};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, time::Duration};

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

#[derive(Clone, Copy, Debug)]
enum Timer {
    Gossip,
}

fn main() -> anyhow::Result<()> {
    let state = BroadcastState {
        messages: &mut HashSet::new(),
    };

    let mut node = Node::<BroadcastState, BroadcastServer, Payload, Timer>::init(state)?;
    node.run()
}

struct BroadcastServer {}

struct BroadcastState<'a> {
    messages: &'a mut HashSet<usize>,
}

impl<'a> Server<Payload, Timer, BroadcastState<'a>> for BroadcastServer {
    fn init(_: &ClusterState, timers: &mut Timers<Payload, Timer>) -> Result<BroadcastServer> {
        let server = BroadcastServer{
        };

        timers.register_timer(Timer::Gossip, Duration::from_millis(500));

        Ok(server)
    }

    fn on_event(
        &mut self,
        _: &ClusterState,
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
            Event::Timer(t) => match t {
                Timer::Gossip => {
                    println!("sadasddd: {:?}", t);
                },
            },
            Event::EOF => todo!(),
        }

        Ok(())
    }
}
