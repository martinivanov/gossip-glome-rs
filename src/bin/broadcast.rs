use gossip_glomers_rs::{ClusterState, Event, Node, Server, Timers, IO};
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
    let mut node = Node::<BroadcastServer, Payload, Timer>::init()?;
    node.run()
}

struct BroadcastServer{
    messages: HashSet<usize>
}

impl Server<Payload, Timer> for BroadcastServer {
    fn init(_: &ClusterState, timers: &mut Timers<Payload, Timer>) -> Result<BroadcastServer> {
        timers.register_timer(Timer::Gossip, Duration::from_millis(500));

        let server = BroadcastServer {
            messages: HashSet::<usize>::new(),
        };

        Ok(server)
    }

    fn on_event(
        &mut self,
        _: &ClusterState,
        io: &mut IO<Payload>,
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
                        self.messages.insert(message.to_owned());
                        let reply = Payload::BroadcastOk;
                        io.reply_to(&msg, reply)?;
                    }
                    Payload::BroadcastOk => bail!("unexpected broadcast_ok message"),
                    Payload::Read => {
                        let values = self.messages.to_owned();
                        let reply = Payload::ReadOk {
                            messages: values.into_iter().collect(),
                        };
                        io.reply_to(&msg, reply)?;
                    }
                    Payload::ReadOk { .. } => bail!("unexpected read_ok message"),
                };
            }
            Event::Timer(t) => match t {
                Timer::Gossip => todo!()
            },
            Event::EOF => todo!(),
        }

        Ok(())
    }
}
