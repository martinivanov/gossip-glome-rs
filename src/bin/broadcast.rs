use gossip_glomers_rs::{ClusterState, Event, Message, Node, Server, Timers, IO};
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
    Gossip { message: String }
}

#[derive(Clone, Copy, Debug)]
enum Timer {
    Gossip,
}

fn main() -> anyhow::Result<()> {
    let mut node = Node::<BroadcastServer, Payload, Timer>::init()?;
    node.run()
}

struct BroadcastServer {
    messages: HashSet<usize>,
}

impl Server<Payload, Timer> for BroadcastServer {
    fn init(_: &ClusterState, timers: &mut Timers<Payload, Timer>) -> Result<BroadcastServer> {
        timers.register_timer(Timer::Gossip, Duration::from_millis(500));

        let server = BroadcastServer {
            messages: HashSet::<usize>::new(),
        };

        Ok(server)
    }

    fn on_message(
        &mut self,
        _: &ClusterState,
        io: &mut IO<Payload>,
        input: Message<Payload>,
    ) -> Result<()> {
        let payload = &input.body.payload;
        match payload {
            Payload::Topology => {
                let reply = Payload::TopologyOk;
                io.reply_to(&input, reply)?;
            }
            Payload::TopologyOk => bail!("unexpected topology_ok message"),
            Payload::Broadcast { message } => {
                self.messages.insert(message.to_owned());
                let reply = Payload::BroadcastOk;
                io.reply_to(&input, reply)?;
            }
            Payload::BroadcastOk => bail!("unexpected broadcast_ok message"),
            Payload::Read => {
                let values = self.messages.to_owned();
                let reply = Payload::ReadOk {
                    messages: values.into_iter().collect(),
                };
                io.reply_to(&input, reply)?;
            }
            Payload::ReadOk { .. } => bail!("unexpected read_ok message"),
            Payload::Gossip { message } => {
                println!("{}", message);
            }
        };

        Ok(())
    }

    fn on_timer(&mut self, cluster_state: &ClusterState, io: &mut IO<Payload>, input: Timer) -> Result<()>
    where
        Self: Sized,
    {
        match input {
            Timer::Gossip => {
                for n in &cluster_state.node_ids {
                    io.send(n.clone(), None, Payload::Gossip { message: format!("{}-gosho", &n) })?;
                }
            },
        }

        Ok(())
    }
}
