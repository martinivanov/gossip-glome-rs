use gossip_glomers_rs::{ClusterState, Message, Node, Server, Timers, IO};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use anyhow::{bail, Result};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
    Broadcast {
        message: usize,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: Vec<usize>,
    },
    Gossip {
        messages: Vec<usize>,
    },
}

#[derive(Clone, Copy, Debug)]
enum Timer {
    Gossip,
    RetryBroadcast,
}

fn main() -> anyhow::Result<()> {
    let mut node = Node::<BroadcastServer, Payload, Timer>::init()?;
    node.run()
}

struct BroadcastServer {
    messages: HashSet<usize>,
    seen: HashMap<String, HashSet<usize>>,
    neighbours: Vec<String>,
    pending_broadcasts: HashMap<usize, (String, Payload)>,
}

impl Server<Payload, Timer> for BroadcastServer {
    fn init(
        cluster_state: &ClusterState,
        timers: &mut Timers<Payload, Timer>,
    ) -> Result<BroadcastServer> {
        //timers.register_timer(Timer::Gossip, Duration::from_millis(300));
        timers.register_timer(Timer::RetryBroadcast, Duration::from_millis(1000));

        let seen = cluster_state
            .node_ids
            .iter()
            .map(|n| (n.to_string(), HashSet::new()))
            .collect();

        let neighbours = Vec::new();

        let server = BroadcastServer {
            messages: HashSet::<usize>::new(),
            seen,
            neighbours,
            pending_broadcasts: HashMap::<usize, (String, Payload)>::new(),
        };

        Ok(server)
    }

    fn on_message(
        &mut self,
        cluster_state: &ClusterState,
        io: &mut IO<Payload>,
        input: Message<Payload>,
    ) -> Result<()> {
        let payload = &input.body.payload;
        match payload {
            Payload::Topology { topology } => {
                let neighbours = topology
                    .get(&cluster_state.node_id)
                    .unwrap()
                    .iter()
                    .cloned();

                self.neighbours.extend(neighbours);
                eprintln!("Discovered neighbours: {:?}", &self.neighbours);

                let reply = Payload::TopologyOk;
                io.rpc_reply_to(&input, reply)?;
            }
            Payload::TopologyOk => bail!("unexpected topology_ok message"),
            Payload::Broadcast { message } => {
                if !self.messages.contains(message) {
                    for n in self.neighbours.iter().cloned() {
                        let dst = n.clone();
                        let broadcast = Payload::Broadcast {
                            message: message.clone(),
                        };

                        _ = io.rpc_request_with_retry(dst.to_string(), broadcast.clone())?;
                    }
                }

                self.messages.insert(message.to_owned());

                let reply = Payload::BroadcastOk;
                io.rpc_reply_to(&input, reply)?;
            }
            Payload::BroadcastOk => {
                io.rpc_mark_completed(&input);
            }
            Payload::Read => {
                let values = self.messages.to_owned();
                let reply = Payload::ReadOk {
                    messages: values.into_iter().collect(),
                };
                io.rpc_reply_to(&input, reply)?;
            }
            Payload::ReadOk { .. } => bail!("unexpected read_ok message"),
            Payload::Gossip { messages } => {
                let new = messages
                    .iter()
                    .copied()
                    .filter(|&m| self.messages.insert(m))
                    .map(|m| m.clone());

                self.seen
                    .get_mut(&input.src)
                    .expect("got gossip from unknown node")
                    .extend(new);
            }
        };

        Ok(())
    }

    fn on_timer(
        &mut self,
        cluster_state: &ClusterState,
        io: &mut IO<Payload>,
        input: Timer,
    ) -> Result<()>
    where
        Self: Sized,
    {
        match input {
            Timer::Gossip => {
                //let dst = cluster_state
                //    .node_ids
                //    .choose(&mut rand::thread_rng())
                //    .expect("couldn't pick random node")
                //    .to_string();

                for n in &self.neighbours {
                    let dst_seen = &self.seen[n];
                    let to_send: Vec<usize> = self.messages.difference(dst_seen).copied().collect();

                    if !to_send.is_empty() {
                        let gossip = Payload::Gossip { messages: to_send };
                        io.send(n.to_string(), None, gossip)?;
                    }
                }
            }
            Timer::RetryBroadcast => {
                io.rpc_run_retries()?
            }
        }

        Ok(())
    }
}
