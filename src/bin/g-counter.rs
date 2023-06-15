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
    Add { delta: usize },
    AddOk,
    Read,
    ReadOk { value: usize },
    Replicate { values: HashSet<(usize, usize)> },
    ReplicateOk,
}

#[derive(Clone, Copy, Debug)]
enum Timer {
    Replicate,
}

fn main() -> anyhow::Result<()> {
    let mut node = Node::<GCounterServer, Payload, Timer>::init()?;
    node.run()
}

struct GCounter {
    node_counters: HashMap<String, HashSet<(usize, usize)>>,
}

impl GCounter {
    fn new() -> Self {
        GCounter {
            node_counters: HashMap::<String, HashSet<(usize, usize)>>::new()
        }
    }

    fn add(&mut self, node: String, id: usize, value: usize) -> anyhow::Result<()> {
        let counter = self.node_counters.entry(node).or_insert_with(HashSet::<(usize, usize)>::new);
        counter.insert((id, value));
        Ok(())
    }

    fn total(&self) -> anyhow::Result<usize> {
        let total: usize = self
            .node_counters
            .values()
            .flatten()
            .map(|(_, v)| v)
            .sum();
        Ok(total)
    }
}

struct GCounterServer {
    gcounter: GCounter,
}

impl Server<Payload, Timer> for GCounterServer {
    fn init(
        _: &ClusterState,
        _: &mut Timers<Payload, Timer>,
    ) -> Result<GCounterServer> {
        let server = GCounterServer {
            gcounter: GCounter::new(),
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
            Payload::Add { delta } => {
                let Some(id) = input.body.id else {
                    bail!("got RPC add message with no message id");
                    
                };

                self.gcounter.add(input.src.to_string(), id, *delta)?;

                for node in cluster_state.node_ids.iter().filter(|&n| n != &cluster_state.node_id && n != &input.src) {
                    let mut values = HashSet::<(usize, usize)>::new();
                    values.insert((id, *delta));

                    let replicate = Payload::Replicate {
                        values
                    };

                    io.rpc_request_with_retry(node, &replicate, Duration::from_millis(500))?;
                }

                let add_ok = Payload::AddOk{};
                io.rpc_reply_to(&input, &add_ok)?;
            },
            Payload::AddOk => {
            },
            Payload::Read => {
                let total = self.gcounter.total()?;
                let read_ok = Payload::ReadOk { value: total };

                io.rpc_reply_to(&input, &read_ok)?;
            },
            Payload::ReadOk { .. } => bail!("unexpected read_ok message"),
            Payload::Replicate { values } => {
                for v in values {
                    let (id, value) = *v;
                    self.gcounter.add(input.src.to_string(), id, value)?;
                }
            },
            Payload::ReplicateOk => {
                io.rpc_mark_completed(&input);
            },
        }
        Ok(())
    }

    fn on_timer(&mut self, _: &ClusterState, _: &mut IO<Payload>, _: Timer) -> Result<()>
    where
        Self: Sized,
    {
        Ok(())
    }

    fn on_rpc_timeout(
        &mut self,
        _: &ClusterState,
        timeout: gossip_glomers_rs::Request<Payload>,
    ) -> Result<()>
    where
        Self: Sized,
    {
        eprintln!("Timeout: {:?}", timeout);

        Ok(())
    }
}
