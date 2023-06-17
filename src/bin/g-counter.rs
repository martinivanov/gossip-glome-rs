use gossip_glomers_rs::{ClusterState, Message, Node, Server, Timers, IO};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap},
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
    Replicate { delta: usize },
    ReplicateOk,
}

fn main() -> anyhow::Result<()> {
    let mut node = Node::<GCounterServer, Payload, ()>::init()?;
    node.run()
}

struct GCounter {
    counters: HashMap<String, usize>,
}

impl GCounter {
    fn new() -> Self {
        GCounter {
            counters: HashMap::<String, usize>::new(),
        }
    }

    fn increment(&mut self, node: String, value: usize) {
        match self.counters.entry(node) {
            std::collections::hash_map::Entry::Occupied(o) => {
                let v = o.into_mut();
                *v += value;
            }
            std::collections::hash_map::Entry::Vacant(v) => {
                v.insert(value);
            }
        };
    }

    fn value(&self) -> usize {
        self.counters.values().sum()
    }
}

struct GCounterServer {
    gcounter: GCounter,
}

impl Server<Payload, ()> for GCounterServer {
    fn init(_: &ClusterState, _: &mut Timers<Payload, ()>) -> Result<GCounterServer> {
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
                self.gcounter.increment(input.src.to_string(), *delta);

                for node in cluster_state
                    .node_ids
                    .iter()
                    .filter(|&n| n != &cluster_state.node_id && n != &input.src)
                {
                    let replicate = Payload::Replicate { delta: *delta };

                    io.rpc_request_with_retry(node, &replicate, Duration::from_millis(500))?;
                }

                let add_ok = Payload::AddOk {};
                io.rpc_reply_to(&input, &add_ok)?;
            }
            Payload::AddOk => {
                io.rpc_mark_completed(&input);
            }
            Payload::Read => {
                let total = self.gcounter.value();
                let read_ok = Payload::ReadOk { value: total };

                io.rpc_reply_to(&input, &read_ok)?;
            }
            Payload::ReadOk { .. } => bail!("unexpected read_ok message"),
            Payload::Replicate { delta } => {
                self.gcounter.increment(input.src.to_string(), *delta);
                let replicate_ok = Payload::ReplicateOk {};
                io.rpc_reply_to(&input, &replicate_ok)?;
            }
            Payload::ReplicateOk => {
                io.rpc_mark_completed(&input);
            }
        }
        Ok(())
    }

    fn on_timer(&mut self, _: &ClusterState, _: &mut IO<Payload>, _: ()) -> Result<()>
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
