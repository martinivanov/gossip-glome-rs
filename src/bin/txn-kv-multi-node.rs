use std::{collections::HashMap, time::Duration};

use gossip_glomers_rs::{ClusterState, Message, Node, Server, Timers, IO};
use serde::{ser::SerializeSeq, Deserialize, Serialize};

use anyhow::{bail, Result};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Txn { txn: Vec<Op> },
    TxnOk { txn: Vec<Op> },

    Replicate { ops: Vec<Op> },
    ReplicateOk,
}

#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(tag = "0")]
enum Op {
    #[serde(rename = "r")]
    Read { key: usize, value: Option<usize> },
    #[serde(rename = "w")]
    Write { key: usize, value: usize },
}

impl Serialize for Op {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(3))?;
        match self {
            Op::Read { key, value } => {
                seq.serialize_element("r")?;
                seq.serialize_element(key)?;
                seq.serialize_element(value)?;
            }
            Op::Write { key, value } => {
                seq.serialize_element("w")?;
                seq.serialize_element(key)?;
                seq.serialize_element(value)?;
            }
        }

        seq.end()
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Txn {
    txn: Vec<Op>,
}

fn main() -> anyhow::Result<()> {
    let mut node = Node::<TxnKVServer, Payload, ()>::init()?;
    node.run()
}

struct TxnKVServer {
    store: HashMap<usize, usize>,
}

impl Server<Payload, ()> for TxnKVServer {
    fn init(_: &ClusterState, _: &mut Timers<Payload, ()>) -> Result<TxnKVServer> {
        let server = TxnKVServer {
            store: HashMap::<usize, usize>::new(),
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
            Payload::Txn { txn } => {
                let mut result = Vec::new();
                let mut writes = Vec::new();
                for t in txn {
                    match t {
                        Op::Read { key, value: _ } => match self.store.get(key) {
                            Some(v) => {
                                result.push(Op::Read {
                                    key: *key,
                                    value: Some(*v),
                                });
                            }
                            None => {
                                result.push(Op::Read {
                                    key: *key,
                                    value: None,
                                });
                            }
                        },
                        Op::Write { key, value } => {
                            self.store.insert(*key, *value);
                            result.push(*t);
                            writes.push(*t);
                        }
                    }
                }

                if !writes.is_empty() {
                    let nodes = cluster_state
                        .node_ids
                        .iter()
                        .filter(|&n| n != &cluster_state.node_id);

                    for n in nodes {
                        let replicate = Payload::Replicate {
                            ops: writes.clone(),
                        };

                        io.rpc_request_with_retry(n, &replicate, Duration::from_millis(500))?;
                    }
                }

                let txn_ok = Payload::TxnOk { txn: result };
                io.rpc_reply_to(&input, &txn_ok)?;
            }
            Payload::Replicate { ops } if !io.rpc_still_pending(&input) => {
                for op in ops {
                    if let Op::Write { key, value } = op {
                        self.store.insert(*key, *value);
                    }
                }

                let replicate_ok = Payload::ReplicateOk {};
                io.rpc_reply_to(&input, &replicate_ok)?;
            }
            Payload::ReplicateOk => {
                io.rpc_mark_completed(&input);
            }
            _ if input.body.in_reply_to.is_some() && !io.rpc_still_pending(&input) => {
                eprintln!("received late response");
            }
            _ => bail!("unexpected payload {:?}", payload),
        };

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
        _: gossip_glomers_rs::Request<Payload>,
    ) -> Result<()>
    where
        Self: Sized,
    {
        Ok(())
    }
}
