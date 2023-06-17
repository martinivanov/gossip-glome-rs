use std::{
    collections::{hash_map::Entry, HashMap},
    time::Duration,
};

use gossip_glomers_rs::{ClusterState, Message, Node, Server, Timers, IO};
use serde::{Deserialize, Serialize};

use anyhow::{bail, Result};

type Offset = usize;
type Record = (Offset, usize);
type ForwardedFor = (String, usize);

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Send {
        key: String,
        msg: usize,
        #[serde(skip_serializing_if = "Option::is_none")]
        forwarded_for: Option<ForwardedFor>,
    },
    SendOk {
        offset: Offset,
        #[serde(skip_serializing_if = "Option::is_none")]
        forwarded_for: Option<ForwardedFor>,
    },
    Poll {
        offsets: HashMap<String, Offset>,
    },
    PollOk {
        msgs: HashMap<String, Vec<Record>>,
    },
    CommitOffsets {
        offsets: HashMap<String, Offset>,
    },
    CommitOffsetsOk,
    ListCommittedOffsets {
        keys: Vec<String>,
    },
    ListCommittedOffsetsOk {
        offsets: HashMap<String, Offset>,
    },
}

#[derive(Clone, Copy, Debug)]
enum Timer {
    Poll,
}

fn main() -> anyhow::Result<()> {
    let mut node = Node::<KafkaServer, Payload, Timer>::init()?;
    node.run()
}

struct Log {
    records: Vec<Record>,
}

impl Log {
    fn new() -> Self {
        Log {
            records: Vec::<Record>::new(),
        }
    }

    fn append(&mut self, message: usize) -> Offset {
        let offset = self.records.len();
        let record = (offset, message);
        self.records.push(record);
        offset
    }

    fn read_from(&self, offset: Offset) -> Vec<Record> {
        // TODO: there is probably a better way
        self.records[offset..].iter().take(10).copied().collect()
    }
}

struct KafkaServer {
    logs: HashMap<String, Log>,
    offset_store: HashMap<String, Offset>,
    my_id: usize,
}

impl Server<Payload, Timer> for KafkaServer {
    fn init(
        cluster_state: &ClusterState,
        timers: &mut Timers<Payload, Timer>,
    ) -> Result<KafkaServer> {
        timers.register_timer(Timer::Poll, Duration::from_millis(100));

        let my_id = cluster_state.node_id[1..].parse::<usize>()?;
        Ok(KafkaServer {
            logs: HashMap::<String, Log>::new(),
            offset_store: HashMap::<String, Offset>::new(),
            my_id,
        })
    }

    fn on_message(
        &mut self,
        cluster_state: &ClusterState,
        io: &mut IO<Payload>,
        input: Message<Payload>,
    ) -> Result<()> {
        let payload = &input.body.payload;
        match payload {
            Payload::Send {
                key,
                msg,
                forwarded_for,
            } => {
                let int_key = key.parse::<usize>()?;
                let leader = int_key % cluster_state.node_ids.len();
                if leader == self.my_id {
                    let log = match self.logs.entry(key.to_string()) {
                        Entry::Occupied(o) => o.into_mut(),
                        Entry::Vacant(v) => v.insert(Log::new()),
                    };

                    let offset = log.append(*msg);
                    let send_ok = Payload::SendOk {
                        offset,
                        forwarded_for: forwarded_for.clone(),
                    };
                    io.rpc_reply_to(&input, &send_ok)?;
                } else {
                    let send = Payload::Send {
                        key: key.to_string(),
                        msg: *msg,
                        forwarded_for: Some((input.src, input.body.id.unwrap())),
                    };

                    let dst = format!("n{}", leader);
                    io.rpc_request_with_retry(&dst, &send, Duration::from_millis(250))?;
                }
            }
            Payload::SendOk {
                offset,
                forwarded_for,
            } => {
                let (dst, msg_id) = forwarded_for.clone().unwrap();
                let send_ok = Payload::SendOk {
                    offset: *offset,
                    forwarded_for: None,
                };

                io.send(&dst, Some(msg_id), &send_ok)?;
                io.rpc_mark_completed(&input);
            }
            Payload::Poll { offsets } => {
                let messages: HashMap<String, Vec<Record>> = offsets
                    .iter()
                    .map(|(key, offset)| {
                        let records = match self.logs.entry(key.to_string()) {
                            Entry::Occupied(o) => o.get().read_from(*offset),
                            Entry::Vacant(_) => Vec::<Record>::new(),
                        };

                        (key.to_string(), records)
                    })
                    .collect();

                let poll_ok = Payload::PollOk { msgs: messages };
                io.rpc_reply_to(&input, &poll_ok)?;
            }
            Payload::PollOk { msgs } => todo!(),
            Payload::CommitOffsets { offsets } => {
                for (key, value) in offsets {
                    match self.offset_store.entry(key.to_string()) {
                        Entry::Occupied(o) => {
                            let current = o.into_mut();
                            let c = *current;
                            *current = c.max(*value);
                        }
                        Entry::Vacant(v) => {
                            v.insert(*value);
                        }
                    }
                }

                let nodes = cluster_state
                    .node_ids
                    .iter()
                    .filter(|&n| n != &cluster_state.node_id && n != &input.src);

                for n in nodes {
                    let commit_offsets = Payload::CommitOffsets {
                        offsets: offsets.clone(),
                    };

                    io.rpc_request_with_retry(&n, &commit_offsets, Duration::from_millis(250))?;
                }

                let commit_offsets_ok = Payload::CommitOffsetsOk {};
                io.rpc_reply_to(&input, &commit_offsets_ok)?;
            }
            Payload::CommitOffsetsOk => {
                io.rpc_mark_completed(&input);
            }
            Payload::ListCommittedOffsets { keys } => {
                let mut offsets = HashMap::new();
                for k in keys {
                    if let Some(offset) = self.offset_store.get(k) {
                        offsets.insert(k.to_string(), *offset);
                    }
                }

                let list_committed_offsets_ok = Payload::ListCommittedOffsetsOk { offsets };
                io.rpc_reply_to(&input, &list_committed_offsets_ok)?;
            }
            Payload::ListCommittedOffsetsOk {..} => bail!("unexpected list_committed_offsets_ok message"),
        };

        Ok(())
    }

    fn on_timer(
        &mut self,
        cluster_state: &ClusterState,
        io: &mut IO<Payload>,
        timer: Timer,
    ) -> Result<()>
    where
        Self: Sized,
    {
        match timer {
            Timer::Poll => {}
        }

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
        bail!("unexpected RPC timeout");
    }
}
