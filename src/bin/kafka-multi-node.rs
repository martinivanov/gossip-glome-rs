use std::{
    collections::{hash_map::Entry, HashMap},
    time::Duration,
};

use itertools::Itertools;

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
    ReplicaPoll {
        offsets: HashMap<String, Offset>,
    },
    ReplicaPollOk {
        msgs: HashMap<String, Vec<Record>>,
    },
}

#[derive(Clone, Copy, Debug)]
enum Timer {
    ReplicaPoll,
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

    fn append_records(&mut self, mut records: Vec<Record>) {
        self.records.append(&mut records);
    }

    fn read_from(&self, offset: Offset) -> Vec<Record> {
        // TODO: there is probably a better way
        self.records[offset..].iter().take(50).copied().collect()
    }

    fn current_offset(&self) -> Offset {
        match self.records.last() {
            Some((offset, _)) => *offset,
            None => 0,
        }
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
        timers.register_timer(Timer::ReplicaPoll, Duration::from_millis(250));

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
            } if io.rpc_still_pending(&input) => {
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

                    io.rpc_request_with_retry(n, &commit_offsets, Duration::from_millis(250))?;
                }

                let commit_offsets_ok = Payload::CommitOffsetsOk {};
                io.rpc_reply_to(&input, &commit_offsets_ok)?;
            }
            Payload::CommitOffsetsOk if io.rpc_still_pending(&input) => {
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
            Payload::ReplicaPoll { offsets } => {
                let messages: HashMap<String, Vec<Record>> = self
                    .logs
                    .iter()
                    .filter(|(k, _)| {
                        let Ok(int_key) = k.parse::<usize>() else {
                            return false;
                        };

                        let leader = int_key % cluster_state.node_ids.len();
                        leader == self.my_id
                    })
                    .map(|(k, v)| {
                        let offset = offsets.get(k).unwrap_or(&0);
                        let records = v.read_from(*offset);
                        (k.clone(), records)
                    })
                    .collect();

                let replica_poll_ok = Payload::ReplicaPollOk { msgs: messages };
                io.rpc_reply_to(&input, &replica_poll_ok)?;
            }
            Payload::ReplicaPollOk { msgs } if io.rpc_still_pending(&input) => {
                for (l, recs) in msgs {
                    let log = self.logs.entry(l.clone()).or_insert_with(Log::new);
                    log.append_records(recs.to_vec())
                }

                io.rpc_mark_completed(&input);
            }
            _ if input.body.in_reply_to.is_some() && !io.rpc_still_pending(&input) => {
                eprintln!("received late response");
            }
            _ => bail!("unexpected payload {:?}", payload),
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
            Timer::ReplicaPoll => {
                let offset_groups = self
                    .logs
                    .iter()
                    .filter_map(|(k, v)| {
                        let Ok(int_key) = k.parse::<usize>() else {
                            return None;
                        };

                        let leader = int_key % cluster_state.node_ids.len();
                        Some((leader, k, v))
                    })
                    .filter(|(leader, _, _)| leader != &self.my_id)
                    .group_by(|(leader, _, _)| *leader);

                let requests: HashMap<String, Payload> = offset_groups
                    .into_iter()
                    .map(|(leader, group)| {
                        let offsets: HashMap<String, Offset> = group
                            .map(|(_, k, l)| {
                                let offset = l.current_offset();
                                (k.to_string(), offset + 1)
                            })
                            .collect();

                        let node = format!("n{}", leader);
                        let request = Payload::ReplicaPoll { offsets };
                        (node, request)
                    })
                    .collect();

                let empty = Payload::ReplicaPoll {
                    offsets: HashMap::<String, Offset>::new(),
                };

                for node in cluster_state
                    .node_ids
                    .iter()
                    .filter(|&n| n != &cluster_state.node_id)
                {
                    let request = requests.get(node).unwrap_or(&empty);
                    io.rpc_request(&node, &request, Duration::from_secs(5), false)?;
                }
            }
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
