use std::collections::HashMap;

use gossip_glomers_rs::{ClusterState, Message, Node, Server, Timers, IO};
use serde::{Deserialize, Serialize};

use anyhow::{bail, Result};

type Offset = usize;
type Record = (Offset, usize);

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Send { key: String, msg: usize },
    SendOk { offset: Offset },
    Poll { offsets: HashMap<String, Offset> },
    PollOk { msgs: HashMap<String, Vec<Record>> },
    CommitOffsets { offsets: HashMap<String, Offset> },
    CommitOffsetsOk,
    ListCommittedOffsets { keys: Vec<String> },
    ListCommittedOffsetsOk { offsets: HashMap<String, Offset> },
}

fn main() -> anyhow::Result<()> {
    let mut node = Node::<KafkaServer, Payload, ()>::init()?;
    node.run()
}

struct Log {
    records: Vec<Record>,
}

impl Log {
    fn new() -> Self {
        Log {
            records: Vec::<Record>::new()
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
}

impl Server<Payload, ()> for KafkaServer {
    fn init(_: &ClusterState, _: &mut Timers<Payload, ()>) -> Result<KafkaServer> {
        Ok(KafkaServer {
            logs: HashMap::<String, Log>::new(),
            offset_store: HashMap::<String, Offset>::new(),
        })
    }

    fn on_message(
        &mut self,
        _: &ClusterState,
        io: &mut IO<Payload>,
        input: Message<Payload>,
    ) -> Result<()> {
        let payload = &input.body.payload;
        match payload {
            Payload::Send { key, msg } => {
                let log = match self.logs.entry(key.to_string()) {
                    std::collections::hash_map::Entry::Occupied(o) => o.into_mut(),
                    std::collections::hash_map::Entry::Vacant(v) => v.insert(Log::new()),
                };

                let offset = log.append(*msg);

                let send_ok = Payload::SendOk { offset };
                io.rpc_reply_to(&input, &send_ok)?;
            },
            Payload::SendOk { offset } => todo!(),
            Payload::Poll { offsets } => {
                let messages: HashMap<String, Vec<Record>> = offsets.iter().map(|(key, offset)| {
                    let records = match self.logs.entry(key.to_string()) {
                        std::collections::hash_map::Entry::Occupied(o) => o.get().read_from(*offset),
                        std::collections::hash_map::Entry::Vacant(_) => Vec::<Record>::new(),
                    };

                    (key.to_string(), records)
                }).collect();

                let poll_ok = Payload::PollOk { msgs: messages };
                io.rpc_reply_to(&input, &poll_ok)?;
            },
            Payload::PollOk { msgs } => todo!(),
            Payload::CommitOffsets { offsets } => {
                for (k, v) in offsets {
                    self.offset_store.insert(k.to_string(), *v);
                }

                let commit_offsets_ok = Payload::CommitOffsetsOk{};
                io.rpc_reply_to(&input, &commit_offsets_ok)?;
            },
            Payload::CommitOffsetsOk => todo!(),
            Payload::ListCommittedOffsets { keys } => {
                let mut offsets = HashMap::new();
                for k in keys {
                    if let Some(offset) = self.offset_store.get(k) {
                        offsets.insert(k.to_string(), *offset);
                    }
                }

                let list_committed_offsets_ok = Payload::ListCommittedOffsetsOk { offsets };
                io.rpc_reply_to(&input, &list_committed_offsets_ok)?;
            },
            Payload::ListCommittedOffsetsOk { offsets } => todo!(),
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
        bail!("unexpected RPC timeout");
    }
}
