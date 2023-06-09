use gossip_glomers_rs::{ClusterState, Event, Server, Node, IO, Timers};
use serde::{Deserialize, Serialize};

use anyhow::{bail, Result};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Generate,
    GenerateOk { id: String },
}

fn main() -> anyhow::Result<()> {
    let mut node = Node::<(), UniqueIdServer, Payload, ()>::init(())?;
    node.run()
}

struct UniqueIdServer {}

impl Server<Payload, ()> for UniqueIdServer {
    fn init(_: &ClusterState, _: &mut Timers<Payload, ()>) -> Result<UniqueIdServer> {
        Ok(UniqueIdServer{})
    }

    fn on_event(
        &mut self,
        cluster_state: &ClusterState,
        io: &mut IO<Payload>,
        _: &mut (),
        input: Event<Payload, ()>,
    ) -> Result<()> {
        match input {
            Event::Message(msg) => {
                let payload = &msg.body.payload;
                match payload {
                    Payload::Generate => {
                        let id = format!("{}-{}", cluster_state.node_id, io.seq);
                        let reply = Payload::GenerateOk { id };
                        io.reply_to(&msg, reply)?;
                    }
                    Payload::GenerateOk { .. } => {
                        bail!("received unexpected GenerateOk message");
                    }
                };
            },
            _ =>  { },
        }

        Ok(())
    }
}
