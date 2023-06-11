use gossip_glomers_rs::{ClusterState, Message, Node, Server, Timers, IO};
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
    let mut node = Node::<UniqueIdServer, Payload, ()>::init()?;
    node.run()
}

struct UniqueIdServer {}

impl Server<Payload, ()> for UniqueIdServer {
    fn init(_: &ClusterState, _: &mut Timers<Payload, ()>) -> Result<UniqueIdServer> {
        Ok(UniqueIdServer {})
    }

    fn on_message(
        &mut self,
        cluster_state: &ClusterState,
        io: &mut IO<Payload>,
        input: Message<Payload>,
    ) -> Result<()> {
        let payload = &input.body.payload;
        match payload {
            Payload::Generate => {
                let id = format!("{}-{}", cluster_state.node_id, io.seq);
                let reply = Payload::GenerateOk { id };
                io.rpc_reply_to(&input, &reply)?;
            }
            Payload::GenerateOk { .. } => {
                bail!("received unexpected GenerateOk message");
            }
        };

        Ok(())
    }

    fn on_timer(&mut self, _: &ClusterState, _: &mut IO<Payload>, _: ()) -> Result<()>
    where
        Self: Sized,
    {
        todo!()
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
