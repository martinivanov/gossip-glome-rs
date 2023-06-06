use gossip_glomers_rs::{event_loop, Node, Init, Message, Body};
use serde::{Deserialize, Serialize};
use std::io::{StdoutLock};

use anyhow::{Result};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Echo { echo: String },
    EchoOk { echo: String },
}

fn main() -> anyhow::Result<()> {
    event_loop::<EchoNode, Payload>()
}

struct EchoNode {
    id: String,
    seq: usize,
}

impl Node<Payload> for EchoNode {
    fn init(init: Init) -> anyhow::Result<Self> {
        let node = EchoNode {
            id: init.node_id,
            seq: 0,
        };

        Ok(node)
    }

    fn step(&mut self, input: Message<Payload>, output: &mut StdoutLock) -> Result<()> {
        match input.body.payload {
            Payload::Echo { echo } => {
                let reply = Message {
                    src: self.id.clone(),
                    dst: input.src,
                    body: Body {
                        id: Some(self.seq),
                        in_reply_to: input.body.id,
                        payload: Payload::EchoOk { echo },
                    },
                };
                self.seq += 1;

                reply.send(output)?
            }
            Payload::EchoOk { .. } => {}
        };

        Ok(())
    }
}
