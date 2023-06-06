use serde::{Deserialize, Serialize};
use serde_json::{Deserializer, Serializer};
use std::io::{BufRead, BufWriter, LineWriter, StdoutLock, Write};

use anyhow::{bail, Context, Result};

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Body<P> {
    #[serde(rename = "msg_id")]
    id: Option<usize>,
    in_reply_to: Option<usize>,
    #[serde(flatten)]
    payload: P,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InitPayload {
    Init(Init),
    InitOk,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Init {
    node_id: String,
    node_ids: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Message<P> {
    src: String,
    #[serde(rename = "dest")]
    dst: String,
    body: Body<P>,
}

impl<P> Message<P> {
    fn send(&self, output: &mut impl Write) -> anyhow::Result<()>
    where
        P: Serialize,
    {
        serde_json::to_writer(&mut *output, &self).context("serializing message")?;
        output
            .write_all(b"\n")
            .context("appending trailing newline")?;
        output.flush().context("flushing message to STDOUT")?;

        Ok(())
    }
}

fn event_loop() -> anyhow::Result<()> {
    let mut stdin = std::io::stdin().lock().lines();

    let init_msg: Message<InitPayload> = serde_json::from_str(
        &stdin
            .next()
            .expect("failed reading line from STDIN")
            .context("failed to read init message from STDIN")?,
    )
    .context("failed to deserialize init message from STDIN")?;

    drop(stdin);

    let mut stdout = std::io::stdout().lock();


    let InitPayload::Init(init) = init_msg.body.payload else {
        panic!("first message should be init");
    };

    let mut node = EchoNode {
        id: init.node_id,
        seq: 0,
    };


    let reply = Message::<InitPayload> {
        src: init_msg.dst,
        dst: init_msg.src,
        body: Body {
            id: Some(0),
            in_reply_to: init_msg.body.id,
            payload: InitPayload::InitOk,
        },
    };

    reply
        .send(&mut stdout)
        .context("failed sending init_ok to STDOUT")?;

    let stdin = std::io::stdin().lock();
    let in_stream = serde_json::Deserializer::from_reader(stdin).into_iter();

    for msg in in_stream {
        let msg = msg.context("failed to deserialize message from STDIN")?;
        node.step(msg, &mut stdout)
            .context("failed processing message")?;
    }

    Ok(())
}

fn main() -> anyhow::Result<()> {
    event_loop()
}

trait Node<P> {
    fn step(&mut self, input: Message<P>, output: &mut StdoutLock) -> Result<()>;
}

struct EchoNode {
    id: String,
    seq: usize,
}

impl Node<Payload> for EchoNode {
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
