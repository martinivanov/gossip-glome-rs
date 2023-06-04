use serde::{Deserialize, Serialize};
use serde_json::{Deserializer, Serializer};
use std::io::{BufWriter, LineWriter, StdoutLock, Write};

use anyhow::{Context, Result};

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Body {
    #[serde(rename = "msg_id")]
    id: Option<usize>,
    in_reply_to: Option<usize>,
    #[serde(flatten)]
    payload: Payload,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Message {
    src: String,
    #[serde(rename = "dest")]
    dst: String,
    body: Body,
}

fn main() -> Result<()> {
    let stdin = std::io::stdin().lock();
    let in_stream = Deserializer::from_reader(stdin).into_iter::<Message>();

    let mut stdout = std::io::stdout().lock();

    let mut node = EchoNode {
        id: "".to_string(),
        seq: 0,
    };

    for msg in in_stream {
        let msg = msg.context("Bad STDIN input")?;
        node.step(msg, &mut stdout)
            .context("failed to execute step")?;
    }

    Ok(())
}

struct AndNewLineWriter<W>
where
    W: std::io::Write,
{
    inner: W,
}

impl<W: std::io::Write> AndNewLineWriter<W> {
    pub fn new(writer: W) -> Self {
        AndNewLineWriter { inner: writer }
    }
}

impl<W: std::io::Write> Write for AndNewLineWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.write_all(buf)?;
        self.inner.write_all(b"\n")?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

struct EchoNode {
    id: String,
    seq: usize,
}

impl EchoNode {
    pub fn step(&mut self, input: Message, output: &mut StdoutLock) -> Result<()> {
        match input.body.payload {
            Payload::Init { node_id, node_ids } => {
                self.id = node_id;

                let reply = Message {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        id: Some(self.seq),
                        in_reply_to: input.body.id,
                        payload: Payload::InitOk,
                    },
                };
                self.seq += 1;

                serde_json::to_writer(&mut *output, &reply)
                    .context("failed to write serialized output to STDOUT")?;
                output
                    .write_all(b"\n")
                    .context("failed to append new line character to STDOUT")?;
                output
                    .flush()
                    .context("failed to flush message to STDOUT")?;
            }
            Payload::InitOk => {}
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

                serde_json::to_writer(&mut *output, &reply)
                    .context("failed to write serialized output to STDOUT")?;
                output
                    .write_all(b"\n")
                    .context("failed to append new line character to STDOUT")?;
                output
                    .flush()
                    .context("failed to flush message to STDOUT")?;
            }
            Payload::EchoOk { .. } => {}
        };

        Ok(())
    }
}
