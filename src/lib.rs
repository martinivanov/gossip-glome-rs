use serde::{Deserialize, Serialize};
use std::{
    io::{BufRead, StdoutLock, Write},
    marker::PhantomData,
};

use anyhow::{Context, Result};

pub struct Node<S, H, P>
where
    H: Handler<P, S>,
    P: Sized,
{
    pub id: String,
    pub seq: usize,
    pub state: S,
    pub handler: H,
    _payload: PhantomData<P>,
}

impl<'a, S, H, P> Node<S, H, P>
where
    H: Handler<P, S>,
    P: Serialize + Deserialize<'a>
{
    pub fn init(state: S, handler: H) -> anyhow::Result<Node<S, H, P>> {
        let mut stdin = std::io::stdin().lock().lines();

        let init_msg: Message<InitPayload> = serde_json::from_str(
            &stdin
                .next()
                .expect("failed reading line from STDIN")
                .context("failed to read init message from STDIN")?,
        )
        .context("failed to deserialize init message from STDIN")?;

        let mut stdout = std::io::stdout().lock();

        let InitPayload::Init(init) = init_msg.body.payload else {
            panic!("first message should be init");
        };

        let node = Node::<S, H, P> {
            id: init.node_id,
            seq: 0,
            state,
            handler,
            _payload: PhantomData,
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

        Ok(node)
    }

    pub fn run(&mut self) -> anyhow::Result<()> {
        let stdin = std::io::stdin().lock();
        let in_stream = serde_json::Deserializer::from_reader(stdin).into_iter();

        let mut stdout = std::io::stdout().lock();

        for msg in in_stream {
            let msg = msg.context("failed to deserialize message from STDIN")?;
            self.handler.step(msg, self.state, &mut stdout)
                .context("failed processing message")?;
        }

        Ok(())
    }
}

pub trait Handler<P, S> {
    fn step(&mut self, state: S, input: Message<P>, output: &mut StdoutLock) -> Result<()>;
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Body<P> {
    #[serde(rename = "msg_id")]
    pub id: Option<usize>,
    pub in_reply_to: Option<usize>,
    #[serde(flatten)]
    pub payload: P,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InitPayload {
    Init(Init),
    InitOk,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Init {
    pub node_id: String,
    pub node_ids: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Message<P> {
    pub src: String,
    #[serde(rename = "dest")]
    pub dst: String,
    pub body: Body<P>,
}

impl<P> Message<P> {
    pub fn send(&self, output: &mut impl Write) -> anyhow::Result<()>
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
