use serde::{Deserialize, Serialize};
use std::{
    io::{BufRead, StdoutLock, Write},
    marker::PhantomData,
    sync::Arc,
};

use anyhow::{Context, Result};

pub struct ClusterState {
    pub node_id: String,
    pub node_ids: Vec<String>,
}

pub struct IO<P>
where
    P: Serialize,
{
    pub seq: usize,
    cluster_state: Arc<ClusterState>,
    _payload: PhantomData<P>,
}

impl<'a, P> IO<P>
where
    P: Serialize,
{
    pub fn send(
        &mut self,
        to: String,
        in_reply_to: Option<usize>,
        payload: P,
        output: &mut impl Write,
    ) -> anyhow::Result<()> {
        let message = Message::<P> {
            src: self.cluster_state.node_id.clone(),
            dst: to,
            body: Body::<P> {
                id: Some(self.seq),
                in_reply_to,
                payload,
            },
        };

        serde_json::to_writer(&mut *output, &message).context("serializing message")?;

        output
            .write_all(b"\n")
            .context("appending trailing newline")?;
        output.flush().context("flushing message to STDOUT")?;

        self.seq += 1;

        Ok(())
    }

    pub fn reply_to(
        &mut self,
        message: &Message<P>,
        reply: P,
        output: &mut impl Write,
    ) -> anyhow::Result<()> {
        let dst = message.src.clone();
        let in_reply_to = message.body.id;
        self.send(dst, in_reply_to, reply, output)
    }
}

pub struct Node<S, H, P>
where
    H: Handler<P, S>,
    P: Sized + Serialize,
{
    pub cluster_state: Arc<ClusterState>,
    pub io: IO<P>,
    pub state: S,
    pub handler: H,
}

impl<'a, S, H, P> Node<S, H, P>
where
    H: Handler<P, S>,
    P: Serialize + Deserialize<'a>,
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

        let InitPayload::Init(init) = &init_msg.body.payload else {
            panic!("first message should be init");
        };

        let cluster_state = ClusterState {
            node_id: init.node_id.clone(),
            node_ids: init.node_ids.clone(),
        };

        let cluster_state = Arc::new(cluster_state);

        let io = IO::<P> {
            seq: 0,
            cluster_state: cluster_state.clone(),
            _payload: PhantomData,
        };

        let node = Node::<S, H, P> {
            cluster_state: cluster_state.clone(),
            io,
            state,
            handler,
        };

        let mut init_io = IO::<InitPayload> {
            seq: 0,
            cluster_state: cluster_state.clone(),
            _payload: PhantomData,
        };

        init_io.reply_to(&init_msg, InitPayload::InitOk, &mut stdout)?;

        Ok(node)
    }

    pub fn run(&mut self) -> anyhow::Result<()> {
        let stdin = std::io::stdin().lock();
        let in_stream = serde_json::Deserializer::from_reader(stdin).into_iter();

        let mut stdout = std::io::stdout().lock();

        for msg in in_stream {
            let msg = msg.context("failed to deserialize message from STDIN")?;
            self.handler
                .step(
                    &self.cluster_state,
                    &mut self.io,
                    &mut self.state,
                    msg,
                    &mut stdout,
                )
                .context("failed processing message")?;
        }

        Ok(())
    }
}

pub trait Handler<P, S = ()>
where
    P: Serialize,
{
    fn step(
        &mut self,
        cluster_state: &ClusterState,
        io: &mut IO<P>,
        state: &mut S,
        input: Message<P>,
        output: &mut StdoutLock,
    ) -> Result<()>
    where
        Self: Sized;
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
