use serde::{Deserialize, Serialize};
use std::{
    io::{BufRead, StdoutLock, Write},
    marker::PhantomData,
    sync::{mpsc, Arc},
    thread,
};

use anyhow::{Context, Result};

pub struct ClusterState {
    pub node_id: String,
    pub node_ids: Vec<String>,
}

pub struct IO<'a, P>
where
    P: Serialize,
{
    pub seq: usize,
    cluster_state: Arc<ClusterState>,
    stdout: StdoutLock<'a>,
    _payload: PhantomData<P>,
}

impl<'a, P> IO<'a, P>
where
    P: Serialize,
{
    pub fn send(
        &mut self,
        to: String,
        in_reply_to: Option<usize>,
        payload: P,
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

        serde_json::to_writer(&mut self.stdout, &message).context("serializing message")?;

        self.stdout
            .write_all(b"\n")
            .context("appending trailing newline")?;
        self.stdout.flush().context("flushing message to STDOUT")?;

        self.seq += 1;

        Ok(())
    }

    pub fn reply_to(&mut self, message: &Message<P>, reply: P) -> anyhow::Result<()> {
        let dst = message.src.clone();
        let in_reply_to = message.body.id;
        self.send(dst, in_reply_to, reply)
    }
}

pub struct Node<'a, S, H, P, T>
where
    H: Handler<P, T, S>,
    P: Sized + Serialize,
{
    pub cluster_state: Arc<ClusterState>,
    pub io: IO<'a, P>,
    pub state: S,
    pub handler: H,
    _timer: PhantomData<T>,
}

impl<'a, S, H, P, T> Node<'a, S, H, P, T>
where
    H: Handler<P, T, S>,
    P: Serialize + Deserialize<'a> + Send + 'static,
    T: Send + 'static,
{
    pub fn init(state: S, handler: H) -> anyhow::Result<Node<'a, S, H, P, T>> {
        let mut stdin = std::io::stdin().lock().lines();

        let init_msg: Message<InitPayload> = serde_json::from_str(
            &stdin
                .next()
                .expect("failed reading line from STDIN")
                .context("failed to read init message from STDIN")?,
        )
        .context("failed to deserialize init message from STDIN")?;

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
            stdout: std::io::stdout().lock(),
            _payload: PhantomData,
        };

        let node = Node::<S, H, P, T> {
            cluster_state: cluster_state.clone(),
            io,
            state,
            handler,
            _timer: PhantomData,
        };

        let mut init_io = IO::<InitPayload> {
            seq: 0,
            cluster_state: cluster_state.clone(),
            stdout: std::io::stdout().lock(),
            _payload: PhantomData,
        };

        init_io.reply_to(&init_msg, InitPayload::InitOk)?;

        Ok(node)
    }

    pub fn run(&mut self) -> anyhow::Result<()> {
        let (in_tx, in_rx) = mpsc::channel();

        let jh = thread::spawn(move || {
            let stdin = std::io::stdin().lock();
            let in_stream = serde_json::Deserializer::from_reader(stdin).into_iter();
            for msg in in_stream {
                let msg: Message<P> = msg.context("failed to deserialize message from STDIN")?;
                let event: Event<P, T> = Event::Message(msg);

                if let Err(_) = in_tx.send(event) {
                    return Ok::<_, anyhow::Error>(());
                }
            }

            if let Err(_) = in_tx.send(Event::EOF) {
                return Ok::<_, anyhow::Error>(());
            }

            Ok(())
        });

        for event in in_rx {
            self.handler
                .step(&self.cluster_state, &mut self.io, &mut self.state, event)
                .context("failed processing message")?;
        }

        jh.join()
            .expect("STDIN processing failed")?;

        Ok(())
    }
}

pub trait Handler<P, T, S = ()>
where
    P: Serialize,
{
    fn step(
        &mut self,
        cluster_state: &ClusterState,
        io: &mut IO<P>,
        state: &mut S,
        input: Event<P, T>,
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

pub enum Event<P, T> {
    Timer(T),
    Message(Message<P>),
    EOF,
}
