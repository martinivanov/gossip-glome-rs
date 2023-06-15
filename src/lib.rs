use serde::{Deserialize, Serialize};
use std::{
    cmp,
    collections::HashMap,
    io::{BufRead, StdoutLock, Write},
    marker::PhantomData,
    sync::{
        mpsc::{self, Receiver, Sender},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

use anyhow::{bail, Context, Result};

// TODO: membership table?
pub struct ClusterState {
    pub node_id: String,
    pub node_ids: Vec<String>,
}

//pub enum RPCRetryPolicy {
//    None,
//    FixedInterval,
//    ExponentialBackoffRetry {
//    }
//}

#[derive(Clone, Debug)]
pub struct Request<P> {
    pub id: usize,
    pub dst: String,
    pub payload: P,
    pub timeout: Duration,
    pub issued_at: Instant,
    // TODO: encapsulate all retry parameters in an enum?
    pub retry: bool,
}

pub struct IO<'a, P>
where
    P: Serialize,
{
    pub seq: usize,
    cluster_state: Arc<ClusterState>,
    stdout: StdoutLock<'a>,
    _payload: PhantomData<P>,
    pending_requests: HashMap<usize, Request<P>>,
}

impl<'a, P> IO<'a, P>
where
    P: Serialize + Clone,
{
    pub fn send(
        &mut self,
        to: &str,
        in_reply_to: Option<usize>,
        payload: &P,
    ) -> anyhow::Result<usize> {
        let message = Message::<P> {
            src: self.cluster_state.node_id.clone(),
            dst: to.to_string(),
            body: Body::<P> {
                id: Some(self.seq),
                in_reply_to,
                payload: payload.clone(),
            },
        };

        serde_json::to_writer(&mut self.stdout, &message).context("serializing message")?;

        self.stdout
            .write_all(b"\n")
            .context("appending trailing newline")?;
        self.stdout.flush().context("flushing message to STDOUT")?;

        let seq = self.seq;

        self.seq += 1;

        Ok(seq)
    }

    pub fn fire_and_forget(&mut self, dst: &str, message: &P) -> anyhow::Result<()> {
        // TODO: handle errors?
        _ = self.send(dst, None, message);
        Ok(())
    }

    pub fn rpc_request(
        &mut self,
        dst: &str,
        request: &P,
        timeout: Duration,
        retry: bool,
    ) -> anyhow::Result<usize> {
        let dst = dst;
        let id = self.send(dst, None, request)?;
        let request = Request {
            id,
            dst: dst.to_string(), //TODO: try to do it with reference?
            payload: request.clone(),
            timeout,
            issued_at: Instant::now(),
            retry,
        };

        self.pending_requests.insert(id, request);

        Ok(id)
    }

    pub fn rpc_request_with_retry(
        &mut self,
        dst: &str,
        request: &P,
        retry_after: Duration,
    ) -> anyhow::Result<usize> {
        self.rpc_request(dst, request, retry_after, true)
    }

    pub fn rpc_reply_to(&mut self, message: &Message<P>, reply: &P) -> anyhow::Result<usize> {
        let dst = &message.src;
        let in_reply_to = message.body.id;
        self.send(dst, in_reply_to, reply)
    }

    pub fn rpc_mark_completed(&mut self, message: &Message<P>) {
        if let Some(in_reply_to) = message.body.in_reply_to {
            _ = self.pending_requests.remove(&in_reply_to);
        }
    }

    pub fn rpc_tend(&mut self) -> anyhow::Result<(Vec<Request<P>>, Duration)> {
        let (timedout, future): (Vec<Request<P>>, Vec<Request<P>>) = self
            .pending_requests
            .values()
            .cloned()
            .partition(|r| r.issued_at.elapsed() >= r.timeout);

        for r in &timedout {
            let Some(request) = self.pending_requests.remove(&r.id) else {
                bail!("this shouldn't happen");
            };

            if request.retry {
                self.rpc_request_with_retry(&request.dst, &request.payload, request.timeout)?;
            }
        }

        let sleep = future
            .iter()
            .map(|r| {
                r.timeout
                    .checked_sub(r.issued_at.elapsed())
                    .unwrap_or_default()
            })
            .min()
            .unwrap_or(Duration::MAX);

        Ok((timedout, sleep))
    }
}

pub struct Node<'a, H, P, T>
where
    H: Server<P, T>,
    P: Sized + Serialize + Clone,
{
    pub cluster_state: Arc<ClusterState>,
    pub io: IO<'a, P>,
    pub handler: H,
    in_tx: Sender<Event<P, T>>,
    in_rx: Receiver<Event<P, T>>,
    timers: Timers<P, T>,
}

const TICK_DURATION: Duration = Duration::from_millis(1000);

impl<'a, H, P, T> Node<'a, H, P, T>
where
    H: Server<P, T>,
    P: Send + Serialize + Deserialize<'a> + Send + Clone + 'static,
    T: Send + Clone + Copy + 'static,
{
    pub fn init() -> anyhow::Result<Node<'a, H, P, T>> {
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
            pending_requests: HashMap::<usize, Request<P>>::new(),
        };

        let (in_tx, in_rx) = mpsc::channel();
        let mut timers: Timers<P, T> = Timers::new(in_tx.clone());

        let server = Server::init(&cluster_state, &mut timers)?;

        let node = Node::<H, P, T> {
            cluster_state: cluster_state.clone(),
            io,
            handler: server,
            timers,
            in_tx,
            in_rx,
        };

        let mut init_io = IO::<InitPayload> {
            seq: 0,
            cluster_state: cluster_state.clone(),
            stdout: std::io::stdout().lock(),
            _payload: PhantomData,
            pending_requests: HashMap::<usize, Request<InitPayload>>::new(),
        };

        init_io.rpc_reply_to(&init_msg, &InitPayload::InitOk)?;

        Ok(node)
    }

    pub fn run(&mut self) -> anyhow::Result<()> {
        let stdin_tx = self.in_tx.clone();
        let jh = thread::spawn(move || {
            let stdin = std::io::stdin().lock();
            let in_stream = serde_json::Deserializer::from_reader(stdin).into_iter();
            for msg in in_stream {
                let msg: Message<P> = msg.context("failed to deserialize message from STDIN")?;
                let event: Event<P, T> = Event::Message(msg);

                if stdin_tx.send(event).is_err() {
                    return Ok::<_, anyhow::Error>(());
                }
            }

            if stdin_tx.send(Event::EOF).is_err() {
                return Ok::<_, anyhow::Error>(());
            }

            Ok(())
        });

        let mut tick_timeout = Duration::ZERO;
        let mut last_tick = Instant::now();
        loop {
            let event = self.in_rx.recv_timeout(tick_timeout).unwrap_or(Event::Tick);
            match event {
                Event::Message(message) => {
                    self.handler
                        .on_message(&self.cluster_state, &mut self.io, message)
                        .context("failed processing message")?;
                }
                Event::Timer(timer) => {
                    self.handler
                        .on_timer(&self.cluster_state, &mut self.io, timer)
                        .context("failed processing message")?;
                }
                Event::EOF => break,
                Event::Tick => (),
            }

            let since_last_tick = last_tick.elapsed();

            if since_last_tick >= tick_timeout {
                let (timeouts, to_next_timeout) = self.io.rpc_tend()?;
                tick_timeout = cmp::min(to_next_timeout, TICK_DURATION);
                for r in timeouts {
                    self.handler
                        .on_rpc_timeout(&self.cluster_state, r)
                        .context("failed processing message")?;
                }

                let to_next_timer = self.timers.fire()?;
                tick_timeout = cmp::min(tick_timeout, to_next_timer);
            } else {
                tick_timeout = tick_timeout.checked_sub(since_last_tick).unwrap_or_default();
            }

            last_tick = Instant::now();
        }

        jh.join().expect("STDIN processing panicked")?;

        Ok(())
    }
}

pub trait Server<P, T>
where
    P: Serialize + Clone,
{
    fn init(cluster_state: &ClusterState, timers: &mut Timers<P, T>) -> Result<Self>
    where
        Self: Sized;

    fn on_message(
        &mut self,
        cluster_state: &ClusterState,
        io: &mut IO<P>,
        input: Message<P>,
    ) -> Result<()>
    where
        Self: Sized;

    fn on_timer(&mut self, cluster_state: &ClusterState, io: &mut IO<P>, input: T) -> Result<()>
    where
        Self: Sized;

    fn on_rpc_timeout(&mut self, cluster_state: &ClusterState, timeout: Request<P>) -> Result<()>
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
    Tick,
    EOF,
}

#[derive(Copy, Clone)]
struct TimerRegistration<T> {
    timer: T,
    interval: Duration,
    last_fire: Instant,
}

pub struct Timers<P, T> {
    regs: Vec<TimerRegistration<T>>,
    trigger: Sender<Event<P, T>>,
}

impl<P, T> Timers<P, T>
where
    P: Send + 'static,
    T: Clone + Copy + Send + 'static,
{
    fn new(trigger: Sender<Event<P, T>>) -> Self {
        Timers {
            regs: Vec::new(),
            trigger,
        }
    }

    pub fn register_timer(&mut self, timer: T, interval: Duration) {
        let reg = TimerRegistration {
            timer,
            interval,
            last_fire: Instant::now(),
        };

        self.regs.push(reg);
    }

    fn fire(&mut self) -> anyhow::Result<Duration> {
        let mut sleep = Duration::MAX;
        for reg in &mut self.regs {
            let last_fire = reg.last_fire.elapsed();
            if last_fire >= reg.interval {
                let event: Event<P, T> = Event::<P, T>::Timer(reg.timer);
                if self.trigger.send(event).is_err() {
                    anyhow::bail!("")
                }
                reg.last_fire = Instant::now();
                sleep = cmp::min(sleep, reg.interval);
            } else {
                sleep = reg.interval.checked_sub(last_fire).unwrap_or_default();
            }
        }

        Ok(sleep)
    }
}
