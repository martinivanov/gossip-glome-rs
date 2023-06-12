use gossip_glomers_rs::{ClusterState, Message, Node, Server, Timers, IO};
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    time::Duration, iter,
};

use anyhow::{bail, Result};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
    Broadcast {
        message: usize,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: Vec<usize>,
    },
    Gossip {
        messages: Vec<usize>,
    },
}

#[derive(Clone, Copy, Debug)]
enum Timer {
    Gossip,
}

fn main() -> anyhow::Result<()> {
    let mut node = Node::<BroadcastServer, Payload, Timer>::init()?;
    node.run()
}

struct BroadcastServer {
    messages: HashSet<usize>,
    seen: HashMap<String, HashSet<usize>>,
    neighbours: Vec<String>,
}

impl Server<Payload, Timer> for BroadcastServer {
    fn init(
        cluster_state: &ClusterState,
        timers: &mut Timers<Payload, Timer>,
    ) -> Result<BroadcastServer> {
        //timers.register_timer(Timer::Gossip, Duration::from_millis(250));

        let seen = cluster_state
            .node_ids
            .iter()
            .map(|n| (n.to_string(), HashSet::new()))
            .collect();



        let mut nodes: Vec<String> = (0..cluster_state.node_ids.len()).map(|n| format!("n{}", n)).collect();
        let mut topology: HashMap<String, Vec<String>> = nodes.iter().map(|n| (n.clone(), Vec::new())).collect();

        let root = nodes.remove(0);
        for c in nodes {
            let neighours = topology.get_mut(&c).unwrap();
            neighours.push(root.clone());
            let root_neigbours = topology.get_mut(&root).unwrap();
            root_neigbours.push(c.clone());
        }
        
        //let root1 = nodes.remove(0);
        //let root2 = nodes.remove(0);
        //let mid = nodes.len() / 2;

        //let (children1, children2) = nodes.split_at(mid);
        //for c in children1 {
        //    let neighours = topology.get_mut(c).unwrap();
        //    neighours.push(root1.clone());
        //    neighours.push(root2.clone());
        //    let root1_neigbours = topology.get_mut(&root1).unwrap();
        //    root1_neigbours.push(c.clone());
        //    let root2_neigbours = topology.get_mut(&root2).unwrap();
        //    root2_neigbours.push(c.clone());
        //}

        //for c in children2 {
        //    let neighours = topology.get_mut(c).unwrap();
        //    neighours.push(root1.clone());
        //    neighours.push(root2.clone());
        //    let root1_neigbours = topology.get_mut(&root1).unwrap();
        //    root1_neigbours.push(c.clone());
        //    let root2_neigbours = topology.get_mut(&root2).unwrap();
        //    root2_neigbours.push(c.clone());
        //}

        //eprintln!("Topology: {:?}", topology);

        let neighbours = topology[&cluster_state.node_id].clone();
        eprintln!("Discovered neighbours: {:?}", &neighbours);

        let server = BroadcastServer {
            messages: HashSet::<usize>::new(),
            seen,
            neighbours,
        };


        Ok(server)
    }

    fn on_message(
        &mut self,
        cluster_state: &ClusterState,
        io: &mut IO<Payload>,
        input: Message<Payload>,
    ) -> Result<()> {
        let payload = &input.body.payload;
        match payload {
            Payload::Topology { topology } => {
                //let neighbours = topology
                //    .get(&cluster_state.node_id)
                //    .unwrap()
                //    .iter()
                //    .cloned();

                //self.neighbours.extend(neighbours);
                //eprintln!("Discovered neighbours: {:?}", &self.neighbours);

                let reply = Payload::TopologyOk;
                io.rpc_reply_to(&input, &reply)?;
            }
            Payload::TopologyOk => bail!("unexpected topology_ok message"),
            Payload::Broadcast { message } => {
                if self.messages.insert(message.clone()) {
                    eprintln!("Sending message {} to all our neighbours: {:?}", message, self.neighbours);
                    for n in &self.neighbours {
                        if n == &input.src {
                            eprintln!("Skipping {} for message {}", n, message);
                            continue;
                        }

                        let broadcast = Payload::Broadcast {
                            message: message.clone(),
                        };

                        _ = io.rpc_request_with_retry(&n, &broadcast, Duration::from_millis(400))?;
                    }
                } else {
                    eprintln!("Skipping message {} from {} as we already have it", message, input.src);
                }

                let reply = Payload::BroadcastOk;
                io.rpc_reply_to(&input, &reply)?;
            }
            Payload::BroadcastOk => {
                io.rpc_mark_completed(&input);
            }
            Payload::Read => {
                let values = self.messages.to_owned();
                let reply = Payload::ReadOk {
                    messages: values.into_iter().collect(),
                };
                io.rpc_reply_to(&input, &reply)?;
            }
            Payload::ReadOk { .. } => bail!("unexpected read_ok message"),
            Payload::Gossip { messages } => {
                let new = messages
                    .iter()
                    .copied()
                    .filter(|&m| self.messages.insert(m))
                    .map(|m| m.clone());

                self.seen
                    .get_mut(&input.src)
                    .expect("got gossip from unknown node")
                    .extend(new);
            }
        };

        Ok(())
    }

    fn on_timer(
        &mut self,
        cluster_state: &ClusterState,
        io: &mut IO<Payload>,
        input: Timer,
    ) -> Result<()>
    where
        Self: Sized,
    {
        match input {
            Timer::Gossip => {
                let nodes: Vec<&String> = cluster_state.node_ids.choose_multiple(&mut rand::thread_rng(), 5).collect();
                for n in &nodes {
                    //let dst_seen = &self.seen[n];
                    //let to_send: Vec<usize> = self.messages.difference(dst_seen).copied().collect();
                    let to_send: Vec<usize> = self.messages.iter().copied().collect();

                    if !to_send.is_empty() {
                        let gossip = Payload::Gossip { messages: to_send };
                        io.fire_and_forget(n, &gossip)?;
                    }
                }
            }
        }

        Ok(())
    }

    fn on_rpc_timeout(
        &mut self,
        _: &ClusterState,
        timeout: gossip_glomers_rs::Request<Payload>,
    ) -> Result<()>
    where
        Self: Sized,
    {
        eprintln!("Timeout: {:?}", timeout);

        Ok(())
    }
}
