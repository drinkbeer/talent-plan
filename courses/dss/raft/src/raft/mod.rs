use std::collections::HashSet;
use std::sync::mpsc::{sync_channel, Receiver};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::channel::mpsc::{self, UnboundedReceiver, UnboundedSender};
use futures::channel::oneshot;
use futures::executor::ThreadPool;
use futures::task::SpawnExt;
use futures::{select, FutureExt, StreamExt};
use rand::Rng;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;

/// As each Raft peer becomes aware that successive log entries are committed,
/// the peer should send an `ApplyMsg` to the service (or tester) on the same
/// server, via the `apply_ch` passed to `Raft::new`.
pub enum ApplyMsg {
    Command {
        data: Vec<u8>,
        index: u64,
    },
    // For 2D:
    Snapshot {
        data: Vec<u8>,
        term: u64,
        index: u64,
    },
}

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}

#[derive(Debug)]
pub enum Role {
    Follower,
    Candidate {
        // number of votes received
        votes: HashSet<usize>,
    },
    Leader {
        // for each server, the next index to send to
        next_index: Vec<usize>,
        // for each server, the last index of log that has been sent to
        match_index: Vec<usize>,
    },
}

#[derive(Debug)]
pub struct PersistentState {
    current_term: u64,
    voted_for: Option<usize>,
    log: Vec<(u64, Entry)>,
}

impl PersistentState {
    pub fn new() -> Self {
        Self {
            current_term: 0,
            voted_for: None,
            log: vec![Default::default()], // Default::default() is the dummy entry at index 0, the real log starts at index 1
        }
    }
}

impl Default for PersistentState {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct VolatileState {
    pub commit_index: u64,
    pub last_applied: u64,
}

impl VolatileState {
    pub fn new() -> Self {
        Self {
            commit_index: 0,
            last_applied: 0,
        }
    }
}

impl Default for VolatileState {
    fn default() -> Self {
        Self::new()
    }
}

pub enum Event {
    Heartbeat,
    ResetTimeout,
    Timeout,
    RequestVoteReply(usize, RequestVoteReply),
    AppendEntriesReply(usize, AppendEntriesReply),
}

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: usize,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.

    // States
    p: PersistentState,
    v: VolatileState,
    role: Role,

    // Channels
    apply_tx: UnboundedSender<ApplyMsg>,
    event_tx: Option<UnboundedSender<Event>>,

    // Executor
    executor: ThreadPool,
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        apply_tx: UnboundedSender<ApplyMsg>,
        // event_tx: UnboundedSender<Event>,
    ) -> Raft {
        let raft_state = persister.raft_state();

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            p: PersistentState::new(),
            v: VolatileState::new(),
            role: Role::Follower,
            apply_tx,
            event_tx: None,
            executor: ThreadPool::new().unwrap(),
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        // crate::your_code_here((rf, apply_ch))
        rf.turn_follower();

        rf
    }

    fn turn_follower(&mut self) {
        self.role = Role::Follower;
    }

    fn turn_candidate(&mut self) {
        // Initialize votes to include only self.me
        let mut votes = HashSet::new();
        votes.insert(self.me);
        self.role = Role::Candidate { votes };
    }

    fn turn_leader(&mut self) {
        let next_index = vec![self.p.log.len(); self.peers.len()];
        let match_index = vec![0; self.peers.len()];
        self.role = Role::Leader {
            next_index,
            match_index,
        };
    }

    fn update_term(&mut self, term: u64) {
        if term > self.p.current_term {
            self.p.current_term = term;
            self.p.voted_for = None;
        } else if term < self.p.current_term {
            panic!("term is lower than current term");
        }
    }

    /// save Raft's persistent state to stable storage,
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
    }

    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns Ok(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/lib.rs for more details.
    fn send_request_vote(
        &self,
        server: usize,
        args: RequestVoteArgs,
    ) -> Receiver<Result<RequestVoteReply>> {
        // Your code here if you want the rpc becomes async.
        // Example:
        // ```
        // let peer = &self.peers[server];
        // let peer_clone = peer.clone();
        // let (tx, rx) = channel();
        // peer.spawn(async move {
        //     let res = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
        //     tx.send(res);
        // });
        // rx
        // ```
        let (tx, rx) = sync_channel::<Result<RequestVoteReply>>(1);
        crate::your_code_here((server, args, tx, rx))
    }

    fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let index = 0;
        let term = 0;
        let is_leader = true;
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).

        if is_leader {
            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
    }

    fn cond_install_snapshot(
        &mut self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: &[u8],
    ) -> bool {
        // Your code here (2D).
        crate::your_code_here((last_included_term, last_included_index, snapshot));
    }

    fn snapshot(&mut self, index: u64, snapshot: &[u8]) {
        // Your code here (2D).
        crate::your_code_here((index, snapshot));
    }
}

impl Raft {
    fn append_entries_args(&self, entries: Vec<Entry>) -> AppendEntriesArgs {
        AppendEntriesArgs {
            term: self.p.current_term,
            leader_id: self.me as u64,
            prev_log_index: self.p.log.len() as u64,
            prev_log_term: self.p.log.last().unwrap().0,
            entries,
            leader_commit: self.v.commit_index,
        }
    }

    fn request_vote_args(&self) -> RequestVoteArgs {
        RequestVoteArgs {
            term: self.p.current_term,
            candidate_id: self.me as u64,
            last_log_index: self.p.log.len() as u64 - 1,
            last_log_term: self.p.log.last().unwrap().0,
        }
    }
}

impl Raft {
    fn send_event(&mut self, event: Event) {
        if let Err(e) = self.event_tx.as_ref().unwrap().unbounded_send(event) {
            panic!("failed to send event: {:?}", e);
        }
    }

    fn handle_event(&mut self, event: Event) {
        match event {
            Event::Heartbeat => {
                self.handle_heartbeat();
            }
            Event::ResetTimeout => unreachable!(),
            Event::Timeout => {
                self.handle_timeout();
            }
            Event::RequestVoteReply(from_server, reply) => {
                self.handle_request_vote_reply(from_server, reply);
            }
            Event::AppendEntriesReply(_from_server, reply) => {
                self.handle_append_entries_reply(reply);
            }
        }
    }

    fn handle_heartbeat(&mut self) {
        match self.role {
            Role::Leader { .. } => {
                // send heartbeat to all peers
                for (i, peer) in self.peers.iter().enumerate() {
                    if i == self.me {
                        continue;
                    }
                    let tx = self.event_tx.as_ref().unwrap().clone();
                    let fut = peer.append_entries(&self.append_entries_args(vec![]));

                    self.executor
                        .spawn(async move {
                            if let Ok(reply) = fut.await {
                                tx.unbounded_send(Event::AppendEntriesReply(i, reply))
                                    .unwrap();
                            }
                        })
                        .unwrap();
                }
            }
            _ => {}
        }
    }

    fn handle_timeout(&mut self) {
        match self.role {
            Role::Leader { .. } => {} // no timeout for leader
            Role::Candidate { .. } | Role::Follower => {
                // start a new election
                self.turn_candidate();
                self.update_term(self.p.current_term + 1);
                self.p.voted_for = Some(self.me);
                self.send_event(Event::ResetTimeout);

                // ask peers to vote me as leader
                for (i, peer) in self.peers.iter().enumerate() {
                    if i == self.me {
                        continue;
                    }
                    let tx = self.event_tx.as_ref().unwrap().clone();
                    let fut = peer.request_vote(&self.request_vote_args());

                    self.executor
                        .spawn(async move {
                            if let Ok(reply) = fut.await {
                                tx.unbounded_send(Event::RequestVoteReply(i, reply))
                                    .unwrap();
                            }
                        })
                        .unwrap();
                }
            }
        }
    }

    fn handle_request_vote_request(
        &mut self,
        args: RequestVoteArgs,
    ) -> labrpc::Result<RequestVoteReply> {
        let vote_granted = {
            if args.term < self.p.current_term {
                false
            } else {
                if args.term > self.p.current_term {
                    self.update_term(args.term);
                    self.turn_follower();
                }

                let id = args.candidate_id as usize;
                if self.p.voted_for.map(|v| v == id) != Some(false) {
                    self.p.voted_for = Some(id);
                    true
                } else {
                    false
                }
            }
        };

        Ok(RequestVoteReply {
            term: self.p.current_term,
            vote_granted,
        })
    }

    fn handle_request_vote_reply(&mut self, from_server: usize, reply: RequestVoteReply) {
        if reply.term > self.p.current_term {
            self.update_term(reply.term);
            self.turn_follower();
            return;
        }

        match &mut self.role {
            Role::Candidate { votes } => {
                if reply.vote_granted && reply.term == self.p.current_term {
                    votes.insert(from_server);

                    // check if I am the leader after receiving votes
                    if votes.len() > self.peers.len() / 2 {
                        self.turn_leader();
                        self.handle_heartbeat(); // send heartbeat to all peers immediately, broadcast a new leader
                    }
                }
            }
            _ => {}
        }
    }

    fn handle_append_entries_request(
        &mut self,
        args: AppendEntriesArgs,
    ) -> labrpc::Result<AppendEntriesReply> {
        let success = {
            if args.term < self.p.current_term {
                // the append entries request is stale, reject it
                false
            } else {
                if args.term > self.p.current_term || matches!(self.role, Role::Candidate { .. }) {
                    // this node is stale, a new leader is elected. If it is a candidate, it means election is failed.
                    self.update_term(args.term);
                    self.turn_follower();
                }

                match &mut self.role {
                    Role::Follower => {
                        // todo: log replication
                        self.send_event(Event::ResetTimeout);
                        true
                    }
                    Role::Candidate { .. } => {
                        unreachable!("candidate should turn to follower before");
                    }
                    Role::Leader { .. } => {
                        unreachable!("another leader with the same term should not be elected");
                    }
                }
            }
        };

        Ok(AppendEntriesReply {
            term: self.p.current_term,
            success,
        })
    }

    fn handle_append_entries_reply(&mut self, reply: AppendEntriesReply) {
        if reply.term > self.p.current_term {
            self.update_term(reply.term);
            self.turn_follower();
        }

        match &mut self.role {
            Role::Leader { .. } => {
                // todo: log replication in lab 2B+
            }
            _ => {}
        }
    }
}

impl Raft {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = self.start(&0);
        let _ = self.cond_install_snapshot(0, 0, &[]);
        self.snapshot(0, &[]);
        let _ = self.send_request_vote(0, Default::default());
        self.persist();
        let _ = &self.apply_tx;
        let _ = &self.me;
        let _ = &self.persister;
        let _ = &self.peers;
    }
}

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your code here.
    raft: Arc<Mutex<Raft>>,
    event_loop_tx: UnboundedSender<Event>,
    shutdown_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    executor: ThreadPool,
}

impl Node {
    /// Create a new raft service.
    pub fn new(mut raft: Raft) -> Node {
        // Your code here.
        // crate::your_code_here(raft)
        let (event_loop_tx, event_loop_rx) = mpsc::unbounded();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let executor = ThreadPool::new().unwrap();

        raft.event_tx = Some(event_loop_tx.clone());

        let node = Node {
            raft: Arc::new(Mutex::new(raft)),
            event_loop_tx,
            shutdown_tx: Arc::new(Mutex::new(Some(shutdown_tx))),
            executor,
        };

        node.start_event_loop(event_loop_rx, shutdown_rx);

        node
    }

    fn start_event_loop(
        &self,
        mut event_loop_rx: UnboundedReceiver<Event>,
        mut shutdown_rx: oneshot::Receiver<()>,
    ) {
        let raft = self.raft.clone();
        let event_loop_tx = self.event_loop_tx.clone();

        self.executor
            .spawn(async move {
                let build_rand_timer = || {
                    // fuse() is used to convert the timer into a future that can be Poll::Pending after it's been cancelled
                    // If the timer is cancelled, the future will return Poll::Pending; if not fused, it may cause a panic
                    futures_timer::Delay::new(Duration::from_millis(
                        rand::thread_rng().gen_range(300, 600),
                    ))
                    .fuse()
                };

                let build_hb_timer =
                    || futures_timer::Delay::new(Duration::from_millis(100)).fuse();

                let mut timeout_timer = build_rand_timer();
                let mut hb_timer = build_hb_timer();

                loop {
                    select! {
                        event = event_loop_rx.select_next_some() => {
                            match event {
                                Event::ResetTimeout => {
                                    timeout_timer = build_rand_timer();
                                    // Only reset heartbeat timer if we're the leader
                                    if matches!(raft.lock().unwrap().role, Role::Leader { .. }) {
                                        hb_timer = build_hb_timer();
                                    }
                                },
                                event => {
                                    raft.lock().unwrap().handle_event(event);
                                }
                            }
                        }
                        _ = timeout_timer => {
                            event_loop_tx.unbounded_send(Event::Timeout).unwrap();
                            timeout_timer = build_rand_timer();
                        }
                        _ = hb_timer => {
                            // if matches!(raft.lock().unwrap().role, Role::Leader { .. }) {
                                event_loop_tx.unbounded_send(Event::Heartbeat).unwrap();
                            // }
                            hb_timer = build_hb_timer();
                        }
                        _ = shutdown_rx => break, // shutdown the event loop, drop event_loop_rx
                    }
                }
            })
            .expect("failed to start event loop");
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        // Your code here.
        // Example:
        // self.raft.start(command)
        // crate::your_code_here(command)
        self.raft.lock().unwrap().start(command)
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        // Example:
        // self.raft.term
        self.raft.lock().unwrap().p.current_term
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        matches!(self.raft.lock().unwrap().role, Role::Leader { .. })
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        State {
            term: self.term(),
            is_leader: self.is_leader(),
        }
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        // Your code here, if desired.
        if let Some(shutdown_tx) = self.shutdown_tx.lock().unwrap().take() {
            shutdown_tx.send(()).unwrap();
        }
    }

    /// A service wants to switch to snapshot.
    ///
    /// Only do so if Raft hasn't have more recent info since it communicate
    /// the snapshot on `apply_ch`.
    pub fn cond_install_snapshot(
        &self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: &[u8],
    ) -> bool {
        // Your code here.
        // Example:
        // self.raft.cond_install_snapshot(last_included_term, last_included_index, snapshot)
        crate::your_code_here((last_included_term, last_included_index, snapshot));
    }

    /// The service says it has created a snapshot that has all info up to and
    /// including index. This means the service no longer needs the log through
    /// (and including) that index. Raft should now trim its log as much as
    /// possible.
    pub fn snapshot(&self, index: u64, snapshot: &[u8]) {
        // Your code here.
        // Example:
        // self.raft.snapshot(index, snapshot)
        crate::your_code_here((index, snapshot));
    }
}

#[async_trait::async_trait]
impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn request_vote(&self, args: RequestVoteArgs) -> labrpc::Result<RequestVoteReply> {
        // Your code here (2A, 2B).
        // crate::your_code_here(args)
        let mut raft = self.raft.lock().unwrap();
        raft.handle_request_vote_request(args)
    }

    async fn append_entries(&self, args: AppendEntriesArgs) -> labrpc::Result<AppendEntriesReply> {
        // Your code here (2A, 2B).
        let mut raft = self.raft.lock().unwrap();
        raft.handle_append_entries_request(args)
    }
}
