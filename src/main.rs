// use std::{ collections::HashMap, fmt, net::{ Ipv4Addr, SocketAddrV4 }, str::FromStr };
// use anyhow::Result;
// use clap::Parser;
// use futures_lite::StreamExt;
// use iroh::{
//     discovery::static_provider::StaticProvider,
//     protocol::Router,
//     Endpoint,
//     NodeAddr,
//     NodeId,
//     RelayMode,
//     RelayUrl,
//     SecretKey,
// };
// use iroh_gossip::{ api::{ Event, GossipReceiver }, net::Gossip, proto::TopicId };
// use serde::{ Deserialize, Serialize };

// use iroh_gossip::net::GOSSIP_ALPN;

// /// Chat over iroh-gossip
// ///
// /// This broadcasts unsigned messages over iroh-gossip.
// ///
// /// By default a new node id is created when starting the example.
// ///
// /// By default, we use the default n0 discovery services to dial by `NodeId`.
// #[derive(Parser, Debug)]
// struct Args {
//     /// secret key to derive our node id from.
//     #[clap(long)]
//     secret_key: Option<String>,
//     /// Set a custom relay server. By default, the relay server hosted by n0 will be used.
//     #[clap(short, long)]
//     relay: Option<RelayUrl>,
//     /// Disable relay completely.
//     #[clap(long)]
//     no_relay: bool,
//     /// Set your nickname.
//     #[clap(short, long)]
//     name: Option<String>,
//     /// Set the bind port for our socket. By default, a random port will be used.
//     #[clap(short, long, default_value = "0")]
//     bind_port: u16,
//     #[clap(subcommand)]
//     command: Command,
// }

// #[derive(Parser, Debug)]
// enum Command {
//     /// Open a chat room for a topic and print a ticket for others to join.
//     Open,
//     /// Join a chat room from a ticket.
//     Join {
//         /// The ticket, as base32 string.
//         ticket: String,
//     },
// }

// #[tokio::main]
// async fn main() -> Result<()> {
//     let args = Args::parse();

//     // parse the cli command
//     let (topic, nodes) = match &args.command {
//         Command::Open => {
//             let topic = TopicId::from_bytes(rand::random());
//             println!("> opening chat room for topic {topic}");
//             (topic, vec![])
//         }
//         Command::Join { ticket } => {
//             let Ticket { topic, nodes } = Ticket::from_str(ticket)?;
//             println!("> joining chat room for topic {topic}");
//             (topic, nodes)
//         }
//     };

//     // parse or generate our secret key
//     let secret_key = match args.secret_key {
//         None => SecretKey::generate(&mut rand::rng()),
//         Some(key) => key.parse()?,
//     };
//     println!("> our secret key: {}", data_encoding::HEXLOWER.encode(&secret_key.to_bytes()));

//     // configure our relay map
//     let relay_mode = match (args.no_relay, args.relay) {
//         (false, None) => RelayMode::Default,
//         (false, Some(url)) => RelayMode::Custom(url.into()),
//         (true, None) => RelayMode::Disabled,
//         (true, Some(_)) => { unreachable!() }
//     };
//     println!("> using relay servers: {}", fmt_relay_mode(&relay_mode));

//     // create a static provider to pass in node addresses to
//     let static_provider = StaticProvider::new();

//     // let endpoint = Endpoint::builder().discovery_n0().bind().await?;
//     let endpoint = Endpoint::builder()
//         .secret_key(secret_key)
//         .add_discovery(static_provider.clone())
//         .relay_mode(relay_mode.clone())
//         .bind_addr_v4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, args.bind_port))
//         .bind().await?;

//     println!("> our node id: {}", endpoint.node_id());
//     let gossip = Gossip::builder().spawn(endpoint.clone());

//     if !matches!(relay_mode, RelayMode::Disabled) {
//         // if we are expecting a relay, wait until we get a home relay
//         // before moving on
//         endpoint.online().await;
//     }

//     // print a ticket that includes our own node id and endpoint addresses
//     let ticket = {
//         let me = endpoint.node_addr();
//         let nodes = nodes.iter().cloned().chain([me]).collect();
//         Ticket { topic, nodes }
//     };
//     println!("> ticket to join us: {ticket}");

//     let router = Router::builder(endpoint.clone()).accept(GOSSIP_ALPN, gossip.clone()).spawn();

//     // join the gossip topic by connecting to known nodes, if any
//     let node_ids = nodes
//         .iter()
//         .map(|p| p.node_id)
//         .collect();
//     if nodes.is_empty() {
//         println!("> waiting for nodes to join us...");
//     } else {
//         println!("> trying to connect to {} peers...", nodes.len());
//         // add the peer addrs from the ticket to our endpoint's addressbook so that they can be dialed
//         for node in nodes.into_iter() {
//             static_provider.add_node_info(node);
//         }
//     }
//     let (sender, receiver) = gossip.subscribe_and_join(topic, node_ids).await?.split();
//     println!("> connected!");

//     // broadcast our name, if set
//     if let Some(name) = args.name {
//         let message = Message::new(MessageBody::AboutMe {
//             from: endpoint.node_id(),
//             name,
//         });
//         sender.broadcast(message.to_vec().into()).await?;
//     }

//     // subscribe and print loop
//     tokio::spawn(subscribe_loop(receiver));

//     // spawn an input thread that reads stdin
//     // create a multi-provider, single-consumer channel
//     let (line_tx, mut line_rx) = tokio::sync::mpsc::channel(1);
//     // and pass the `sender` portion to the `input_loop`
//     std::thread::spawn(move || input_loop(line_tx));

//     // broadcast each line we type
//     println!("> type a message and hit enter to broadcast...");
//     // listen for lines that we have typed to be sent from `stdin`
//     while let Some(text) = line_rx.recv().await {
//         // create a message from the text
//         let message = Message::new(MessageBody::Message {
//             from: endpoint.node_id(),
//             text: text.clone(),
//         });
//         // broadcast the encoded message
//         sender.broadcast(message.to_vec().into()).await?;
//         // print to ourselves the text that we sent
//         println!("> sent: {text}");
//     }
//     router.shutdown().await?;

//     Ok(())
// }

// #[derive(Debug, Serialize, Deserialize)]
// struct Message {
//     body: MessageBody,
//     nonce: [u8; 16],
// }

// #[derive(Debug, Serialize, Deserialize)]
// enum MessageBody {
//     AboutMe {
//         from: NodeId,
//         name: String,
//     },
//     Message {
//         from: NodeId,
//         text: String,
//     },
// }

// impl Message {
//     fn from_bytes(bytes: &[u8]) -> Result<Self> {
//         serde_json::from_slice(bytes).map_err(Into::into)
//     }

//     pub fn new(body: MessageBody) -> Self {
//         Self {
//             body,
//             nonce: rand::random(),
//         }
//     }

//     pub fn to_vec(&self) -> Vec<u8> {
//         serde_json::to_vec(self).expect("serde_json::to_vec is infallible")
//     }
// }

// /// Handle incoming events
// async fn subscribe_loop(mut receiver: GossipReceiver) -> Result<()> {
//     // keep track of the mapping between `NodeId`s and names
//     let mut names = HashMap::new();
//     // iterate over all events
//     while let Some(event) = receiver.try_next().await? {
//         // if the Event is a `GossipEvent::Received`, let's deserialize the message:
//         if let Event::Received(msg) = event {
//             // deserialize the message and match on the
//             // message type:
//             match Message::from_bytes(&msg.content)?.body {
//                 MessageBody::AboutMe { from, name } => {
//                     // if it's an `AboutMe` message
//                     // add an entry into the map
//                     // and print the name
//                     names.insert(from, name.clone());
//                     println!("> {} is now known as {}", from.fmt_short(), name);
//                 }
//                 MessageBody::Message { from, text } => {
//                     // if it's a `Message` message,
//                     // get the name from the map
//                     // and print the message
//                     let name = names.get(&from).map_or_else(|| from.to_string(), String::to_string);
//                     println!("{}: {}", name, text);
//                 }
//             }
//         }
//     }
//     Ok(())
// }

// /// Read input from stdin
// fn input_loop(line_tx: tokio::sync::mpsc::Sender<String>) -> Result<()> {
//     // create a new string buffer
//     let mut buffer = String::new();
//     // get a handle on `Stdin`
//     let stdin = std::io::stdin();
//     loop {
//         // loop through reading from the buffer...
//         stdin.read_line(&mut buffer)?;
//         // and then sending over the channel
//         line_tx.blocking_send(buffer.clone())?;
//         // clear the buffer after we've sent the content
//         buffer.clear();
//     }
// }

// #[derive(Debug, Serialize, Deserialize)]
// struct Ticket {
//     topic: TopicId,
//     nodes: Vec<NodeAddr>,
// }

// impl Ticket {
//     /// Deserialize from a slice of bytes to a Ticket.
//     fn from_bytes(bytes: &[u8]) -> Result<Self> {
//         serde_json::from_slice(bytes).map_err(Into::into)
//     }

//     /// Serialize from a `Ticket` to a `Vec` of bytes.
//     pub fn to_bytes(&self) -> Vec<u8> {
//         serde_json::to_vec(self).expect("serde_json::to_vec is infallible")
//     }
// }

// // The `Display` trait allows us to use the `to_string`
// // method on `Ticket`.
// impl fmt::Display for Ticket {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         let mut text = data_encoding::BASE32_NOPAD.encode(&self.to_bytes()[..]);
//         text.make_ascii_lowercase();
//         write!(f, "{}", text)
//     }
// }

// // The `FromStr` trait allows us to turn a `str` into
// // a `Ticket`
// impl FromStr for Ticket {
//     type Err = anyhow::Error;
//     fn from_str(s: &str) -> Result<Self, Self::Err> {
//         let bytes = data_encoding::BASE32_NOPAD.decode(s.to_ascii_uppercase().as_bytes())?;
//         Self::from_bytes(&bytes)
//     }
// }

// fn fmt_relay_mode(relay_mode: &RelayMode) -> String {
//     match relay_mode {
//         RelayMode::Disabled => "None".to_string(),
//         RelayMode::Default => "Default Relay (production) servers".to_string(),
//         RelayMode::Staging => "Default Relay (staging) servers".to_string(),
//         RelayMode::Custom(map) =>
//             map
//                 .urls()
//                 .map(|url| url.to_string())
//                 .collect::<Vec<_>>()
//                 .join(", "),
//     }
// }

use std::{ collections::HashMap, net::{ Ipv4Addr, SocketAddrV4 }, sync::Arc };
use anyhow::Result;
use futures_lite::StreamExt;
use iroh::{
    discovery::static_provider::StaticProvider,
    protocol::Router,
    Endpoint,
    NodeAddr,
    NodeId,
    RelayMode,
    SecretKey,
};
use iroh_gossip::{ api::{ Event, GossipReceiver, GossipSender }, net::Gossip, proto::TopicId };
use serde::{ Deserialize, Serialize };
use tokio::sync::{ mpsc, RwLock };
use async_broadcast::{
    broadcast,
    InactiveReceiver,
    Receiver as BroadcastReceiver,
    Sender as BroadcastSender,
};

// ============================================================================
// MEDIA FRAME
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MediaFrame {
    pub stream_id: String,
    pub frame_num: u64,
    pub timestamp: u64,
    pub data: Vec<u8>,
    pub nonce: [u8; 16],
}

impl MediaFrame {
    pub fn new(stream_id: String, frame_num: u64, data: Vec<u8>) -> Self {
        Self {
            stream_id,
            frame_num,
            timestamp: std::time::SystemTime
                ::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            data,
            nonce: rand::random(),
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes).map_err(Into::into)
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("serde_json::to_vec is infallible")
    }
}

// ============================================================================
// GOSSIP NODE - Core component
// ============================================================================

pub struct GossipNode {
    endpoint: Endpoint,
    gossip: Gossip,
    stream_topics: Arc<RwLock<HashMap<String, TopicId>>>,
    active_subscriptions: Arc<RwLock<HashMap<TopicId, InactiveReceiver<MediaFrame>>>>,
    static_provider: StaticProvider,
}

impl GossipNode {
    pub async fn new(secret_key: SecretKey, bind_port: u16) -> Result<Self> {
        let static_provider = StaticProvider::new();

        let endpoint = Endpoint::builder()
            .secret_key(secret_key)
            .add_discovery(static_provider.clone())
            .relay_mode(RelayMode::Default)
            .bind_addr_v4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, bind_port))
            .bind().await?;

        let gossip = Gossip::builder().spawn(endpoint.clone());

        let _router = Router::builder(endpoint.clone())
            .accept(iroh_gossip::net::GOSSIP_ALPN, gossip.clone())
            .spawn();

        println!("> [GossipNode] Node ID: {}", endpoint.node_id());

        Ok(Self {
            endpoint,
            gossip,
            stream_topics: Arc::new(RwLock::new(HashMap::new())),
            active_subscriptions: Arc::new(RwLock::new(HashMap::new())),
            static_provider,
        })
    }

    pub fn node_id(&self) -> NodeId {
        self.endpoint.node_id()
    }

    pub fn node_addr(&self) -> NodeAddr {
        self.endpoint.node_addr()
    }

    /// Get or create topic từ stream_id
    async fn get_or_create_topic(&self, stream_id: &str) -> TopicId {
        let mut map = self.stream_topics.write().await;
        if let Some(topic) = map.get(stream_id) {
            *topic
        } else {
            let hash = blake3::hash(stream_id.as_bytes());
            let topic = TopicId::from_bytes(hash.as_bytes()[..32].try_into().unwrap());
            map.insert(stream_id.to_string(), topic);
            println!("> [GossipNode] Created topic for stream: {} -> {}", stream_id, topic);
            topic
        }
    }

    /// Publisher: Join topic và nhận sender
    pub async fn join_as_publisher(
        &self,
        stream_id: &str,
        peer_nodes: Vec<NodeAddr>
    ) -> Result<GossipSender> {
        let topic = self.get_or_create_topic(stream_id).await;

        // Add peers to discovery
        for node in peer_nodes.iter() {
            self.static_provider.add_node_info(node.clone());
        }

        let node_ids = peer_nodes
            .iter()
            .map(|p| p.node_id)
            .collect();

        let (sender, receiver) = self.gossip.subscribe_and_join(topic, node_ids).await?.split();

        // Spawn task để handle incoming messages từ peers
        self.spawn_gossip_receiver(topic, receiver).await;

        println!("> [GossipNode] Publisher joined topic: {}", topic);
        Ok(sender)
    }

    /// Subscriber: Subscribe vào topic và nhận broadcast channel
    pub async fn subscribe(
        &self,
        stream_id: &str,
        publisher_node: NodeAddr
    ) -> Result<BroadcastReceiver<MediaFrame>> {
        let topic = self.get_or_create_topic(stream_id).await;

        // Check if already subscribed
        {
            let subs = self.active_subscriptions.read().await;
            if let Some(inactive_receiver) = subs.get(&topic) {
                return Ok(inactive_receiver.activate_cloned());
            }
        }

        // Add publisher to discovery
        self.static_provider.add_node_info(publisher_node.clone());

        let node_ids = vec![publisher_node.node_id];

        let (_, receiver) = self.gossip.subscribe_and_join(topic, node_ids).await?.split();

        // Create broadcast channel
        let (tx, rx) = broadcast::<MediaFrame>(100);

        // Register subscription
        {
            let mut subs = self.active_subscriptions.write().await;
            subs.insert(topic, rx.clone().deactivate());
        }

        let tx_clone = tx.clone();
        tokio::spawn(async move {
            Self::gossip_receiver_loop(receiver, tx_clone).await;
        });

        println!("> [GossipNode] Subscribed to topic: {}", topic);
        Ok(rx)
    }

    /// Publish frame to gossip cluster
    pub async fn publish_frame(&self, sender: &GossipSender, frame: &MediaFrame) -> Result<()> {
        sender.broadcast(frame.to_bytes().into()).await?;
        Ok(())
    }

    // spawn task to handle incoming gossip messages
    async fn spawn_gossip_receiver(&self, topic: TopicId, mut receiver: GossipReceiver) {
        tokio::spawn(async move {
            while let Ok(Some(event)) = receiver.try_next().await {
                if let Event::Received(msg) = event {
                    if let Ok(frame) = MediaFrame::from_bytes(&msg.content) {
                        println!(
                            "> [GossipNode] Gossip received: stream={}, frame={}",
                            frame.stream_id,
                            frame.frame_num
                        );
                    }
                }
            }
        });
    }

    /// handle received gossip messages and broadcast to subscribers
    async fn gossip_receiver_loop(mut receiver: GossipReceiver, tx: BroadcastSender<MediaFrame>) {
        while let Ok(Some(event)) = receiver.try_next().await {
            if let Event::Received(msg) = event {
                if let Ok(frame) = MediaFrame::from_bytes(&msg.content) {
                    let _ = tx.try_broadcast(frame);
                }
            }
        }
    }
}

// ============================================================================
// MEDIA SERVICE - Client-facing service
// ============================================================================

pub struct MediaService {
    gossip_node: Arc<GossipNode>,
    active_streams: Arc<RwLock<HashMap<String, StreamHandle>>>,
}

pub struct StreamHandle {
    pub sender: Option<GossipSender>,
    pub receiver: Option<BroadcastReceiver<MediaFrame>>,
}

impl MediaService {
    pub fn new(gossip_node: Arc<GossipNode>) -> Self {
        Self {
            gossip_node,
            active_streams: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Client upload stream (from WebTransport/WebRTC)
    pub async fn start_ingest(
        &self,
        stream_id: String,
        peer_nodes: Vec<NodeAddr>
    ) -> Result<IngestHandle> {
        let sender = self.gossip_node.join_as_publisher(&stream_id, peer_nodes).await?;

        let (_tx, rx) = mpsc::channel(10);

        {
            let mut streams = self.active_streams.write().await;
            streams.insert(stream_id.clone(), StreamHandle {
                sender: Some(sender.clone()),
                receiver: None,
            });
        }

        println!("> [MediaService] Ingest started: {}", stream_id);

        Ok(IngestHandle {
            stream_id,
            sender,
            frame_rx: rx,
        })
    }

    /// Publisher send frame
    pub async fn send_frame(&self, stream_id: &str, frame: MediaFrame) -> Result<()> {
        let streams = self.active_streams.read().await;
        if let Some(handle) = streams.get(stream_id) {
            if let Some(sender) = &handle.sender {
                self.gossip_node.publish_frame(sender, &frame).await?;
                println!("> [MediaService] Frame sent: {}", stream_id);
            }
        }
        Ok(())
    }

    /// Client subscribe stream
    pub async fn subscribe(
        &self,
        stream_id: String,
        publisher_node: NodeAddr
    ) -> Result<SubscribeHandle> {
        let rx = self.gossip_node.subscribe(&stream_id, publisher_node).await?;

        println!("> [MediaService] Subscribe started: {}", stream_id);

        Ok(SubscribeHandle { stream_id, rx })
    }

    /// Get frame từ subscription
    pub async fn get_frame(&mut self, handle: &mut SubscribeHandle) -> Option<MediaFrame> {
        handle.rx.recv().await.ok()
    }
}

// ============================================================================
// HANDLES for client interaction
// ============================================================================

pub struct IngestHandle {
    pub stream_id: String,
    pub sender: GossipSender,
    pub frame_rx: mpsc::Receiver<MediaFrame>,
}

pub struct SubscribeHandle {
    pub stream_id: String,
    pub rx: BroadcastReceiver<MediaFrame>,
}

// ============================================================================
// EXAMPLE USAGE
// ============================================================================

#[tokio::main]
async fn main() -> Result<()> {
    // ===== INGEST SERVER (VPS 1) =====
    let ingest_secret = SecretKey::generate(&mut rand::rng());
    let ingest_node = Arc::new(GossipNode::new(ingest_secret, 11111).await?);
    let ingest_service = MediaService::new(ingest_node.clone());

    // Start ingest
    let _ingest_handle = ingest_service.start_ingest("my-stream".to_string(), vec![]).await?;

    // Simulate WebTransport/WebRTC receiving frames from client publisher
    tokio::spawn({
        let service = ingest_service.clone();
        async move {
            for frame_num in 0..10 {
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

                let frame = MediaFrame::new("my-stream".to_string(), frame_num, vec![42u8; 100]);

                if let Err(e) = service.send_frame("my-stream", frame).await {
                    eprintln!("Error sending frame: {}", e);
                }
            }
        }
    });

    // ===== EDGE SERVER (VPS 2) =====
    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

    let edge_secret = SecretKey::generate(&mut rand::rng());
    let edge_node = Arc::new(GossipNode::new(edge_secret, 22222).await?);
    let edge_service = MediaService::new(edge_node.clone());

    // Subscribe
    let sub_handle = edge_service.subscribe(
        "my-stream".to_string(),
        ingest_node.node_addr()
    ).await?;

    // Simulate client receiving frames via WebTransport/WebRTC
    tokio::spawn({
        let mut handle = sub_handle;
        async move {
            for _ in 0..10 {
                match handle.rx.recv().await {
                    Ok(frame) => {
                        println!(
                            "> [Client] Received: stream={}, frame={}, size={} bytes",
                            frame.stream_id,
                            frame.frame_num,
                            frame.data.len()
                        );
                    }
                    Err(e) => {
                        eprintln!("> [Client] Error: {}", e);
                        break;
                    }
                }
            }
        }
    });

    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    Ok(())
}

// ============================================================================
// HELPER TRAITS (for cloning services)
// ============================================================================

impl Clone for MediaService {
    fn clone(&self) -> Self {
        Self {
            gossip_node: self.gossip_node.clone(),
            active_streams: self.active_streams.clone(),
        }
    }
}
