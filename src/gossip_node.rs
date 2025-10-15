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
use clap::Parser;

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

        //     // let endpoint = Endpoint::builder().discovery_n0().bind().await?;
        //     let endpoint = Endpoint::builder()
        //         .secret_key(secret_key)
        //         .add_discovery(static_provider.clone())
        //         .relay_mode(relay_mode.clone())
        //         .bind_addr_v4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, args.bind_port))
        //         .bind().await?;

        let endpoint = Endpoint::builder()
            .secret_key(secret_key)
            .add_discovery(static_provider.clone())
            .relay_mode(RelayMode::Default)
            .bind_addr_v4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, bind_port))
            .bind().await?;

        let gossip = Gossip::builder().spawn(endpoint.clone());

        // if !matches!(relay_mode, RelayMode::Disabled) {
        // if we are expecting a relay, wait until we get a home relay
        // before moving on
        endpoint.online().await;
        // }

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
        println!("> [GossipNode] start subscription: {:?}", node_ids);
        let (sender, _receiver) = self.gossip.subscribe(topic, node_ids).await?.split();
        println!("> [GossipNode] Connected to gossip topic: {}", topic);

        // Spawn task để handle incoming messages từ peers
        // self.spawn_gossip_receiver(topic, receiver).await;

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

        let (_, receiver) = self.gossip.subscribe(topic, node_ids).await?.split();

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

#[derive(Parser, Debug)]
struct Args {
    /// secret key to derive our node id from.
    #[clap(long)]
    nn: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let ingest_secret = SecretKey::generate(&mut rand::rng());
    let ingest_node = Arc::new(GossipNode::new(ingest_secret, 0).await?);
    match args.nn.as_str() {
        "1" => {
            println!("> Starting Ingest Server (VPS 1)");
            // ===== INGEST SERVER (VPS 1) =====
            let ingest_service = MediaService::new(ingest_node.clone());

            // Start ingest
            let _ingest_handle = ingest_service.start_ingest(
                "my-stream".to_string(),
                vec![]
            ).await?;
            println!("> Ingest service started for stream 'my-stream'");
            // Simulate WebTransport/WebRTC receiving frames from client publisher
            tokio::spawn({
                let service = ingest_service.clone();
                println!("> [Ingest] Simulating frame ingestion...");
                async move {
                    for frame_num in 0..100 {
                        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

                        let frame = MediaFrame::new(
                            "my-stream".to_string(),
                            frame_num,
                            vec![42u8; 100]
                        );

                        println!(
                            "> [Ingest] Received frame: stream={}, frame={}",
                            frame.stream_id,
                            frame.frame_num
                        );

                        if let Err(e) = service.send_frame("my-stream", frame).await {
                            eprintln!("Error sending frame: {}", e);
                        }
                    }
                }
            });
        }
        "2" => {
            println!("> Starting Edge Server (VPS 2)");
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
                    for _ in 0..100 {
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
        }
        _ => {
            println!("Please specify --nn 1 for Ingest Server or --nn 2 for Edge Server");
            return Ok(());
        }
    }

    // ===== EDGE SERVER (VPS 2) =====

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
