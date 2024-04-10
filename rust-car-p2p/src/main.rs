const STORAGE_FILE_PATH: &str = "./carinfo.json";

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync + 'static>>;

static KEYS: Lazy<identity::Keypair> = Lazy::new(|| identity::Keypair::generate_ed25519());
static PEER_ID: Lazy<PeerId> = Lazy::new(|| PeerId::from(KEYS.public()));
static TOPIC: Lazy<Topic> = Lazy::new(|| Topic::new("carinfo"));

type Carinfos = Vec<Carinfo>;

#[derive(Debug, Serialize, Deserialize)]
struct Recipe {
    id: usize,
    make: String,
    model: String,
    horsepower: String,
    public: bool,
}

#[derive(Debug, Serialize, Deserialize)]
enum ListMode {
    ALL,
    One(String),
}

#[derive(Debug, Serialize, Deserialize)]
struct ListRequest {
    mode: ListMode,
}

#[derive(Debug, Serialize, Deserialize)]
struct ListResponse {
    mode: ListMode,
    data: Carinfos,
    receiver: String,
}

enum EventType {
    Response(ListResponse),
    Input(String),
}

#[tokio::main]
async fn main() {
    pretty_env_logger::init();

    info!("Peer Id: {}", PEER_ID.clone());
    let (response_sender, mut response_rcv) = mpsc::unbounded_channel();

    let auth_keys = Keypair::<X25519Spec>::new()
        .into_authentic(&KEYS)
        .expect("can create auth keys");
    
    let transp = TokioTcpConfig::new()
    .upgrade(upgrade::Version::V1)
    .authenticate(NoiseConfig::xx(auth_keys).into_authenticated())
    .multiplex(mplex::MplexConfig::new())
    .boxed();

    let mut behaviour = RecipeBehaviour {
        floodsub: Floodsub::new(PEER_ID.clone()),
        mdns: TokioMdns::new().expect("can create mdns"),
        response_sender,
    };

    behaviour.floodsub.subscribe(TOPIC.clone());

    let mut swarm = SwarmBuilder::new(transp, behaviour, PEER_ID.clone())
        .executor(Box::new(|fut| {
            tokio::spawn(fut);
        }))
        .build();
    
    Swarm::listen_on(
        &mut swarm,
        "/ip4/0.0.0.0/tcp/0"
            .parse()
            .expect("can get a local socket"),
    )
    .expect("swarm can be started");

    let mut stdin = tokio::io::BufReader::new(tokio::io::stdin()).lines();

    loop {
        let evt = {
            tokio::select! {
                line = stdin.next_line() => Some(EventType::Input(line.expect("can get line").expect("can read line from stdin"))),
                event = swarm.next() => {
                    info!("Unhandled Swarm Event: {:?}", event);
                    None
                },
                response = response_rcv.recv() => Some(EventType::Response(response.expect("response exists"))),
            }
        };
        if let Some(event) = evt {
            match event {
                EventType::Response(resp) => {
                   ...
                }
                EventType::Input(line) => match line.as_str() {
                    "ls p" => handle_list_peers(&mut swarm).await,
                    cmd if cmd.starts_with("ls r") => handle_list_recipes(cmd, &mut swarm).await,
                    cmd if cmd.starts_with("create r") => handle_create_recipe(cmd).await,
                    cmd if cmd.starts_with("publish r") => handle_publish_recipe(cmd).await,
                    _ => error!("unknown command"),
                },
            }
        }
    }

    async fn handle_list_peers(swarm: &mut Swarm<CarBehavior>) {
        info!("Discovered Peers:");
        let nodes = swarm.mdns.discovered_nodes();
        let mut unique_peers = HashSet::new();
        for peer in nodes {
            unique_peers.insert(peer);
        }
        unique_peers.iter().for_each(|p| info!("{}", p));
    }

    async fn handle_create_Carinfo(cmd: &str) {
        if let Some(rest) = cmd.strip_prefix("create r") {
            let elements: Vec<&str> = rest.split("|").collect();
            if elements.len() < 3 {
                info!("too few arguments - Format: make|model|horsepower");
            } else {
                let make = elements.get(0).expect("make is there");
                let model = elements.get(1).expect("model is there");
                let horsepower = elements.get(2).expect("hp is there");
                if let Err(e) = create_new_Carinfo(make, model, horsepower).await {
                    error!("error creating car: {}", e);
                };
            }
        }
    }
    
    async fn handle_publish_Carinfo(cmd: &str) {
        if let Some(rest) = cmd.strip_prefix("publish r") {
            match rest.trim().parse::<usize>() {
                Ok(id) => {
                    if let Err(e) = publish_Carinfo(id).await {
                        info!("error publishing car with id {}, {}", id, e)
                    } else {
                        info!("Published car with id: {}", id);
                    }
                }
                Err(e) => error!("invalid id: {}, {}", rest.trim(), e),
            };
        }
    }




    async fn create_new_Carinfo(make: &str, model: &str, horsepower: &str) -> Result<()> {
        let mut local_Carinfo = read_local_Carinfo().await?;
        let new_id = match local_Carinfo.iter().max_by_key(|r| r.id) {
            Some(v) => v.id + 1,
            None => 0,
        };
        local_Carinfo.push(Carinfo {
            id: new_id,
            make: make.to_owned(),
            model: model.to_owned(),
            horsepower: horsepower.to_owned(),
            public: false,
        });
        write_local_Carinfo(&local_Carinfo).await?;
    
        info!("Created New Car:");
        info!("Make: {}", make);
        info!("Model: {}", model);
        info!("Horsepower:: {}", horsepower);
    
        Ok(())
    }
    
    async fn publish_Carinfo(id: usize) -> Result<()> {
        let mut local_Carinfo = read_local_Carinfo().await?;
        local_Carinfo
            .iter_mut()
            .filter(|r| r.id == id)
            .for_each(|r| r.public = true);
        write_local_Carinfo(&local_Carinfo).await?;
        Ok(())
    }
    
    async fn read_local_Carinfo() -> Result<Carinfos> {
        let content = fs::read(STORAGE_FILE_PATH).await?;
        let result = serde_json::from_slice(&content)?;
        Ok(result)
    }
    
    async fn write_local_Carinfo(carinfo: &Carinfos) -> Result<()> {
        let json = serde_json::to_string(&carinfo)?;
        fs::write(STORAGE_FILE_PATH, &json).await?;
        Ok(())
    }

    async fn handle_list_Carinfo(cmd: &str, swarm: &mut Swarm<CarBehaviour>) {
        let rest = cmd.strip_prefix("ls r ");
        match rest {
            Some("all") => {
                let req = ListRequest {
                    mode: ListMode::ALL,
                };
                let json = serde_json::to_string(&req).expect("can jsonify request");
                swarm.floodsub.publish(TOPIC.clone(), json.as_bytes());
            }
            Some(carinfo_peer_id) => {
                let req = ListRequest {
                    mode: ListMode::One(carinfo_peer_id.to_owned()),
                };
                let json = serde_json::to_string(&req).expect("can jsonify request");
                swarm.floodsub.publish(TOPIC.clone(), json.as_bytes());
            }
            None => {
                match read_local_Carinfo().await {
                    Ok(v) => {
                        info!("Local Car info ({})", v.len());
                        v.iter().for_each(|r| info!("{:?}", r));
                    }
                    Err(e) => error!("error fetching local car info: {}", e),
                };
            }
        };
    }


    #[derive(NetworkBehaviour)]
    struct RecipeBehaviour {
        floodsub: Floodsub,
        mdns: TokioMdns,
        #[behaviour(ignore)]
        response_sender: mpsc::UnboundedSender<ListResponse>,
    }

    impl NetworkBehaviourEventProcess<MdnsEvent> for CarBehaviour {
        fn inject_event(&mut self, event: MdnsEvent) {
            match event {
                MdnsEvent::Discovered(discovered_list) => {
                    for (peer, _addr) in discovered_list {
                        self.floodsub.add_node_to_partial_view(peer);
                    }
                }
                MdnsEvent::Expired(expired_list) => {
                    for (peer, _addr) in expired_list {
                        if !self.mdns.has_node(&peer) {
                            self.floodsub.remove_node_from_partial_view(&peer);
                        }
                    }
                }
            }
        }
    }




    impl NetworkBehaviourEventProcess<FloodsubEvent> for CarBehaviour {
        fn inject_event(&mut self, event: FloodsubEvent) {
            match event {
                FloodsubEvent::Message(msg) => {
                    if let Ok(resp) = serde_json::from_slice::<ListResponse>(&msg.data) {
                        if resp.receiver == PEER_ID.to_string() {
                            info!("Response from {}:", msg.source);
                            resp.data.iter().for_each(|r| info!("{:?}", r));
                        }
                    } else if let Ok(req) = serde_json::from_slice::<ListRequest>(&msg.data) {
                        match req.mode {
                            ListMode::ALL => {
                                info!("Received ALL req: {:?} from {:?}", req, msg.source);
                                respond_with_public_Carinfo(
                                    self.response_sender.clone(),
                                    msg.source.to_string(),
                                );
                            }
                            ListMode::One(ref peer_id) => {
                                if peer_id == &PEER_ID.to_string() {
                                    info!("Received req: {:?} from {:?}", req, msg.source);
                                    respond_with_public_Carinfo(
                                        self.response_sender.clone(),
                                        msg.source.to_string(),
                                    );
                                }
                            }
                        }
                    }
                }
                _ => (),
            }
        }
    }


    fn respond_with_public_Carinfo(sender: mpsc::UnboundedSender<ListResponse>, receiver: String) {
        tokio::spawn(async move {
            match read_local_carinfo().await {
                Ok(carinfo) => {
                    let resp = ListResponse {
                        mode: ListMode::ALL,
                        receiver,
                        data: carinfo.into_iter().filter(|r| r.public).collect(),
                    };
                    if let Err(e) = sender.send(resp) {
                        error!("error sending response via channel, {}", e);
                    }
                }
                Err(e) => error!("error fetching local car info to answer ALL request, {}", e),
            }
        });
    }

    EventType::Response(resp) => {
        let json = serde_json::to_string(&resp).expect("can jsonify response");
        swarm.floodsub.publish(TOPIC.clone(), json.as_bytes());
    }
}
