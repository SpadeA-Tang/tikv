// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::hash_map::Entry as MapEntry,
    result,
    sync::{Arc, Mutex, RwLock},
    thread::{self},
    time::Duration,
};

use collections::{HashMap, HashSet};
use encryption_export::DataKeyManager;
use engine_rocks::{RocksDbVector, RocksEngine, RocksSnapshot};
use engine_test::raft::RaftTestEngine;
use engine_traits::{
    Iterable, KvEngine, MiscExt, OpenOptions, Peekable, RaftEngine, RaftEngineReadOnly,
    RaftLogBatch, ReadOptions, SyncMutable, TabletFactory, CF_DEFAULT,
};
use file_system::IoRateLimiter;
use futures::{compat::Future01CompatExt, executor::block_on, select, FutureExt};
use keys::{data_key, Prefix};
use kvproto::{
    errorpb::Error as PbError,
    kvrpcpb::ApiVersion,
    metapb::{self, Buckets, PeerRole, RegionEpoch},
    raft_cmdpb::{
        AdminCmdType, CmdType, RaftCmdRequest, RaftCmdResponse, RegionDetailResponse, Request,
        Response, StatusCmdType,
    },
    raft_serverpb::{RaftApplyState, RegionLocalState, StoreIdent},
};
use pd_client::PdClient;
use raftstore::{
    store::{
        cmd_resp, initial_region, util::check_key_in_region, Bucket, BucketRange, Callback,
        RegionSnapshot, WriteResponse, INIT_EPOCH_CONF_VER, INIT_EPOCH_VER,
    },
    Error, Result,
};
use raftstore_v2::{
    create_store_batch_system,
    router::{PeerMsg, QueryResult},
    write_initial_states, StoreMeta, StoreRouter, StoreSystem,
};
use slog::o;
use tempfile::TempDir;
use test_pd_client::TestPdClient;
use test_raftstore::{
    is_error_response, new_admin_request, new_delete_cmd, new_delete_range_cmd, new_get_cf_cmd,
    new_peer, new_put_cf_cmd, new_region_detail_cmd, new_region_leader_cmd, new_request,
    new_snap_cmd, new_status_request, new_store, new_tikv_config_with_api_ver,
    new_transfer_leader_cmd, sleep_ms, Config, Filter, FilterFactory, PartitionFilterFactory,
    RawEngine,
};
use tikv::server::Result as ServerResult;
use tikv_util::{
    box_err, box_try, debug, error, safe_panic, thread_group::GroupProperties, time::Instant,
    timer::GLOBAL_TIMER_HANDLE, warn, HandyRwLock,
};

use crate::{transport_simulate::RaftStoreRouter, util::create_test_engine};

// We simulate 3 or 5 nodes, each has a store.
// Sometimes, we use fixed id to test, which means the id
// isn't allocated by pd, and node id, store id are same.
// E,g, for node 1, the node id and store id are both 1.
pub trait Simulator {
    // Pass 0 to let pd allocate a node id if db is empty.
    // If node id > 0, the node must be created in db already,
    // and the node id must be the same as given argument.
    // Return the node id.
    // TODO: we will rename node name here because now we use store only.
    fn run_node(
        &mut self,
        node_id: u64,
        cfg: Config,
        store_meta: Arc<Mutex<StoreMeta<RocksEngine>>>,
        router: StoreRouter<RocksEngine, RaftTestEngine>,
        system: StoreSystem<RocksEngine, RaftTestEngine>,
        raft_engine: RaftTestEngine,
        factory: Arc<dyn TabletFactory<RocksEngine>>,
    ) -> ServerResult<u64>;

    fn stop_node(&mut self, node_id: u64);
    fn get_node_ids(&self) -> HashSet<u64>;
    fn add_send_filter(&mut self, node_id: u64, filter: Box<dyn Filter>);
    fn clear_send_filters(&mut self, node_id: u64);
    fn get_router(&self, node_id: u64) -> Option<StoreRouter<RocksEngine, RaftTestEngine>>;
    fn get_snap_dir(&self, node_id: u64) -> String;

    fn read(&mut self, request: RaftCmdRequest, timeout: Duration) -> Result<RaftCmdResponse> {
        let node_id = request.get_header().get_peer().get_store_id();
        let region_id = request.get_header().get_region_id();

        let mut req_clone = request.clone();
        req_clone.clear_requests();
        req_clone.mut_requests().push(new_snap_cmd());
        match self.snapshot(req_clone, timeout) {
            Ok(snap) => {
                let requests = request.get_requests();
                let mut response = RaftCmdResponse::default();
                let mut responses = Vec::with_capacity(requests.len());
                for req in requests {
                    let cmd_type = req.get_cmd_type();
                    match cmd_type {
                        CmdType::Get => {
                            let mut resp = Response::default();
                            let key = req.get_get().get_key();
                            let cf = req.get_get().get_cf();
                            let region = snap.get_region();

                            if let Err(e) = check_key_in_region(key, region) {
                                return Ok(cmd_resp::new_error(e));
                            }

                            let res = if cf.is_empty() {
                                snap.get_value(key).unwrap_or_else(|e| {
                                    panic!(
                                        "[region {}] failed to get {} with cf {}: {:?}",
                                        snap.get_region().get_id(),
                                        log_wrappers::Value::key(key),
                                        cf,
                                        e
                                    )
                                })
                            } else {
                                snap.get_value_cf(cf, key).unwrap_or_else(|e| {
                                    panic!(
                                        "[region {}] failed to get {}: {:?}",
                                        snap.get_region().get_id(),
                                        log_wrappers::Value::key(key),
                                        e
                                    )
                                })
                            };
                            if let Some(res) = res {
                                resp.mut_get().set_value(res.to_vec());
                            }
                            resp.set_cmd_type(cmd_type);
                            responses.push(resp);
                        }
                        _ => unimplemented!(),
                    }
                }
                response.set_responses(responses.into());

                Ok(response)
            }
            Err(e) => Ok(e),
        }
    }

    fn snapshot(
        &mut self,
        request: RaftCmdRequest,
        timeout: Duration,
    ) -> std::result::Result<
        RegionSnapshot<<RocksEngine as KvEngine>::Snapshot, Prefix>,
        RaftCmdResponse,
    >;

    fn async_peer_msg_on_node(&self, node_id: u64, region_id: u64, msg: PeerMsg) -> Result<()>;

    fn call_query(&self, request: RaftCmdRequest, timeout: Duration) -> Result<RaftCmdResponse> {
        let node_id = request.get_header().get_peer().get_store_id();
        self.call_query_on_node(node_id, request, timeout)
    }

    fn call_query_on_node(
        &self,
        node_id: u64,
        request: RaftCmdRequest,
        timeout: Duration,
    ) -> Result<RaftCmdResponse> {
        let region_id = request.get_header().get_region_id();
        let (msg, sub) = PeerMsg::raft_query(request);
        match self.async_peer_msg_on_node(node_id, region_id, msg) {
            Ok(()) => {}
            Err(e) => {
                let mut resp = RaftCmdResponse::default();
                resp.mut_header().set_error(e.into());
                return Ok(resp);
            }
        }

        // todo(SpadeA): unwrap and timeout
        match block_on(sub.result()).unwrap() {
            QueryResult::Read(_) => unreachable!(),
            QueryResult::Response(resp) => Ok(resp),
        }
    }

    fn call_command(&self, request: RaftCmdRequest, timeout: Duration) -> Result<RaftCmdResponse> {
        let node_id = request.get_header().get_peer().get_store_id();
        self.call_command_on_node(node_id, request, timeout)
    }

    fn call_command_on_node(
        &self,
        node_id: u64,
        request: RaftCmdRequest,
        timeout: Duration,
    ) -> Result<RaftCmdResponse> {
        let region_id = request.get_header().get_region_id();
        let (msg, sub) = PeerMsg::raft_command(request);
        match self.async_peer_msg_on_node(node_id, region_id, msg) {
            Ok(()) => {}
            Err(e) => {
                let mut resp = RaftCmdResponse::default();
                resp.mut_header().set_error(e.into());
                return Ok(resp);
            }
        }

        let timeout_f = GLOBAL_TIMER_HANDLE.delay(std::time::Instant::now() + timeout);
        block_on(async move {
            select! {
                // todo: unwrap?
                res = sub.result().fuse() => Ok(res.unwrap()),
                _ = timeout_f.compat().fuse() => Err(Error::Timeout(format!("request timeout for {:?}", timeout))),

            }
        })
    }
}

pub struct Cluster<T: Simulator> {
    pub cfg: Config,
    leaders: HashMap<u64, metapb::Peer>,
    pub count: usize,

    pub paths: Vec<TempDir>,
    pub dbs: Vec<(
        Arc<dyn TabletFactory<RocksEngine> + Send + Sync>,
        RaftTestEngine,
    )>,
    pub tablet_factories: HashMap<u64, Arc<dyn TabletFactory<RocksEngine> + Send + Sync>>,
    pub raft_engines: HashMap<u64, RaftTestEngine>,
    pub store_metas: HashMap<u64, Arc<Mutex<StoreMeta<RocksEngine>>>>,
    key_managers: Vec<Option<Arc<DataKeyManager>>>,
    pub io_rate_limiter: Option<Arc<IoRateLimiter>>,
    key_managers_map: HashMap<u64, Option<Arc<DataKeyManager>>>,
    group_props: HashMap<u64, GroupProperties>,

    pub pd_client: Arc<TestPdClient>,

    pub sim: Arc<RwLock<T>>,
}

impl<T: Simulator> Cluster<T> {
    pub fn new(
        id: u64,
        count: usize,
        sim: Arc<RwLock<T>>,
        pd_client: Arc<TestPdClient>,
        api_version: ApiVersion,
    ) -> Cluster<T> {
        Cluster {
            cfg: Config {
                tikv: new_tikv_config_with_api_ver(id, api_version),
                prefer_mem: true,
            },
            count,
            tablet_factories: HashMap::default(),
            key_managers_map: HashMap::default(),
            group_props: HashMap::default(),
            raft_engines: HashMap::default(),
            store_metas: HashMap::default(),
            leaders: HashMap::default(),
            paths: vec![],
            dbs: vec![],
            key_managers: vec![],
            io_rate_limiter: None,
            sim,
            pd_client,
        }
    }

    pub fn id(&self) -> u64 {
        self.cfg.server.cluster_id
    }

    // Bootstrap the store with fixed ID (like 1, 2, .. 5) and
    // initialize first region in all stores, then start the cluster.
    pub fn run(&mut self) {
        self.create_engines();
        self.bootstrap_region().unwrap();
        self.start().unwrap();
    }

    // Bootstrap the store with fixed ID (like 1, 2, .. 5) and
    // initialize first region in store 1, then start the cluster.
    pub fn run_conf_change(&mut self) -> u64 {
        self.create_engines();
        let region_id = self.bootstrap_conf_change();
        self.start().unwrap();
        region_id
    }

    pub fn create_engines(&mut self) {
        self.io_rate_limiter = Some(Arc::new(
            self.cfg
                .storage
                .io_rate_limit
                .build(true /* enable_statistics */),
        ));
        for _ in 0..self.count {
            self.create_engine(None);
        }
    }

    fn create_engine(&mut self, router: Option<StoreRouter<RocksEngine, RaftTestEngine>>) {
        let (factory, raft_engine, key_manager, dir) =
            create_test_engine(router, self.io_rate_limiter.clone(), &self.cfg);
        self.dbs.push((factory, raft_engine));
        self.key_managers.push(key_manager);
        self.paths.push(dir);
    }

    pub fn start(&mut self) -> ServerResult<()> {
        if self.cfg.raft_store.store_io_pool_size == 0 {
            // v2 always use async write.
            self.cfg.raft_store.store_io_pool_size = 1;
        }

        let node_ids: Vec<u64> = self.tablet_factories.iter().map(|(&id, _)| id).collect();
        for node_id in node_ids {
            self.run_node(node_id)?;
        }

        // Try start new nodes.
        for _ in 0..self.count - self.raft_engines.len() {
            let logger = slog_global::borrow_global().new(o!());
            let (router, system) = create_store_batch_system(&self.cfg.raft_store, logger);
            self.create_engine(Some(router.clone()));

            let (factory, raft_engine) = self.dbs.last().unwrap().clone();
            let key_mgr = self.key_managers.last().unwrap().clone();
            let store_meta = Arc::new(Mutex::new(StoreMeta::<RocksEngine>::default()));

            let props = GroupProperties::default();
            tikv_util::thread_group::set_properties(Some(props.clone()));

            // todo: GroupProperties
            let mut sim = self.sim.wl();
            let node_id = sim.run_node(
                0,
                self.cfg.clone(),
                store_meta.clone(),
                router,
                system,
                raft_engine.clone(),
                factory.clone(),
            )?;
            self.group_props.insert(node_id, props);
            self.raft_engines.insert(node_id, raft_engine);
            self.tablet_factories.insert(node_id, factory);
            self.store_metas.insert(node_id, store_meta);
            self.key_managers_map.insert(node_id, key_mgr);
        }

        Ok(())
    }

    pub fn run_node(&mut self, node_id: u64) -> ServerResult<()> {
        debug!("starting node {}", node_id);
        let logger = slog_global::borrow_global().new(o!());
        let factory = self.tablet_factories[&node_id].clone();
        let raft_engine = self.raft_engines[&node_id].clone();
        let cfg = self.cfg.clone();

        // if let Some(labels) = self.labels.get(&node_id) {
        //     cfg.server.labels = labels.to_owned();
        // }
        let (router, system) = create_store_batch_system(&cfg.raft_store, logger.clone());
        let store_meta = match self.store_metas.entry(node_id) {
            MapEntry::Occupied(o) => {
                let mut meta = o.get().lock().unwrap();
                *meta = StoreMeta::new();
                o.get().clone()
            }
            MapEntry::Vacant(v) => v.insert(Arc::new(Mutex::new(StoreMeta::new()))).clone(),
        };

        let props = GroupProperties::default();
        self.group_props.insert(node_id, props.clone());
        tikv_util::thread_group::set_properties(Some(props));

        debug!("calling run node"; "node_id" => node_id);
        self.sim.wl().run_node(
            node_id,
            cfg,
            store_meta,
            router,
            system,
            raft_engine,
            factory,
        )?;
        debug!("node {} started", node_id);
        Ok(())
    }

    pub fn stop_node(&mut self, node_id: u64) {
        debug!("stopping node {}", node_id);
        self.group_props[&node_id].mark_shutdown();

        match self.sim.write() {
            Ok(mut sim) => sim.stop_node(node_id),
            Err(_) => safe_panic!("failed to acquire write lock."),
        }
        self.pd_client.shutdown_store(node_id);
        debug!("node {} stopped", node_id);
    }

    /// Multiple nodes with fixed node id, like node 1, 2, .. 5,
    /// First region 1 is in all stores with peer 1, 2, .. 5.
    /// Peer 1 is in node 1, store 1, etc.
    ///
    /// Must be called after `create_engines`.
    pub fn bootstrap_region(&mut self) -> Result<()> {
        for (i, (factory, raft_engine)) in self.dbs.iter().enumerate() {
            let id = i as u64 + 1;
            self.tablet_factories.insert(id, factory.clone());
            self.raft_engines.insert(id, raft_engine.clone());
            let store_meta = Arc::new(Mutex::new(StoreMeta::<RocksEngine>::default()));
            self.store_metas.insert(id, store_meta);
            self.key_managers_map
                .insert(id, self.key_managers[i].clone());
            // todo: sst workers
        }

        let mut region = metapb::Region::default();
        region.set_id(1);
        region.set_start_key(keys::EMPTY_KEY.to_vec());
        region.set_end_key(keys::EMPTY_KEY.to_vec());
        region.mut_region_epoch().set_version(INIT_EPOCH_VER);
        region.mut_region_epoch().set_conf_ver(INIT_EPOCH_CONF_VER);

        for (&id, raft_engine) in &self.raft_engines {
            let peer = new_peer(id, id);
            region.mut_peers().push(peer.clone());
            bootstrap_store(raft_engine, self.id(), id).unwrap();
        }

        for raft_engine in self.raft_engines.values() {
            let mut wb = raft_engine.log_batch(10);
            wb.put_prepare_bootstrap_region(&region)?;
            write_initial_states(&mut wb, region.clone())?;
            box_try!(raft_engine.consume(&mut wb, true));
        }

        self.bootstrap_cluster(region);

        Ok(())
    }

    pub fn bootstrap_conf_change(&mut self) -> u64 {
        for (i, (factory, raft_engine)) in self.dbs.iter().enumerate() {
            let id = i as u64 + 1;
            self.tablet_factories.insert(id, factory.clone());
            self.raft_engines.insert(id, raft_engine.clone());
            let store_meta = Arc::new(Mutex::new(StoreMeta::<RocksEngine>::default()));
            self.store_metas.insert(id, store_meta);
            self.key_managers_map
                .insert(id, self.key_managers[i].clone());
            // todo: sst workers
        }

        for (&id, raft_engine) in &self.raft_engines {
            bootstrap_store(raft_engine, self.id(), id).unwrap();
        }

        let node_id = 1;
        let region_id = 1;
        let peer_id = 1;

        let region = initial_region(node_id, region_id, peer_id);
        let raft_engine = self.raft_engines[&node_id].clone();
        let mut wb = raft_engine.log_batch(10);
        wb.put_prepare_bootstrap_region(&region).unwrap();
        write_initial_states(&mut wb, region.clone()).unwrap();
        raft_engine.consume(&mut wb, true).unwrap();

        self.bootstrap_cluster(region);

        region_id
    }

    // This is only for fixed id test
    fn bootstrap_cluster(&mut self, region: metapb::Region) {
        self.pd_client
            .bootstrap_cluster(new_store(1, "".to_owned()), region)
            .unwrap();
        for id in self.raft_engines.keys() {
            let store = new_store(*id, "".to_owned());
            // todo: labels
            self.pd_client.put_store(store).unwrap();
        }
    }

    pub fn get_engine(&self, node_id: u64) -> WrapFactory {
        WrapFactory::new(
            self.pd_client.clone(),
            self.raft_engines[&node_id].clone(),
            self.tablet_factories[&node_id].clone(),
        )
    }

    // pub fn query_debug_info(&self, node_id: u64, region_id: u64) ->
    // Option<RegionMeta> {     let core =
    // self.node_cluster.channel_transport().core().lock().unwrap();
    //     let router = core.routers.get(&node_id).unwrap();
    //     let (ch, sub) = DebugInfoChannel::pair();
    //     let msg = PeerMsg::QueryDebugInfo(ch);
    //     let res = router.send_peer_msg(region_id, msg);
    //     if res.is_err() {
    //         return None;
    //     }
    //     block_on(sub.result())
    // }

    pub fn get_engine_impl(
        &self,
        node_id: u64,
        region_id: u64,
        suffix: Option<u64>,
    ) -> RocksEngine {
        self.tablet_factories[&node_id]
            .clone()
            .open_tablet(
                region_id,
                suffix,
                OpenOptions::default().set_cache_only(true),
            )
            .unwrap()
    }

    pub fn call_command(
        &mut self,
        request: RaftCmdRequest,
        timeout: Duration,
    ) -> Result<RaftCmdResponse> {
        let mut is_read = false;
        for req in request.get_requests() {
            match req.get_cmd_type() {
                CmdType::Get | CmdType::Snap | CmdType::ReadIndex => {
                    is_read = true;
                }
                _ => (),
            }
        }
        let ret = if is_read {
            self.sim.wl().read(request.clone(), timeout)
        } else {
            if request.has_status_request() {
                self.sim.wl().call_query(request.clone(), timeout)
            } else {
                self.sim.wl().call_command(request.clone(), timeout)
            }
        };
        match ret {
            Err(e) => {
                warn!("failed to call command {:?}: {:?}", request, e);
                Err(e)
            }
            a => a,
        }
    }

    pub fn call_command_on_leader(
        &mut self,
        mut request: RaftCmdRequest,
        timeout: Duration,
    ) -> Result<RaftCmdResponse> {
        let timer = Instant::now();
        let region_id = request.get_header().get_region_id();
        loop {
            let leader = match self.leader_of_region(region_id) {
                None => return Err(Error::NotLeader(region_id, None)),
                Some(l) => l,
            };
            request.mut_header().set_peer(leader);
            let resp = match self.call_command(request.clone(), timeout) {
                e @ Err(_) => return e,
                Ok(resp) => resp,
            };
            if self.refresh_leader_if_needed(&resp, region_id)
                && timer.saturating_elapsed() < timeout
            {
                warn!(
                    "{:?} is no longer leader, let's retry",
                    request.get_header().get_peer()
                );
                continue;
            }
            return Ok(resp);
        }
    }

    pub fn call_command_on_node(
        &self,
        node_id: u64,
        request: RaftCmdRequest,
        timeout: Duration,
    ) -> Result<RaftCmdResponse> {
        match self
            .sim
            .rl()
            .call_command_on_node(node_id, request.clone(), timeout)
        {
            Err(e) => {
                warn!("failed to call command {:?}: {:?}", request, e);
                Err(e)
            }
            a => a,
        }
    }

    pub fn leader_of_region(&mut self, region_id: u64) -> Option<metapb::Peer> {
        let timer = Instant::now_coarse();
        let timeout = Duration::from_secs(5);
        let mut store_ids = None;
        while timer.saturating_elapsed() < timeout {
            match self.voter_store_ids_of_region(region_id) {
                None => thread::sleep(Duration::from_millis(10)),
                Some(ids) => {
                    store_ids = Some(ids);
                    break;
                }
            }
        }
        let store_ids = store_ids?;
        if let Some(l) = self.leaders.get(&region_id) {
            // leader may be stopped in some tests.
            if self.valid_leader_id(region_id, l.get_store_id()) {
                return Some(l.clone());
            }
        }
        self.reset_leader_of_region(region_id);
        let mut leader = None;
        let mut leaders = HashMap::default();

        let node_ids = self.sim.rl().get_node_ids();
        // For some tests, we stop the node but pd still has this information,
        // and we must skip this.
        let alive_store_ids: Vec<_> = store_ids
            .iter()
            .filter(|id| node_ids.contains(id))
            .cloned()
            .collect();
        while timer.saturating_elapsed() < timeout {
            for store_id in &alive_store_ids {
                let l = match self.query_leader(*store_id, region_id, Duration::from_secs(1)) {
                    None => continue,
                    Some(l) => l,
                };
                leaders
                    .entry(l.get_id())
                    .or_insert((l, vec![]))
                    .1
                    .push(*store_id);
            }
            if let Some((_, (l, c))) = leaders.iter().max_by_key(|(_, (_, c))| c.len()) {
                if c.contains(&l.get_store_id()) {
                    leader = Some(l.clone());
                    // Technically, correct calculation should use two quorum when in joint
                    // state. Here just for simplicity.
                    if c.len() > store_ids.len() / 2 {
                        break;
                    }
                }
            }
            debug!("failed to detect leaders"; "leaders" => ?leaders, "store_ids" => ?store_ids);
            sleep_ms(10);
            leaders.clear();
        }

        if let Some(l) = leader {
            self.leaders.insert(region_id, l);
        }

        self.leaders.get(&region_id).cloned()
    }

    pub fn query_leader(
        &mut self,
        store_id: u64,
        region_id: u64,
        timeout: Duration,
    ) -> Option<metapb::Peer> {
        // To get region leader, we don't care real peer id, so use 0 instead.
        let peer = new_peer(store_id, 0);
        let find_leader = new_status_request(region_id, peer, new_region_leader_cmd());
        let mut resp = match self.call_command(find_leader, timeout) {
            Ok(resp) => resp,
            Err(err) => {
                error!(
                    "fail to get leader of region {} on store {}, error: {:?}",
                    region_id, store_id, err
                );
                return None;
            }
        };
        let mut region_leader = resp.take_status_response().take_region_leader();
        // NOTE: node id can't be 0.
        if self.valid_leader_id(region_id, region_leader.get_leader().get_store_id()) {
            Some(region_leader.take_leader())
        } else {
            None
        }
    }

    fn valid_leader_id(&self, region_id: u64, leader_id: u64) -> bool {
        let store_ids = match self.voter_store_ids_of_region(region_id) {
            None => return false,
            Some(ids) => ids,
        };
        let node_ids = self.sim.rl().get_node_ids();
        store_ids.contains(&leader_id) && node_ids.contains(&leader_id)
    }

    fn voter_store_ids_of_region(&self, region_id: u64) -> Option<Vec<u64>> {
        block_on(self.pd_client.get_region_by_id(region_id))
            .unwrap()
            .map(|region| {
                region
                    .get_peers()
                    .iter()
                    .flat_map(|p| {
                        if p.get_role() != PeerRole::Learner {
                            Some(p.get_store_id())
                        } else {
                            None
                        }
                    })
                    .collect()
            })
    }

    pub fn reset_leader_of_region(&mut self, region_id: u64) {
        self.leaders.remove(&region_id);
    }

    // If the resp is "not leader error", get the real leader.
    // Otherwise reset or refresh leader if needed.
    // Returns if the request should retry.
    fn refresh_leader_if_needed(&mut self, resp: &RaftCmdResponse, region_id: u64) -> bool {
        if !is_error_response(resp) {
            return false;
        }

        let err = resp.get_header().get_error();
        if err
            .get_message()
            .contains("peer has not applied to current term")
        {
            // leader peer has not applied to current term
            return true;
        }

        // If command is stale, leadership may have changed.
        // EpochNotMatch is not checked as leadership is checked first in raftstore.
        if err.has_stale_command() {
            self.reset_leader_of_region(region_id);
            return true;
        }

        if !err.has_not_leader() {
            return false;
        }
        let err = err.get_not_leader();
        if !err.has_leader() {
            self.reset_leader_of_region(region_id);
            return true;
        }
        self.leaders.insert(region_id, err.get_leader().clone());
        true
    }

    pub fn request(
        &mut self,
        key: &[u8],
        reqs: Vec<Request>,
        read_quorum: bool,
        timeout: Duration,
    ) -> RaftCmdResponse {
        let timer = Instant::now();
        let mut tried_times = 0;
        while tried_times < 2 || timer.saturating_elapsed() < timeout {
            tried_times += 1;
            let mut region = self.get_region(key);
            let region_id = region.get_id();
            let req = new_request(
                region_id,
                region.take_region_epoch(),
                reqs.clone(),
                read_quorum,
            );
            let result = self.call_command_on_leader(req, timeout);

            let resp = match result {
                e @ Err(Error::Timeout(_))
                | e @ Err(Error::NotLeader(..))
                | e @ Err(Error::StaleCommand) => {
                    warn!("call command failed, retry it"; "err" => ?e);
                    sleep_ms(100);
                    continue;
                }
                Err(e) => panic!("call command failed {:?}", e),
                Ok(resp) => resp,
            };

            if resp.get_header().get_error().has_epoch_not_match() {
                warn!("seems split, let's retry");
                sleep_ms(100);
                continue;
            }
            if resp
                .get_header()
                .get_error()
                .get_message()
                .contains("merging mode")
            {
                warn!("seems waiting for merge, let's retry");
                sleep_ms(100);
                continue;
            }
            return resp;
        }
        panic!("request timeout");
    }

    pub fn get_region(&self, key: &[u8]) -> metapb::Region {
        self.get_region_with(key, |_| true)
    }

    pub fn get_region_id(&self, key: &[u8]) -> u64 {
        self.get_region(key).get_id()
    }

    pub fn get_region_epoch(&self, region_id: u64) -> RegionEpoch {
        block_on(self.pd_client.get_region_by_id(region_id))
            .unwrap()
            .unwrap()
            .take_region_epoch()
    }

    pub fn region_detail(&mut self, region_id: u64, store_id: u64) -> RegionDetailResponse {
        let status_cmd = new_region_detail_cmd();
        let peer = new_peer(store_id, 0);
        let req = new_status_request(region_id, peer, status_cmd);
        let resp = self.call_command(req, Duration::from_secs(5));
        assert!(resp.is_ok(), "{:?}", resp);

        let mut resp = resp.unwrap();
        assert!(resp.has_status_response());
        let mut status_resp = resp.take_status_response();
        assert_eq!(status_resp.get_cmd_type(), StatusCmdType::RegionDetail);
        assert!(status_resp.has_region_detail());
        status_resp.take_region_detail()
    }

    pub fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        self.get_impl(CF_DEFAULT, key, false)
    }

    pub fn get_cf(&mut self, cf: &str, key: &[u8]) -> Option<Vec<u8>> {
        self.get_impl(cf, key, false)
    }

    fn get_impl(&mut self, cf: &str, key: &[u8], read_quorum: bool) -> Option<Vec<u8>> {
        let mut resp = self.request(
            key,
            vec![new_get_cf_cmd(cf, key)],
            read_quorum,
            Duration::from_secs(5),
        );
        if resp.get_header().has_error() {
            panic!("response {:?} has error", resp);
        }
        assert_eq!(resp.get_responses().len(), 1);
        assert_eq!(resp.get_responses()[0].get_cmd_type(), CmdType::Get);
        if resp.get_responses()[0].has_get() {
            Some(resp.mut_responses()[0].mut_get().take_value())
        } else {
            None
        }
    }

    // Get region when the `filter` returns true.
    pub fn get_region_with<F>(&self, key: &[u8], filter: F) -> metapb::Region
    where
        F: Fn(&metapb::Region) -> bool,
    {
        for _ in 0..100 {
            if let Ok(region) = self.pd_client.get_region(key) {
                if filter(&region) {
                    return region;
                }
            }
            // We may meet range gap after split, so here we will
            // retry to get the region again.
            sleep_ms(20);
        }

        panic!("find no region for {}", log_wrappers::hex_encode_upper(key));
    }

    pub fn must_put(&mut self, key: &[u8], value: &[u8]) {
        self.must_put_cf(CF_DEFAULT, key, value);
    }

    pub fn must_put_cf(&mut self, cf: &str, key: &[u8], value: &[u8]) {
        if let Err(e) = self.batch_put(key, vec![new_put_cf_cmd(cf, key, value)]) {
            panic!("has error: {:?}", e);
        }
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> result::Result<(), PbError> {
        self.batch_put(key, vec![new_put_cf_cmd(CF_DEFAULT, key, value)])
            .map(|_| ())
    }

    pub fn batch_put(
        &mut self,
        region_key: &[u8],
        reqs: Vec<Request>,
    ) -> result::Result<RaftCmdResponse, PbError> {
        let resp = self.request(region_key, reqs, false, Duration::from_secs(5));
        if resp.get_header().has_error() {
            Err(resp.get_header().get_error().clone())
        } else {
            Ok(resp)
        }
    }

    pub fn must_delete(&mut self, key: &[u8]) {
        self.must_delete_cf(CF_DEFAULT, key)
    }

    pub fn must_delete_cf(&mut self, cf: &str, key: &[u8]) {
        let resp = self.request(
            key,
            vec![new_delete_cmd(cf, key)],
            false,
            Duration::from_secs(5),
        );
        if resp.get_header().has_error() {
            panic!("response {:?} has error", resp);
        }
    }

    pub fn must_delete_range_cf(&mut self, cf: &str, start: &[u8], end: &[u8]) {
        let resp = self.request(
            start,
            vec![new_delete_range_cmd(cf, start, end)],
            false,
            Duration::from_secs(5),
        );
        if resp.get_header().has_error() {
            panic!("response {:?} has error", resp);
        }
    }

    pub fn must_notify_delete_range_cf(&mut self, cf: &str, start: &[u8], end: &[u8]) {
        let mut req = new_delete_range_cmd(cf, start, end);
        req.mut_delete_range().set_notify_only(true);
        let resp = self.request(start, vec![req], false, Duration::from_secs(5));
        if resp.get_header().has_error() {
            panic!("response {:?} has error", resp);
        }
    }

    // Flush the cf of all opened tablets
    pub fn must_flush_cf(&mut self, cf: &str, sync: bool) {
        for factory in self.tablet_factories.values() {
            factory.for_each_opened_tablet(&mut |_id, _suffix, db| {
                db.flush_cf(cf, true).unwrap();
            });
        }
    }

    pub fn apply_state(&self, region_id: u64, store_id: u64) -> RaftApplyState {
        self.get_raft_engine(store_id)
            .get_apply_state(region_id)
            .unwrap()
            .unwrap()
    }

    pub fn add_send_filter<F: FilterFactory>(&self, factory: F) {
        let mut sim = self.sim.wl();
        for node_id in sim.get_node_ids() {
            for filter in factory.generate(node_id) {
                sim.add_send_filter(node_id, filter);
            }
        }
    }

    pub fn clear_send_filters(&self) {
        let mut sim = self.sim.wl();
        for node_id in sim.get_node_ids() {
            sim.clear_send_filters(node_id);
        }
    }

    pub fn transfer_leader(&mut self, region_id: u64, leader: metapb::Peer) {
        let epoch = self.get_region_epoch(region_id);
        let transfer_leader = new_admin_request(region_id, &epoch, new_transfer_leader_cmd(leader));
        let resp = self
            .call_command_on_leader(transfer_leader, Duration::from_secs(5))
            .unwrap();
        assert_eq!(
            resp.get_admin_response().get_cmd_type(),
            AdminCmdType::TransferLeader,
            "{:?}",
            resp
        );
    }

    // it's so common that we provide an API for it
    pub fn partition(&mut self, s1: Vec<u64>, s2: Vec<u64>) {
        self.add_send_filter(PartitionFilterFactory::new(s1, s2));
    }

    pub fn must_transfer_leader(&mut self, region_id: u64, leader: metapb::Peer) {
        let timer = Instant::now();
        loop {
            self.reset_leader_of_region(region_id);
            let cur_leader = self.leader_of_region(region_id);
            if let Some(ref cur_leader) = cur_leader {
                if cur_leader.get_id() == leader.get_id()
                    && cur_leader.get_store_id() == leader.get_store_id()
                {
                    return;
                }
            }
            if timer.saturating_elapsed() > Duration::from_secs(5) {
                panic!(
                    "failed to transfer leader to [{}] {:?}, current leader: {:?}",
                    region_id, leader, cur_leader
                );
            }
            self.transfer_leader(region_id, leader.clone());
        }
    }

    pub fn try_transfer_leader(&mut self, region_id: u64, leader: metapb::Peer) -> RaftCmdResponse {
        let epoch = self.get_region_epoch(region_id);
        let transfer_leader = new_admin_request(region_id, &epoch, new_transfer_leader_cmd(leader));
        self.call_command_on_leader(transfer_leader, Duration::from_secs(5))
            .unwrap()
    }

    // Get region ids of all opened tablets in a store
    pub fn region_ids(&self, store_id: u64) -> Vec<u64> {
        let mut ids = vec![];
        let factory = self.tablet_factories.get(&store_id).unwrap();
        factory.for_each_opened_tablet(&mut |id, _, _| {
            ids.push(id);
        });
        ids
    }

    pub fn get_raft_engine(&self, node_id: u64) -> RaftTestEngine {
        self.raft_engines[&node_id].clone()
    }

    pub fn scan<F>(
        &self,
        store_id: u64,
        region_id: u64,
        cf: &str,
        start_key: &[u8],
        end_key: &[u8],
        fill_cache: bool,
        f: F,
    ) -> engine_traits::Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> engine_traits::Result<bool>,
    {
        let factory = self.tablet_factories.get(&store_id).unwrap();
        let tablet = factory
            .open_tablet(region_id, None, OpenOptions::default().set_cache_only(true))
            .unwrap();

        let region = block_on(self.pd_client.get_region_by_id(region_id))
            .unwrap()
            .unwrap();
        let region_start_key = region.get_start_key();
        let region_end_key = region.get_end_key();

        let amended_start_key = if start_key > region_start_key {
            start_key
        } else {
            region_start_key
        };
        let amended_end_key = if end_key < region_end_key || region_end_key.is_empty() {
            end_key
        } else {
            region_end_key
        };

        tablet.scan(
            cf,
            &data_key(amended_start_key),
            &data_key(amended_end_key),
            fill_cache,
            f,
        )
    }

    // It's similar to `ask_split`, the difference is the msg, it sends, is
    // `Msg::SplitRegion`, and `region` will not be embedded to that msg.
    // Caller must ensure that the `split_key` is in the `region`.
    pub fn split_region(
        &mut self,
        region: &metapb::Region,
        split_key: &[u8],
        mut cb: Callback<RocksSnapshot>,
    ) {
        let leader = self.leader_of_region(region.get_id()).unwrap();
        let router = self.sim.rl().get_router(leader.get_store_id()).unwrap();
        let split_key = split_key.to_vec();
        let (split_region_req, mut sub) = PeerMsg::split_request(
            region.get_region_epoch().clone(),
            vec![split_key],
            "test".into(),
        );

        router
            .send_peer_msg(region.get_id(), split_region_req)
            .unwrap();

        block_on(async {
            sub.wait_proposed().await;
            cb.invoke_proposed();
            sub.wait_committed().await;
            cb.invoke_committed();
            let res = sub.result().await.unwrap();
            cb.invoke_with_response(res)
        });
    }

    pub fn must_split(&mut self, region: &metapb::Region, split_key: &[u8]) {
        let mut try_cnt = 0;
        let split_count = self.pd_client.get_split_count();
        loop {
            debug!("asking split"; "region" => ?region, "key" => ?split_key);
            // In case ask split message is ignored, we should retry.
            if try_cnt % 50 == 0 {
                self.reset_leader_of_region(region.get_id());
                let key = split_key.to_vec();
                let check = Box::new(move |write_resp: WriteResponse| {
                    let mut resp = write_resp.response;
                    if resp.get_header().has_error() {
                        let error = resp.get_header().get_error();
                        if error.has_epoch_not_match()
                            || error.has_not_leader()
                            || error.has_stale_command()
                            || error
                                .get_message()
                                .contains("peer has not applied to current term")
                        {
                            warn!("fail to split: {:?}, ignore.", error);
                            return;
                        }
                        panic!("failed to split: {:?}", resp);
                    }
                    let admin_resp = resp.mut_admin_response();
                    let split_resp = admin_resp.mut_splits();
                    let regions = split_resp.get_regions();
                    assert_eq!(regions.len(), 2);
                    assert_eq!(regions[0].get_end_key(), key.as_slice());
                    assert_eq!(regions[0].get_end_key(), regions[1].get_start_key());
                });
                if self.leader_of_region(region.get_id()).is_some() {
                    self.split_region(region, split_key, Callback::write(check));
                }
            }

            if self.pd_client.check_split(region, split_key)
                && self.pd_client.get_split_count() > split_count
            {
                return;
            }

            if try_cnt > 250 {
                panic!(
                    "region {:?} has not been split by {}",
                    region,
                    log_wrappers::hex_encode_upper(split_key)
                );
            }
            try_cnt += 1;
            sleep_ms(20);
        }
    }

    pub fn wait_region_split(&mut self, region: &metapb::Region) {
        self.wait_region_split_max_cnt(region, 20, 250, true);
    }

    pub fn wait_region_split_max_cnt(
        &mut self,
        region: &metapb::Region,
        itvl_ms: u64,
        max_try_cnt: u64,
        is_panic: bool,
    ) {
        let mut try_cnt = 0;
        let split_count = self.pd_client.get_split_count();
        loop {
            if self.pd_client.get_split_count() > split_count {
                match self.pd_client.get_region(region.get_start_key()) {
                    Err(_) => {}
                    Ok(left) => {
                        if left.get_end_key() != region.get_end_key() {
                            return;
                        }
                    }
                };
            }

            if try_cnt > max_try_cnt {
                if is_panic {
                    panic!(
                        "region {:?} has not been split after {}ms",
                        region,
                        max_try_cnt * itvl_ms
                    );
                } else {
                    return;
                }
            }
            try_cnt += 1;
            sleep_ms(itvl_ms);
        }
    }

    pub fn get_snap_dir(&self, node_id: u64) -> String {
        self.sim.rl().get_snap_dir(node_id)
    }

    pub fn refresh_region_bucket_keys(
        &mut self,
        region: &metapb::Region,
        buckets: Vec<Bucket>,
        bucket_ranges: Option<Vec<BucketRange>>,
        expect_buckets: Option<Buckets>,
    ) -> u64 {
        unimplemented!()
    }

    pub fn send_half_split_region_message(
        &mut self,
        region: &metapb::Region,
        expected_bucket_ranges: Option<Vec<BucketRange>>,
    ) {
        unimplemented!()
    }

    pub fn shutdown(&mut self) {
        debug!("about to shutdown cluster");
        let keys = match self.sim.read() {
            Ok(s) => s.get_node_ids(),
            Err(_) => {
                safe_panic!("failed to acquire read lock");
                // Leave the resource to avoid double panic.
                return;
            }
        };
        for id in keys {
            self.stop_node(id);
        }
        self.leaders.clear();
        self.store_metas.clear();
        // for sst_worker in self.sst_workers.drain(..) {
        //     sst_worker.stop_worker();
        // }
        debug!("all nodes are shut down.");
    }
}

pub fn bootstrap_store<ER: RaftEngine>(
    raft_engine: &ER,
    cluster_id: u64,
    store_id: u64,
) -> Result<()> {
    let mut ident = StoreIdent::default();

    if !raft_engine.is_empty()? {
        return Err(box_err!("store is not empty and has already had data"));
    }

    ident.set_cluster_id(cluster_id);
    ident.set_store_id(store_id);

    raft_engine.put_store_ident(&ident)?;
    raft_engine.sync()?;

    Ok(())
}

pub struct WrapFactory {
    pd_client: Arc<TestPdClient>,
    raft_engine: RaftTestEngine,
    factory: Arc<dyn TabletFactory<RocksEngine> + Send + Sync>,
}

impl WrapFactory {
    pub fn new(
        pd_client: Arc<TestPdClient>,
        raft_engine: RaftTestEngine,
        factory: Arc<dyn TabletFactory<RocksEngine> + Send + Sync>,
    ) -> Self {
        Self {
            raft_engine,
            factory,
            pd_client,
        }
    }

    fn get_tablet(&self, key: &[u8]) -> Option<RocksEngine> {
        // todo: unwrap
        let region_id = self.pd_client.get_region(key).unwrap().get_id();
        self.factory
            .open_tablet(region_id, None, OpenOptions::default().set_cache_only(true))
            .ok()
    }

    pub fn get_region_state(
        &self,
        region_id: u64,
    ) -> engine_traits::Result<Option<RegionLocalState>> {
        self.raft_engine.get_region_state(region_id)
    }
}

impl Peekable for WrapFactory {
    type DbVector = RocksDbVector;

    fn get_value_opt(
        &self,
        opts: &ReadOptions,
        key: &[u8],
    ) -> engine_traits::Result<Option<Self::DbVector>> {
        match self.get_tablet(key) {
            Some(tablet) => tablet.get_value_opt(opts, key),
            _ => Ok(None),
        }
    }

    fn get_value_cf_opt(
        &self,
        opts: &ReadOptions,
        cf: &str,
        key: &[u8],
    ) -> engine_traits::Result<Option<Self::DbVector>> {
        match self.get_tablet(key) {
            Some(tablet) => tablet.get_value_cf_opt(opts, cf, key),
            _ => Ok(None),
        }
    }

    fn get_msg_cf<M: protobuf::Message + Default>(
        &self,
        cf: &str,
        key: &[u8],
    ) -> engine_traits::Result<Option<M>> {
        unimplemented!()
    }
}

impl SyncMutable for WrapFactory {
    fn put(&self, key: &[u8], value: &[u8]) -> engine_traits::Result<()> {
        match self.get_tablet(key) {
            Some(tablet) => tablet.put(key, value),
            _ => unimplemented!(),
        }
    }

    fn put_cf(&self, cf: &str, key: &[u8], value: &[u8]) -> engine_traits::Result<()> {
        match self.get_tablet(key) {
            Some(tablet) => tablet.put_cf(cf, key, value),
            _ => unimplemented!(),
        }
    }

    fn delete(&self, key: &[u8]) -> engine_traits::Result<()> {
        match self.get_tablet(key) {
            Some(tablet) => tablet.delete(key),
            _ => unimplemented!(),
        }
    }

    fn delete_cf(&self, cf: &str, key: &[u8]) -> engine_traits::Result<()> {
        match self.get_tablet(key) {
            Some(tablet) => tablet.delete_cf(cf, key),
            _ => unimplemented!(),
        }
    }

    fn delete_range(&self, begin_key: &[u8], end_key: &[u8]) -> engine_traits::Result<()> {
        unimplemented!()
    }

    fn delete_range_cf(
        &self,
        cf: &str,
        begin_key: &[u8],
        end_key: &[u8],
    ) -> engine_traits::Result<()> {
        unimplemented!()
    }
}

impl RawEngine for WrapFactory {}
