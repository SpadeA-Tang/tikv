// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use engine_traits::{OpenOptions, Peekable, TabletFactory};
use futures::executor::block_on;
use kvproto::{
    metapb,
    raft_cmdpb::{AdminCmdType, CmdType, Request, TransferLeaderRequest},
};
use raft::prelude::ConfChangeType;
use raftstore_v2::router::PeerMsg;
use tikv_util::store::new_peer;

use crate::cluster::Cluster;

fn put_data(cluster: &Cluster, node_off: usize, node_off_for_verify: usize, key: &[u8]) {
    let router = cluster.router(node_off);
    let mut req = router.new_request_for(2);
    let mut put_req = Request::default();
    put_req.set_cmd_type(CmdType::Put);
    put_req.mut_put().set_key(key.to_vec());
    put_req.mut_put().set_value(b"value".to_vec());
    req.mut_requests().push(put_req);

    router.wait_applied_to_current_term(2, Duration::from_secs(3));

    let tablet_factory = cluster.node(node_off).tablet_factory();
    let tablet = tablet_factory
        .open_tablet(2, None, OpenOptions::default().set_cache_only(true))
        .unwrap();
    assert!(tablet.get_value(key).unwrap().is_none());
    let (msg, mut sub) = PeerMsg::raft_command(req.clone());
    router.send(2, msg).unwrap();
    cluster.dispatch(2, vec![]);
    std::thread::sleep(std::time::Duration::from_millis(100));
    assert!(block_on(sub.wait_proposed()));

    std::thread::sleep(std::time::Duration::from_millis(100));
    cluster.trig_heartbeat(0, 2);
    cluster.dispatch(2, vec![]);
    // triage send snapshot
    std::thread::sleep(std::time::Duration::from_millis(1000));
    cluster.trig_heartbeat(0, 2);
    cluster.dispatch(2, vec![]);
    assert!(block_on(sub.wait_committed()));

    let resp = block_on(sub.result()).unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    assert_eq!(tablet.get_value(key).unwrap().unwrap(), b"value");
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Verify the data is ready in the other node
    cluster.trig_heartbeat(node_off, 2);
    cluster.dispatch(2, vec![]);
    let tablet_factory = cluster.node(node_off_for_verify).tablet_factory();
    let tablet = tablet_factory
        .open_tablet(2, None, OpenOptions::default().set_cache_only(true))
        .unwrap();
    assert_eq!(tablet.get_value(key).unwrap().unwrap(), b"value");
}

pub fn must_transfer_leader(
    cluster: &Cluster,
    region_id: u64,
    from_off: usize,
    to_off: usize,
    to_peer: metapb::Peer,
) {
    let router = cluster.router(from_off);
    let router2 = cluster.router(to_off);
    let mut req = router.new_request_for(region_id);
    let mut transfer_req = TransferLeaderRequest::default();
    transfer_req.set_peer(to_peer.clone());
    let admin_req = req.mut_admin_request();
    admin_req.set_cmd_type(AdminCmdType::TransferLeader);
    admin_req.set_transfer_leader(transfer_req);
    let resp = router.command(region_id, req).unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    cluster.dispatch(region_id, vec![]);

    let meta = router
        .must_query_debug_info(region_id, Duration::from_secs(3))
        .unwrap();
    assert_eq!(meta.raft_status.soft_state.leader_id, to_peer.id);
    let meta = router2
        .must_query_debug_info(region_id, Duration::from_secs(3))
        .unwrap();
    assert_eq!(meta.raft_status.soft_state.leader_id, to_peer.id);
}

#[test]
fn test_transfer_leader() {
    let cluster = Cluster::with_node_count(3, None);
    let router0 = cluster.router(0);

    let mut req = router0.new_request_for(2);
    let admin_req = req.mut_admin_request();
    admin_req.set_cmd_type(AdminCmdType::ChangePeer);
    admin_req
        .mut_change_peer()
        .set_change_type(ConfChangeType::AddNode);
    let store_id = cluster.node(1).id();
    let peer1 = new_peer(store_id, 10);
    admin_req.mut_change_peer().set_peer(peer1.clone());
    let req_clone = req.clone();
    let resp = router0.command(2, req_clone).unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    let epoch = req.get_header().get_region_epoch();
    let new_conf_ver = epoch.get_conf_ver() + 1;
    let leader_peer = req.get_header().get_peer().clone();
    let meta = router0
        .must_query_debug_info(2, Duration::from_secs(3))
        .unwrap();
    assert_eq!(meta.region_state.epoch.version, epoch.get_version());
    assert_eq!(meta.region_state.epoch.conf_ver, new_conf_ver);
    assert_eq!(meta.region_state.peers, vec![leader_peer, peer1.clone()]);
    let peer0_id = meta.raft_status.id;

    cluster.dispatch(2, vec![]);
    std::thread::sleep(std::time::Duration::from_millis(20));
    let router1 = cluster.router(1);
    let meta = router1
        .must_query_debug_info(2, Duration::from_secs(3))
        .unwrap();
    assert_eq!(peer0_id, meta.raft_status.soft_state.leader_id);
    assert_eq!(meta.raft_status.id, peer1.id, "{:?}", meta);
    assert_eq!(meta.region_state.epoch.version, epoch.get_version());
    assert_eq!(meta.region_state.epoch.conf_ver, new_conf_ver);

    // Now, raft group have two peers.

    // Ensure follower has latest entries before transfer leader.
    put_data(&cluster, 0, 1, b"key1");

    // Perform transfer leader
    must_transfer_leader(&cluster, 2, 0, 1, peer1);

    // Before transfer back to peer0, put some data again.
    put_data(&cluster, 1, 0, b"key2");

    // Perform transfer leader
    let store_id = cluster.node(0).id();
    must_transfer_leader(&cluster, 2, 1, 0, new_peer(store_id, peer0_id));
}
