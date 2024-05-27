// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Write},
    str::FromStr,
    sync::{Arc, Mutex},
};

use online_config::{ConfigChange, OnlineConfig};
use raftstore::store::Config as RaftstoreConfig;
use tikv::config::*;
use tikv_util::config::{ReadableOffsetTime, ReadableSchedule, ReadableSize};

fn change(name: &str, value: &str) -> HashMap<String, String> {
    let mut m = HashMap::new();
    m.insert(name.to_owned(), value.to_owned());
    m
}

#[test]
fn test_update_config() {
    let (mut cfg, _dir) = TikvConfig::with_tmp().unwrap();
    cfg.validate().unwrap();
    let cfg_controller = ConfigController::new(cfg);
    let mut cfg = cfg_controller.get_current();

    cfg_controller
        .update(change(
            "raftstore.periodic-full-compact-start-times",
            "[\"12:00 +0800\",\"14:00 +0800\"]",
        ))
        .unwrap();
    cfg.raft_store.periodic_full_compact_start_times = ReadableSchedule(vec![
        ReadableOffsetTime::from_str("12:00 +0800").unwrap(),
        ReadableOffsetTime::from_str("14:00 +0800").unwrap(),
    ]);
    assert_eq!(cfg_controller.get_current(), cfg);

    cfg_controller
        .update(change(
            "raftstore.periodic-full-compact-start-times",
            "[\"12:00\",\"14:00\"]",
        ))
        .unwrap();
    cfg.raft_store.periodic_full_compact_start_times = ReadableSchedule(vec![
        ReadableOffsetTime::from_str("12:00").unwrap(),
        ReadableOffsetTime::from_str("14:00").unwrap(),
    ]);

    // normal update
    cfg_controller
        .update(change("raftstore.raft-log-gc-threshold", "2000"))
        .unwrap();
    cfg.raft_store.raft_log_gc_threshold = 2000;
    assert_eq!(cfg_controller.get_current(), cfg);

    let mut range_cache_config_change = HashMap::new();
    range_cache_config_change.insert("range_cache_engine.enabled".to_owned(), "true".to_owned());
    range_cache_config_change.insert(
        "range_cache_engine.soft-limit-threshold".to_owned(),
        "10GB".to_owned(),
    );
    range_cache_config_change.insert(
        "range_cache_engine.hard-limit-threshold".to_owned(),
        "15GB".to_owned(),
    );
    cfg_controller.update(range_cache_config_change).unwrap();
    cfg.range_cache_engine.enabled = true;
    cfg.range_cache_engine.soft_limit_threshold = Some(ReadableSize::gb(10));
    cfg.range_cache_engine.hard_limit_threshold = Some(ReadableSize::gb(15));
    assert_eq!(cfg_controller.get_current(), cfg);

    cfg_controller
        .update(change("range_cache_engine.soft-limit-threshold", "11GB"))
        .unwrap();
    cfg.range_cache_engine.soft_limit_threshold = Some(ReadableSize::gb(11));
    assert_eq!(cfg_controller.get_current(), cfg);

    // update not support config
    let res = cfg_controller.update(change("server.addr", "localhost:3000"));
    res.unwrap_err();
    assert_eq!(cfg_controller.get_current(), cfg);

    // update to invalid config
    let res = cfg_controller.update(change("raftstore.raft-log-gc-threshold", "0"));
    res.unwrap_err();
    assert_eq!(cfg_controller.get_current(), cfg);

    // bad update request
    let res = cfg_controller.update(change("xxx.yyy", "0"));
    res.unwrap_err();
    let res = cfg_controller.update(change("raftstore.xxx", "0"));
    res.unwrap_err();
    let res = cfg_controller.update(change("raftstore.raft-log-gc-threshold", "10MB"));
    res.unwrap_err();
    let res = cfg_controller.update(change("raft-log-gc-threshold", "10MB"));
    res.unwrap_err();
    assert_eq!(cfg_controller.get_current(), cfg);
}

#[test]
fn test_dispatch_change() {
    use std::{error::Error, result::Result};

    use online_config::ConfigManager;

    #[derive(Clone)]
    struct CfgManager(Arc<Mutex<RaftstoreConfig>>);

    impl ConfigManager for CfgManager {
        fn dispatch(&mut self, c: ConfigChange) -> Result<(), Box<dyn Error>> {
            self.0.lock().unwrap().update(c)
        }
    }

    let (mut cfg, _dir) = TikvConfig::with_tmp().unwrap();
    cfg.validate().unwrap();
    let cfg_controller = ConfigController::new(cfg);
    let mut cfg = cfg_controller.get_current();
    let mgr = CfgManager(Arc::new(Mutex::new(cfg.raft_store.clone())));
    cfg_controller.register(Module::Raftstore, Box::new(mgr.clone()));

    cfg_controller
        .update(change("raftstore.raft-log-gc-threshold", "2000"))
        .unwrap();

    // config update
    cfg.raft_store.raft_log_gc_threshold = 2000;
    assert_eq!(cfg_controller.get_current(), cfg);

    // config change should also dispatch to raftstore config manager
    assert_eq!(mgr.0.lock().unwrap().raft_log_gc_threshold, 2000);
}

#[test]
fn test_write_update_to_file() {
    let (mut cfg, tmp_dir) = TikvConfig::with_tmp().unwrap();
    cfg.cfg_path = tmp_dir.path().join("cfg_file").to_str().unwrap().to_owned();
    {
        let c = r#"
## comment should be reserve
[raftstore]

# config that comment out by one `#` should be update in place
## pd-heartbeat-tick-interval = "30s"
# pd-heartbeat-tick-interval = "30s"

[rocksdb.defaultcf]
## config should be update in place
block-cache-size = "10GB"

[rocksdb.lockcf]
## this config will not update even it has the same last 
## name as `rocksdb.defaultcf.block-cache-size`
block-cache-size = "512MB"

[coprocessor]
## the update to `coprocessor.region-split-keys`, which do not show up 
## as key-value pair after [coprocessor], will be written at the end of [coprocessor]

[gc]
## config should be update in place
max-write-bytes-per-sec = "1KB"

[rocksdb.defaultcf.titan]
blob-run-mode = "normal"
"#;
        let mut f = File::create(&cfg.cfg_path).unwrap();
        f.write_all(c.as_bytes()).unwrap();
        f.sync_all().unwrap();
    }
    let cfg_controller = ConfigController::new(cfg);
    let change = {
        let mut change = HashMap::new();
        change.insert(
            "raftstore.pd-heartbeat-tick-interval".to_owned(),
            "1h".to_owned(),
        );
        change.insert(
            "coprocessor.region-split-keys".to_owned(),
            "10000".to_owned(),
        );
        change.insert("gc.max-write-bytes-per-sec".to_owned(), "100MB".to_owned());
        change.insert(
            "rocksdb.defaultcf.block-cache-size".to_owned(),
            "1GB".to_owned(),
        );
        change.insert(
            "rocksdb.defaultcf.titan.blob-run-mode".to_owned(),
            "read-only".to_owned(),
        );
        change
    };
    cfg_controller.update(change).unwrap();
    let res = {
        let mut buf = Vec::new();
        let mut f = File::open(cfg_controller.get_current().cfg_path).unwrap();
        f.read_to_end(&mut buf).unwrap();
        buf
    };

    let expect = r#"
## comment should be reserve
[raftstore]

# config that comment out by one `#` should be update in place
## pd-heartbeat-tick-interval = "30s"
pd-heartbeat-tick-interval = "1h"

[rocksdb.defaultcf]
## config should be update in place
block-cache-size = "1GB"

[rocksdb.lockcf]
## this config will not update even it has the same last 
## name as `rocksdb.defaultcf.block-cache-size`
block-cache-size = "512MB"

[coprocessor]
## the update to `coprocessor.region-split-keys`, which do not show up 
## as key-value pair after [coprocessor], will be written at the end of [coprocessor]

region-split-keys = 10000
[gc]
## config should be update in place
max-write-bytes-per-sec = "100MB"

[rocksdb.defaultcf.titan]
blob-run-mode = "read-only"
"#;
    assert_eq!(expect.as_bytes(), res.as_slice());
}

#[test]
fn test_update_from_toml_file() {
    use std::{error::Error, result::Result};

    use online_config::ConfigManager;

    #[derive(Clone)]
    struct CfgManager(Arc<Mutex<RaftstoreConfig>>);

    impl ConfigManager for CfgManager {
        fn dispatch(&mut self, c: ConfigChange) -> Result<(), Box<dyn Error>> {
            self.0.lock().unwrap().update(c)
        }
    }

    let (cfg, _dir) = TikvConfig::with_tmp().unwrap();
    let cfg_controller = ConfigController::new(cfg);
    let cfg = cfg_controller.get_current();
    let mgr = CfgManager(Arc::new(Mutex::new(cfg.raft_store.clone())));
    cfg_controller.register(Module::Raftstore, Box::new(mgr));

    // update config file
    let c = r#"
[raftstore]
raft-log-gc-threshold = 2000
"#;
    let mut f = File::create(&cfg.cfg_path).unwrap();
    f.write_all(c.as_bytes()).unwrap();
    // before update this configuration item should be the default value
    assert_eq!(
        cfg_controller
            .get_current()
            .raft_store
            .raft_log_gc_threshold,
        50
    );
    // config update from config file
    cfg_controller.update_from_toml_file().unwrap();
    // after update this configration item should be constant with the modified
    // configuration file
    assert_eq!(
        cfg_controller
            .get_current()
            .raft_store
            .raft_log_gc_threshold,
        2000
    );
}
