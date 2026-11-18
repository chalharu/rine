//! In-memory HashTable backend (step 2 target).
//!
//! Single-revision-per-key map + append-only event history for watch replay,
//! mirroring kine `logstructured` MVCC semantics on top of a HashMap.
//! Events carry full before/after snapshots so Watch responses include the
//! metadata (create/mod revision, version, lease) etcd clients expect.

use crate::backend::{Backend, Event, KeyValue, LeaseId, ListOptions, Revision};
use crate::error::{Error, Result};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};

struct Inner {
    rev: Revision,
    compact_rev: Revision,
    kvs: HashMap<Vec<u8>, KeyValue>,
    /// revision -> events at that revision (Txn may emit several)
    history: Vec<Vec<Event>>,
}

pub struct MemoryBackend {
    inner: Arc<RwLock<Inner>>,
    tx: broadcast::Sender<Event>,
}

impl Default for MemoryBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryBackend {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(1024);
        Self {
            inner: Arc::new(RwLock::new(Inner {
                rev: 1,
                compact_rev: 0,
                kvs: HashMap::new(),
                history: vec![Vec::new()],
            })),
            tx,
        }
    }

    async fn next_rev(inner: &mut Inner) -> Revision {
        inner.rev += 1;
        let r = inner.rev;
        if inner.history.len() <= r as usize {
            inner.history.resize(r as usize + 1, Vec::new());
        }
        r
    }

    fn push(g: &mut Inner, tx: &broadcast::Sender<Event>, ev: Event) {
        g.history[ev.revision as usize].push(ev.clone());
        let _ = tx.send(ev);
    }
}

#[async_trait]
impl Backend for MemoryBackend {
    async fn current_revision(&self) -> Revision {
        self.inner.read().await.rev
    }

    async fn compacted_revision(&self) -> Revision {
        self.inner.read().await.compact_rev
    }

    async fn get(&self, key: &[u8], revision: Revision) -> Result<(Revision, Option<KeyValue>)> {
        let g = self.inner.read().await;
        if revision != 0 && revision < g.compact_rev {
            return Err(Error::Compacted {
                requested: revision,
                compacted: g.compact_rev,
            });
        }
        if revision != 0 && revision > g.rev {
            return Err(Error::FutureRev {
                requested: revision,
                current: g.rev,
            });
        }
        if revision == 0 || revision == g.rev {
            return Ok((g.rev, g.kvs.get(key).cloned()));
        }
        // Historical read: last snapshot at or below the requested revision.
        let mut cur: Option<KeyValue> = None;
        for r in 1..=revision {
            if let Some(evs) = g.history.get(r as usize) {
                for e in evs {
                    if e.key.as_ref() == key {
                        cur = e.kv.clone();
                    }
                }
            }
        }
        Ok((g.rev, cur))
    }

    async fn create(&self, key: Bytes, value: Bytes, lease: LeaseId) -> Result<Revision> {
        let mut g = self.inner.write().await;
        if g.kvs.contains_key(key.as_ref()) {
            return Err(Error::KeyExists(key.to_vec()));
        }
        let r = Self::next_rev(&mut g).await;
        let kv = KeyValue {
            key: key.clone(),
            value,
            create_revision: r,
            mod_revision: r,
            version: 1,
            lease,
        };
        g.kvs.insert(key.to_vec(), kv.clone());
        Self::push(
            &mut g,
            &self.tx,
            Event {
                revision: r,
                key,
                deleted: false,
                kv: Some(kv),
                prev_kv: None,
            },
        );
        Ok(r)
    }

    async fn update(
        &self,
        key: Bytes,
        value: Bytes,
        expected_mod_revision: Revision,
        lease: LeaseId,
    ) -> Result<Revision> {
        let mut g = self.inner.write().await;
        let prev = g.kvs.get(key.as_ref()).cloned();
        match prev {
            None => Err(Error::KeyExists(key.to_vec())),
            Some(p) => {
                if expected_mod_revision != 0 && p.mod_revision != expected_mod_revision {
                    return Err(Error::Backend("mod revision mismatch".into()));
                }
                let r = Self::next_rev(&mut g).await;
                let kv = KeyValue {
                    key: key.clone(),
                    value,
                    create_revision: p.create_revision,
                    mod_revision: r,
                    version: p.version + 1,
                    lease,
                };
                g.kvs.insert(key.to_vec(), kv.clone());
                Self::push(
                    &mut g,
                    &self.tx,
                    Event {
                        revision: r,
                        key,
                        deleted: false,
                        kv: Some(kv),
                        prev_kv: Some(p),
                    },
                );
                Ok(r)
            }
        }
    }

    async fn delete(&self, key: &[u8]) -> Result<(Revision, Option<KeyValue>)> {
        let mut g = self.inner.write().await;
        let prev = g.kvs.remove(key);
        let r = Self::next_rev(&mut g).await;
        Self::push(
            &mut g,
            &self.tx,
            Event {
                revision: r,
                key: Bytes::copy_from_slice(key),
                deleted: true,
                kv: None,
                prev_kv: prev.clone(),
            },
        );
        Ok((r, prev))
    }

    async fn list(
        &self,
        prefix: &[u8],
        range_end: Option<&[u8]>,
        opts: ListOptions,
    ) -> Result<(Revision, Vec<KeyValue>, i64)> {
        let g = self.inner.read().await;
        if opts.revision != 0 && opts.revision < g.compact_rev {
            return Err(Error::Compacted {
                requested: opts.revision,
                compacted: g.compact_rev,
            });
        }
        let rev = g.rev;
        let mut out: Vec<KeyValue> = g
            .kvs
            .values()
            .filter(|kv| in_range(&kv.key, prefix, range_end))
            .cloned()
            .collect();
        out.sort_by(|a, b| a.key.cmp(&b.key));
        let count = out.len() as i64;
        if opts.count_only {
            return Ok((rev, Vec::new(), count));
        }
        if opts.limit > 0 && (out.len() as i64) > opts.limit {
            out.truncate(opts.limit as usize);
        }
        if opts.keys_only {
            for kv in &mut out {
                kv.value = Bytes::new();
            }
        }
        Ok((rev, out, count))
    }

    async fn compact(&self, revision: Revision) -> Result<Revision> {
        let mut g = self.inner.write().await;
        if revision > g.rev {
            return Err(Error::FutureRev {
                requested: revision,
                current: g.rev,
            });
        }
        if revision > g.compact_rev {
            g.compact_rev = revision;
            for r in 1..=revision.min(g.history.len() as i64 - 1) {
                g.history[r as usize].clear();
            }
        }
        Ok(revision)
    }

    async fn db_size(&self) -> i64 {
        let g = self.inner.read().await;
        g.kvs
            .iter()
            .map(|(k, v)| (k.len() + v.value.len()) as i64)
            .sum()
    }

    async fn events(&self, from: Revision, to: Revision) -> Result<Vec<Event>> {
        let g = self.inner.read().await;
        if from <= g.compact_rev {
            return Err(Error::Compacted {
                requested: from,
                compacted: g.compact_rev,
            });
        }
        let mut out = Vec::new();
        let hi = to.min(g.rev);
        for r in from.max(1)..=hi {
            if let Some(evs) = g.history.get(r as usize) {
                out.extend(evs.iter().cloned());
            }
        }
        Ok(out)
    }

    fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.tx.subscribe()
    }
}

fn in_range(key: &[u8], prefix: &[u8], range_end: Option<&[u8]>) -> bool {
    match range_end {
        // proto3 cannot distinguish absent from empty: single key.
        None | Some([]) => key == prefix,
        // "\0" means all keys >= key (etcd convention).
        Some([0]) => key >= prefix,
        Some(end) => key >= prefix && key < end,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn crud() {
        let b = MemoryBackend::new();
        let r1 = b
            .create(Bytes::from("a"), Bytes::from("1"), 0)
            .await
            .unwrap();
        assert!(r1 > 1);
        let (_, kv) = b.get(b"a", 0).await.unwrap();
        assert_eq!(kv.unwrap().value, Bytes::from("1"));
        b.update(Bytes::from("a"), Bytes::from("2"), 0, 0)
            .await
            .unwrap();
        let (rev, kvs, count) = b.list(b"a", None, ListOptions::default()).await.unwrap();
        assert_eq!(count, 1);
        assert_eq!(kvs[0].value, Bytes::from("2"));
        // prefix convention checks
        let (_, kvs_all, _) = b
            .list(b"a", Some(&[0u8]), ListOptions::default())
            .await
            .unwrap();
        assert_eq!(kvs_all.len(), 1);
        let _ = rev;
        b.delete(b"a").await.unwrap();
        let (_, kv) = b.get(b"a", 0).await.unwrap();
        assert!(kv.is_none());
    }

    #[tokio::test]
    async fn history_and_compact() {
        let b = MemoryBackend::new();
        b.create(Bytes::from("k"), Bytes::from("v1"), 0)
            .await
            .unwrap();
        let r2 = b
            .update(Bytes::from("k"), Bytes::from("v2"), 0, 0)
            .await
            .unwrap();
        let (_, kv) = b.get(b"k", r2 - 1).await.unwrap();
        assert_eq!(kv.unwrap().value, Bytes::from("v1"));
        b.compact(r2).await.unwrap();
        assert!(b.get(b"k", r2 - 1).await.is_err());
        assert!(b.events(r2 - 1, r2).await.is_err());
    }
}
