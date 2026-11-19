//! In-memory backend.
//!
//! Latest values and an event history support historical reads and watch replay.
//! Events carry full before/after snapshots so Watch responses include the
//! metadata (create/mod revision, version, lease) etcd clients expect.

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::{RwLock, broadcast};

use crate::{
    backend::{Backend, Event, KeyValue, LeaseId, ListOptions, Revision, in_range},
    error::{Error, Result},
};

struct Inner {
    rev: Revision,
    compact_rev: Revision,
    kvs: HashMap<Vec<u8>, KeyValue>,
    compacted: HashMap<Vec<u8>, KeyValue>,
    /// revision -> events at that revision (Txn may emit several)
    history: Vec<Vec<Event>>,
}

pub struct MemoryBackend {
    inner: Arc<RwLock<Inner>>,
    tx: broadcast::Sender<Vec<Event>>,
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
                compacted: HashMap::new(),
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

    fn publish(g: &mut Inner, tx: &broadcast::Sender<Vec<Event>>, events: Vec<Event>) {
        g.history[g.rev as usize].extend(events.iter().cloned());
        let _ = tx.send(events);
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

    async fn initialize(&self) -> Result<()> {
        Ok(())
    }

    async fn get(&self, key: &[u8], revision: Revision) -> Result<(Revision, Option<KeyValue>)> {
        let g = self.inner.read().await;
        if revision > 0 && revision < g.compact_rev {
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
        if revision <= 0 || revision == g.rev {
            return Ok((g.rev, g.kvs.get(key).cloned()));
        }
        // Historical read: last snapshot at or below the requested revision.
        let mut cur = g.compacted.get(key).cloned();
        for r in (g.compact_rev + 1)..=revision {
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
        Self::publish(
            &mut g,
            &self.tx,
            vec![Event {
                revision: r,
                key,
                deleted: false,
                kv: Some(kv),
                prev_kv: None,
            }],
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
                Self::publish(
                    &mut g,
                    &self.tx,
                    vec![Event {
                        revision: r,
                        key,
                        deleted: false,
                        kv: Some(kv),
                        prev_kv: Some(p),
                    }],
                );
                Ok(r)
            }
        }
    }

    async fn delete(&self, key: &[u8]) -> Result<(Revision, Option<KeyValue>)> {
        let mut g = self.inner.write().await;
        let prev = g.kvs.remove(key);
        if prev.is_none() {
            return Ok((g.rev, None));
        }
        let r = Self::next_rev(&mut g).await;
        Self::publish(
            &mut g,
            &self.tx,
            vec![Event {
                revision: r,
                key: Bytes::copy_from_slice(key),
                deleted: true,
                kv: None,
                prev_kv: prev.clone(),
            }],
        );
        Ok((r, prev))
    }

    async fn commit(&self, base: Revision, events: Vec<Event>) -> Result<()> {
        let mut g = self.inner.write().await;
        if g.rev != base {
            return Err(Error::Conflict);
        }
        if !events.is_empty() {
            Self::next_rev(&mut g).await;
            for event in &events {
                if let Some(kv) = &event.kv {
                    g.kvs.insert(event.key.to_vec(), kv.clone());
                } else {
                    g.kvs.remove(event.key.as_ref());
                }
            }
            Self::publish(&mut g, &self.tx, events);
        }
        Ok(())
    }

    async fn list(
        &self,
        prefix: &[u8],
        range_end: Option<&[u8]>,
        opts: ListOptions,
    ) -> Result<(Revision, Vec<KeyValue>, i64)> {
        let g = self.inner.read().await;
        if opts.revision > 0 && opts.revision < g.compact_rev {
            return Err(Error::Compacted {
                requested: opts.revision,
                compacted: g.compact_rev,
            });
        }
        let rev = g.rev;
        if opts.revision > rev {
            return Err(Error::FutureRev {
                requested: opts.revision,
                current: rev,
            });
        }
        let mut snapshot = HashMap::new();
        let values = if opts.revision <= 0 || opts.revision == rev {
            &g.kvs
        } else {
            snapshot.extend(
                g.compacted
                    .iter()
                    .filter(|(k, _)| in_range(k, prefix, range_end))
                    .map(|(k, v)| (k.clone(), v.clone())),
            );
            for events in &g.history[(g.compact_rev + 1) as usize..=opts.revision as usize] {
                for event in events
                    .iter()
                    .filter(|e| in_range(&e.key, prefix, range_end))
                {
                    if let Some(kv) = &event.kv {
                        snapshot.insert(event.key.to_vec(), kv.clone());
                    } else {
                        snapshot.remove(event.key.as_ref());
                    }
                }
            }
            &snapshot
        };
        let mut out: Vec<KeyValue> = values
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
        if revision <= g.compact_rev {
            return Err(Error::Compacted {
                requested: revision,
                compacted: g.compact_rev,
            });
        }
        for r in (g.compact_rev + 1)..=revision {
            if (r as usize) < g.history.len() {
                for event in std::mem::take(&mut g.history[r as usize]) {
                    if let Some(kv) = event.kv {
                        g.compacted.insert(event.key.to_vec(), kv);
                    } else {
                        g.compacted.remove(event.key.as_ref());
                    }
                }
            }
        }
        g.compact_rev = revision;
        Ok(g.rev)
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

    fn subscribe(&self) -> broadcast::Receiver<Vec<Event>> {
        self.tx.subscribe()
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
