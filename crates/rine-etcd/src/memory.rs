//! In-memory backend.
//!
//! Ordered key maps support range scans; an event history supports historical
//! reads and watch replay.
//! Events carry full before/after snapshots so Watch responses include the
//! metadata (create/mod revision, version, lease) etcd clients expect.

use std::{
    collections::BTreeMap,
    ops::Bound::{Excluded, Included, Unbounded},
    sync::Arc,
};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::{RwLock, broadcast};

use crate::{
    backend::{
        Backend, BackendFactory, Event, KeyValue, LeaseId, ListOptions, ObservedRange, ReadSet,
        Revision, in_range,
    },
    error::{Error, Result},
};

struct Inner {
    rev: Revision,
    compact_rev: Revision,
    kvs: BTreeMap<Bytes, KeyValue>,
    compacted: BTreeMap<Bytes, KeyValue>,
    /// revision -> events at that revision (Txn may emit several)
    history: Vec<Vec<Event>>,
}

pub struct MemoryBackend {
    inner: Arc<RwLock<Inner>>,
    tx: broadcast::Sender<Vec<Event>>,
}

/// Factory for the in-process [`MemoryBackend`]; takes no configuration.
pub struct MemoryFactory;

impl BackendFactory for MemoryFactory {
    fn name(&self) -> &'static str {
        "memory"
    }

    fn build(&self) -> anyhow::Result<Arc<dyn Backend>> {
        Ok(Arc::new(MemoryBackend::new()))
    }
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
                kvs: BTreeMap::new(),
                compacted: BTreeMap::new(),
                history: vec![Vec::new()],
            })),
            tx,
        }
    }

    fn next_rev(inner: &mut Inner) -> Revision {
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

    fn check_range(g: &Inner, range: &ObservedRange) -> Result<()> {
        let mut opts = range.opts.clone();
        opts.revision = 0;
        let (_, kvs, count) =
            Self::list_locked(g, &range.prefix, range.range_end.as_deref(), opts)?;
        if count != range.count
            || kvs.len() != range.observed.len()
            || kvs
                .iter()
                .zip(range.observed.iter())
                .any(|(kv, (key, rev))| kv.key.as_ref() != key.as_ref() || kv.mod_revision != *rev)
        {
            return Err(Error::Conflict);
        }
        Ok(())
    }

    #[allow(clippy::type_complexity)]
    fn list_locked(
        g: &Inner,
        prefix: &[u8],
        range_end: Option<&[u8]>,
        opts: ListOptions,
    ) -> Result<(Revision, Vec<KeyValue>, i64)> {
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
        let upper = match range_end {
            None | Some([]) => Included(prefix),
            Some([0]) => Unbounded,
            Some(end) if end > prefix => Excluded(end),
            Some(_) => return Ok((rev, Vec::new(), 0)),
        };
        let bounds = (Included(prefix), upper);
        let mut snapshot = BTreeMap::new();
        let values = if opts.revision <= 0 || opts.revision == rev {
            &g.kvs
        } else {
            snapshot.extend(
                g.compacted
                    .range::<[u8], _>(bounds)
                    .map(|(k, v)| (k.clone(), v.clone())),
            );
            for events in &g.history[(g.compact_rev + 1) as usize..=opts.revision as usize] {
                for event in events
                    .iter()
                    .filter(|e| in_range(&e.key, prefix, range_end))
                {
                    if let Some(kv) = &event.kv {
                        snapshot.insert(event.key.clone(), kv.clone());
                    } else {
                        snapshot.remove(event.key.as_ref());
                    }
                }
            }
            &snapshot
        };
        let project = |kv: &KeyValue| {
            if opts.keys_only {
                KeyValue {
                    value: Bytes::new(),
                    ..kv.clone()
                }
            } else {
                kv.clone()
            }
        };
        if matches!(range_end, None | Some([])) {
            let kv = values.get(prefix);
            let out = if opts.count_only {
                Vec::new()
            } else {
                kv.into_iter().map(project).collect()
            };
            return Ok((rev, out, i64::from(kv.is_some())));
        }
        let mut rows = values.range::<[u8], _>(bounds);
        if opts.count_only {
            return Ok((rev, Vec::new(), rows.count() as i64));
        }
        let limit = if opts.limit > 0 {
            usize::try_from(opts.limit).unwrap_or(usize::MAX)
        } else {
            usize::MAX
        };
        let out: Vec<_> = rows
            .by_ref()
            .take(limit)
            .map(|(_, kv)| project(kv))
            .collect();
        let count = (out.len() + rows.count()) as i64;
        Ok((rev, out, count))
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
        let r = Self::next_rev(&mut g);
        let kv = KeyValue {
            key: key.clone(),
            value,
            create_revision: r,
            mod_revision: r,
            version: 1,
            lease,
        };
        g.kvs.insert(key.clone(), kv.clone());
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
                let r = Self::next_rev(&mut g);
                let kv = KeyValue {
                    key: key.clone(),
                    value,
                    create_revision: p.create_revision,
                    mod_revision: r,
                    version: p.version + 1,
                    lease,
                };
                g.kvs.insert(key.clone(), kv.clone());
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
        let r = Self::next_rev(&mut g);
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

    async fn commit(&self, reads: ReadSet, events: Vec<Event>) -> Result<Revision> {
        let mut g = self.inner.write().await;
        for (key, expected) in &reads.points {
            let current = g.kvs.get(key.as_ref()).map(|kv| kv.mod_revision);
            if current != *expected {
                return Err(Error::Conflict);
            }
        }
        for range in &reads.ranges {
            Self::check_range(&g, range)?;
        }
        if events.is_empty() {
            return Ok(g.rev);
        }
        let fresh = g.rev + 1;
        let mut rebased = Vec::with_capacity(events.len());
        for mut event in events {
            let staged = event.revision;
            event.revision = fresh;
            if let Some(kv) = event.kv.as_mut() {
                kv.mod_revision = fresh;
                if kv.create_revision == staged {
                    kv.create_revision = fresh;
                }
                g.kvs.insert(event.key.clone(), kv.clone());
            } else {
                g.kvs.remove(event.key.as_ref());
            }
            rebased.push(event);
        }
        g.rev = fresh;
        if g.history.len() <= fresh as usize {
            g.history.resize(fresh as usize + 1, Vec::new());
        }
        Self::publish(&mut g, &self.tx, rebased.clone());
        Ok(fresh)
    }

    fn commit_is_cancel_safe(&self) -> bool {
        true
    }

    async fn list(
        &self,
        prefix: &[u8],
        range_end: Option<&[u8]>,
        opts: ListOptions,
    ) -> Result<(Revision, Vec<KeyValue>, i64)> {
        let g = self.inner.read().await;
        Self::list_locked(&g, prefix, range_end, opts)
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
                        g.compacted.insert(event.key, kv);
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
    async fn ordered_ranges_preserve_etcd_bounds() {
        let b = MemoryBackend::new();
        for key in [b"\xff".as_slice(), b"a\0", b"b", b"a"] {
            b.create(Bytes::copy_from_slice(key), Bytes::from_static(b"value"), 0)
                .await
                .unwrap();
        }
        for (start, end, expected) in [
            (b"a".as_slice(), None, vec![b"a".as_slice()]),
            (b"a", Some(b"".as_slice()), vec![b"a".as_slice()]),
            (b"a", Some(b"b".as_slice()), vec![b"a".as_slice(), b"a\0"]),
            (b"b", Some(b"\0".as_slice()), vec![b"b".as_slice(), b"\xff"]),
            (b"b", Some(b"b".as_slice()), vec![]),
            (b"b", Some(b"a".as_slice()), vec![]),
        ] {
            let (_, kvs, count) = b.list(start, end, ListOptions::default()).await.unwrap();
            let keys: Vec<_> = kvs.iter().map(|kv| kv.key.as_ref()).collect();
            assert_eq!(keys, expected);
            assert_eq!(count as usize, expected.len());
        }
    }

    #[tokio::test]
    async fn historical_range_keeps_order_and_count_with_limit() {
        let b = MemoryBackend::new();
        b.create(Bytes::from_static(b"a"), Bytes::from_static(b"old"), 0)
            .await
            .unwrap();
        let point = b
            .create(Bytes::from_static(b"b"), Bytes::from_static(b"other"), 0)
            .await
            .unwrap();
        b.compact(point).await.unwrap();
        b.delete(b"a").await.unwrap();
        let (_, kvs, count) = b
            .list(
                b"a",
                Some(b"c"),
                ListOptions {
                    revision: point,
                    limit: 1,
                    keys_only: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(count, 2);
        assert_eq!(kvs.len(), 1);
        assert_eq!(kvs[0].key.as_ref(), b"a");
        assert!(kvs[0].value.is_empty());
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
