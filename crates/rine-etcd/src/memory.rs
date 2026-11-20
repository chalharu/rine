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
        BROADCAST_CAP, Backend, BackendFactory, Event, KeyValue, ListOptions, ObservedRange,
        ReadSet, Revision, in_range,
    },
    error::{Error, Result},
};

struct Inner {
    rev: Revision,
    compact_rev: Revision,
    kvs: BTreeMap<Bytes, KeyValue>,
    compacted: BTreeMap<Bytes, KeyValue>,
    /// revision -> events at that revision (Txn may emit several).
    /// Map (not a Vec) so `compact` frees history instead of leaking
    /// empty slots as the revision grows.
    history: BTreeMap<Revision, Vec<Event>>,
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
        let (tx, _) = broadcast::channel(BROADCAST_CAP);
        Self {
            inner: Arc::new(RwLock::new(Inner {
                rev: 1,
                compact_rev: 0,
                kvs: BTreeMap::new(),
                compacted: BTreeMap::new(),
                history: BTreeMap::new(),
            })),
            tx,
        }
    }

    fn publish(g: &mut Inner, tx: &broadcast::Sender<Vec<Event>>, events: Vec<Event>) {
        g.history
            .entry(g.rev)
            .or_default()
            .extend(events.iter().cloned());
        let _ = tx.send(events);
    }

    fn check_range(g: &Inner, range: &ObservedRange) -> Result<()> {
        let mut opts = range.opts.clone();
        opts.revision = 0;
        let (_, kvs, count) =
            Self::list_locked(g, &range.prefix, range.range_end.as_deref(), opts)?;
        let snapshot: Vec<(Bytes, Revision)> = kvs
            .iter()
            .map(|kv| (kv.key.clone(), kv.mod_revision))
            .collect();
        if !range.matches_snapshot(&snapshot, count) {
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
            // Replay post-compaction history; empty when reading exactly at
            // the compaction revision (the range would start past its end).
            if g.compact_rev < opts.revision {
                for events in g
                    .history
                    .range((g.compact_rev + 1)..=opts.revision)
                    .map(|(_, v)| v)
                {
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
        Self::publish(&mut g, &self.tx, rebased);
        Ok(fresh)
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
            if let Some(events) = g.history.remove(&r) {
                for event in events {
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
        for (_, evs) in g.history.range(from.max(1)..=hi) {
            out.extend(evs.iter().cloned());
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

    /// Test-only single-key writes over `commit`.
    async fn put(b: &MemoryBackend, key: &[u8], value: &[u8]) -> Revision {
        let key = Bytes::copy_from_slice(key);
        let revision = b.current_revision().await + 1;
        b.commit(
            ReadSet {
                points: vec![(key.clone(), None)],
                ranges: Vec::new(),
            },
            vec![Event {
                revision,
                key: key.clone(),
                deleted: false,
                kv: Some(KeyValue {
                    key,
                    value: Bytes::copy_from_slice(value),
                    create_revision: revision,
                    mod_revision: revision,
                    version: 1,
                    lease: 0,
                }),
                prev_kv: None,
            }],
        )
        .await
        .unwrap()
    }

    async fn update(b: &MemoryBackend, key: &[u8], value: &[u8]) -> Revision {
        let (_, prev) = get(b, key, 0).await;
        let prev = prev.unwrap();
        let key = Bytes::copy_from_slice(key);
        let revision = b.current_revision().await + 1;
        b.commit(
            ReadSet {
                points: vec![(key.clone(), Some(prev.mod_revision))],
                ranges: Vec::new(),
            },
            vec![Event {
                revision,
                key: key.clone(),
                deleted: false,
                kv: Some(KeyValue {
                    key,
                    value: Bytes::copy_from_slice(value),
                    create_revision: prev.create_revision,
                    mod_revision: revision,
                    version: prev.version + 1,
                    lease: 0,
                }),
                prev_kv: Some(prev),
            }],
        )
        .await
        .unwrap()
    }

    async fn del(b: &MemoryBackend, key: &[u8]) {
        let (_, prev) = get(b, key, 0).await;
        if let Some(prev) = prev {
            let key = Bytes::copy_from_slice(key);
            let revision = b.current_revision().await + 1;
            b.commit(
                ReadSet {
                    points: vec![(key.clone(), Some(prev.mod_revision))],
                    ranges: Vec::new(),
                },
                vec![Event {
                    revision,
                    key,
                    deleted: true,
                    kv: None,
                    prev_kv: Some(prev),
                }],
            )
            .await
            .unwrap();
        }
    }

    async fn get(b: &MemoryBackend, key: &[u8], rev: Revision) -> (Revision, Option<KeyValue>) {
        let (cur, rows, _) = b
            .list(
                key,
                None,
                ListOptions {
                    revision: rev,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        (cur, rows.into_iter().next())
    }

    #[tokio::test]
    async fn crud() {
        let b = MemoryBackend::new();
        let r1 = put(&b, b"a", b"1").await;
        assert!(r1 > 1);
        let (_, kv) = get(&b, b"a", 0).await;
        assert_eq!(kv.unwrap().value, Bytes::from("1"));
        update(&b, b"a", b"2").await;
        let (_, kvs, count) = b.list(b"a", None, ListOptions::default()).await.unwrap();
        assert_eq!(count, 1);
        assert_eq!(kvs[0].value, Bytes::from("2"));
        // prefix convention checks
        let (_, kvs_all, _) = b
            .list(b"a", Some(&[0u8]), ListOptions::default())
            .await
            .unwrap();
        assert_eq!(kvs_all.len(), 1);
        del(&b, b"a").await;
        let (_, kv) = get(&b, b"a", 0).await;
        assert!(kv.is_none());
    }

    #[tokio::test]
    async fn ordered_ranges_preserve_etcd_bounds() {
        let b = MemoryBackend::new();
        for key in [b"\xff".as_slice(), b"a\0", b"b", b"a"] {
            put(&b, key, b"value").await;
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
        put(&b, b"a", b"old").await;
        let point = put(&b, b"b", b"other").await;
        b.compact(point).await.unwrap();
        del(&b, b"a").await;
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
        put(&b, b"k", b"v1").await;
        let r2 = update(&b, b"k", b"v2").await;
        let (_, kv) = get(&b, b"k", r2 - 1).await;
        assert_eq!(kv.unwrap().value, Bytes::from("v1"));
        b.compact(r2).await.unwrap();
        assert!(
            b.list(
                b"k",
                None,
                ListOptions {
                    revision: r2 - 1,
                    ..Default::default()
                },
            )
            .await
            .is_err()
        );
        assert!(b.events(r2 - 1, r2).await.is_err());
    }
}
