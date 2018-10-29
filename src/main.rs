#![feature(drain_filter)]
use rand::prelude::*;
use rand::distributions::{Distribution, Standard};
//use lazy_static::lazy_static;
use std::cell::Cell;
use std::thread::LocalKey;
use std::time::{Duration, Instant};
use std::cmp::Ordering;

type Time = u16;
const ENTITY_COUNT: usize = 1_000_000;
//const ENTITY_COUNT: usize = 1_0;
const CHUNK_SIZE: usize = ENTITY_COUNT * 10;
const ROUNDS: usize = 4;
const TIME_MARGIN: usize = (u8::max_value() as usize * CHUNK_SIZE / ENTITY_COUNT * 10); // times 10 for saftey
const TIME_CAP: Time = Time::max_value() - TIME_MARGIN as Time;
const PREGENED_RANDOM_U8: usize = CHUNK_SIZE * ROUNDS + ENTITY_COUNT + 10;
const NUM_BUCKETS: usize = 256;

struct PregenedRand<N> {
    ints: Vec<N>,
    current: Cell<usize>,
}

impl<N> PregenedRand<N>
where
    Standard: Distribution<N>,
    N: Clone + Copy
{
    fn new(num: usize) -> Self {
        PregenedRand {
            ints: (0..num).into_iter().map(|_| random()).collect(),
            current: Cell::new(0),
        }
    }

    #[inline]
    fn next_(&self) -> N {
        let c = self.current.get();
        self.current.set(c + 1);
        assert!(c < self.ints.len(), "Rand overflow");
        self.ints[c]
    }

    #[inline]
    fn next(r: &'static LocalKey<PregenedRand<N>>) -> N {
        r.with(|r| r.next_())
    }

    #[inline]
    fn reset_(&self) {
        self.current.set(0);
    }

    #[inline]
    fn reset(r: &'static LocalKey<PregenedRand<N>>) {
        r.with(|r| r.reset_())
    }
}

thread_local! {
    static U8RAND: PregenedRand<u8> = PregenedRand::new(PREGENED_RANDOM_U8);
}

#[derive(Debug, Clone)]
struct Entity {
    time: Time,
}

impl Entity {
    fn new() -> Self {
        Entity {
            time: PregenedRand::next(&U8RAND) as Time,
        }
    }
}

fn do_turns(num_buckets: usize) -> Duration {
    PregenedRand::reset(&U8RAND);
    println!("Rand Gen'd");

    let mut entities = vec![];
    for _ in 0..ENTITY_COUNT {
        entities.push(Entity::new());
    }
    println!("Entities Gen'd");

    fn sort_fn(a: &Time, b: &Time) -> Ordering { b.cmp(a) }
    fn bucket_fn(a: &Time, num_buckets: usize) -> usize { *a as usize % num_buckets }
    let mut bucket = Bucket::new(entities.iter().map(|e| e.time).collect(), sort_fn, bucket_fn, num_buckets);
    println!("Buckets Gen'd");

    let now = Instant::now();
    println!("Started");
    for i in 0..ROUNDS {
        println!("Round {}", i);
        for _ in 0..CHUNK_SIZE {
            let (index, _item) = bucket.pop(sort_fn);
            entities[index].time += PregenedRand::next(&U8RAND) as Time;
            bucket.reinsert(index, entities[index].time, sort_fn, bucket_fn);
        }

        let max = bucket.max(sort_fn).unwrap();
        println!("Late Round {} {}", i, max);
        if max > TIME_CAP {
            let min = bucket.min().unwrap();
            println!("Rebase! {} {}", i, min);

            entities.iter_mut().for_each(|e| e.time -= min);
            bucket = bucket.modify(|e| *e -= min, sort_fn, bucket_fn);
            println!("Buckets Gen'd");

            let max = bucket.max(sort_fn).unwrap();
            if max > TIME_CAP {
                panic!("Spread too large.");
            }
        }
    }
    now.elapsed()
}

fn main() {
    println!("Margin {}, Cap {}, Max {}", TIME_MARGIN, TIME_CAP, Time::max_value());
    println!("{} Entities, {} Actions", ENTITY_COUNT, ROUNDS * CHUNK_SIZE);
    assert!(TIME_MARGIN < Time::max_value() as usize);

    for i in &[NUM_BUCKETS] {
        println!("{} took {:?}", i, do_turns(*i));
    }
}

// We got lots of memory overhead, but we are fast.
struct Bucket<T> {
    items: Vec<T>,
    buckets: Vec<Vec<(usize, T)>>,
    heads: Vec<(usize, Option<(usize, T)>)>,
}

impl<T> Bucket<T>
where
    T: std::clone::Clone,
{
    #[inline]
    fn head_cmp<S>(
        a: &Option<(usize, T)>,
        b: &Option<(usize, T)>,
        sort_fn: &mut S
    ) -> Ordering
    where
        S: FnMut(&T, &T) -> Ordering,
    {
        match (&a, &b) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
            (Some((_, ref a)), Some((_, ref b))) => sort_fn(a, b),
        }
    }

    fn new<S, B>(items: Vec<T>, mut sort_fn: S, mut bucket_fn: B, num_buckets: usize) -> Self
    where
        S: FnMut(&T, &T) -> Ordering,
        B: FnMut(&T, usize) -> usize,
    {
        let mut items_c = items
            .clone()
            .into_iter()
            .enumerate()
            .collect::<Vec<_>>();

        let mut buckets = vec![];
        buckets.reserve(num_buckets);
        for i in 0..num_buckets {
            let mut bucket = items_c
                .drain_filter(|(_, t)| {
                    let b = bucket_fn(t, num_buckets);
                    assert!(b < num_buckets);
                    b == i
                })
                .collect::<Vec<_>>();
            bucket.sort_unstable_by(|(_, a), (_, b)| sort_fn(a, b));
            buckets.push(bucket);
        }

        let mut heads = vec![];
        heads.reserve(num_buckets);
        for (i, bucket) in (&buckets).iter().enumerate() {
            heads.push((i, bucket.last().map(|o| o.clone())));
        }
        heads.sort_unstable_by(|(_, a), (_, b)| Bucket::<T>::head_cmp(a, b, &mut sort_fn));

        Bucket {
            items,
            buckets,
            heads,
        }
    }

    // Returns the index and the item
    fn pop<S>(&mut self, mut sort_fn: S) -> (usize, T)
    where
        S: FnMut(&T, &T) -> Ordering,
    {
        let head = self.heads.pop().unwrap();
        let cur = self.buckets[head.0].pop().unwrap();
        assert_eq!(sort_fn(&cur.1, &head.1.unwrap().1), Ordering::Equal);

        let new_head = (head.0, self.buckets[head.0].last().map(|o| o.clone()));
        let pos = self.heads
            .binary_search_by(|a| Bucket::<T>::head_cmp(&a.1, &new_head.1, &mut sort_fn))
            .unwrap_or_else(|o| o);
        self.heads.insert(pos, new_head);

        cur
    }

    // Index must be the index recieved when item was poped.
    fn reinsert<S, B>(&mut self, index: usize, item: T, mut sort_fn: S, mut bucket_fn: B)
    where
        S: FnMut(&T, &T) -> Ordering,
        B: FnMut(&T, usize) -> usize,
    {
        let i = bucket_fn(&item, self.buckets.len());
        assert!(i < self.buckets.len());

        let pos = self.buckets[i]
            .binary_search_by(|(_, a)| sort_fn(a, &item))
            .unwrap_or_else(|o| o);
        let new_elem = (index, item);

        if pos == self.buckets[i].len() {
            let end = (i, self.buckets[i].last().map(|o| o.clone()));
            let rpos = self.heads
                .binary_search_by(|a| Bucket::<T>::head_cmp(&a.1, &end.1, &mut sort_fn))
                .unwrap();

            let mut frpos = rpos;
            let mut found = false;
            while frpos < self.heads.len() && Bucket::<T>::head_cmp(&self.heads[frpos].1, &end.1, &mut sort_fn) == Ordering::Equal {
                if self.heads[frpos].0 == i {
                    found = true;
                    break;
                }
                frpos += 1;
            }

            if !found && frpos != 0 {
                frpos = rpos - 1;
                while Bucket::<T>::head_cmp(&self.heads[frpos].1, &end.1, &mut sort_fn) == Ordering::Equal {
                    if self.heads[frpos].0 == i {
                        found = true;
                        break;
                    }
                    if frpos == 0 {
                        break;
                    }
                    frpos -= 1;
                }
            }

            assert!(found);
            self.heads.remove(frpos);

            let new_elem = (i, Some(new_elem.clone()));
            let pos = self.heads
                .binary_search_by(|a| Bucket::<T>::head_cmp(&a.1, &new_elem.1, &mut sort_fn))
                .unwrap_or_else(|o| o);
            assert!(pos >= frpos);
            self.heads.insert(pos, new_elem);
        }
        self.buckets[i].insert(pos, new_elem);
    }

    #[inline]
    fn max<S>(&self, mut sort_fn: S) -> Option<T>
    where
        S: FnMut(&T, &T) -> Ordering,
    {
        self.buckets
            .iter()
            .max_by(|a, b| Bucket::<T>::head_cmp(&a.first().cloned(), &b.first().cloned(), &mut sort_fn))
            .unwrap()
            .first()
            .map(|(_, a)| a)
            .cloned()
    }

    #[inline]
    fn min(&self) -> Option<T> {
        self.heads
            .last()
            .unwrap()
            .1
            .clone()
            .map(|(_, a)| a)
    }

    #[inline]
    fn modify<M, S, B>(mut self, modify_fn: M, sort_fn: S, bucket_fn: B) -> Self
    where
        M: FnMut(&mut T),
        S: FnMut(&T, &T) -> Ordering,
        B: FnMut(&T, usize) -> usize,
    {
        self.items.iter_mut().for_each(modify_fn);
        Bucket::new(self.items, sort_fn, bucket_fn, self.buckets.len())
    }
}
