/*
 *
 *
 * RADIX TREE
 *
 * Search:
 *
 * 1. Begin at the root
 * 2. Traverse until a leaf is found or it is not possible to continue
 * 3. Match if found if we are at a leaf and have used up x.len() elements
 *
 * Search Cases:
 *
 * 1. The Edge's label completely matches part of x --> Edge: N | X: Node
 * 2. An Edge's label and x share a common prefix that is shorter than both labels --> Edge: NODE | X: NORM
 * 3. x is a prefix of the Edge's label --> Edge: NODE | X: NO
 * 4. x has no common prefix --> Edge: NODE | X: GEODE
 *
 *
 *
 *
 */

use std::{borrow::Cow, ptr::NonNull};

struct RadixTree {
    sentinel: NonNull<Node>,
}

impl RadixTree {
    // TODO: Check
    fn new() -> Self {
        let node = unsafe { NonNull::new_unchecked(Box::into_raw(Box::new(Node::new()))) };
        Self { sentinel: node }
    }

    fn search<X: AsRef<[u8]>>(&self, x: X) -> Option<Cow<'_, [u8]>> {
        Some(Cow::from(b"hello"))
    }
}

struct Edge {
    next: (), // Ptr to Boxed Node?
    label: (),
}

struct Node {
    edges: (),
    is_lead: (),
}

impl Node {
    fn new() -> Self {
        Self {
            edges: (),
            is_lead: (),
        }
    }
}

#[test]
fn borrow() {
    // TODO: Move to &'_ [u8] instead
    let tree = RadixTree::new();

    let mut word = tree.search(b"world").unwrap();

    match &word {
        Cow::Borrowed(_) => println!("Yay"),
        Cow::Owned(_) => println!("Nay"),
    }

    word.to_mut().extend_from_slice(b"world");

    println!("{:?}", word);
    println!("{:?}", tree.search(b"world").unwrap());

    match &word {
        Cow::Borrowed(_) => println!("Yay"),
        Cow::Owned(_) => println!("Nay"),
    }
}
