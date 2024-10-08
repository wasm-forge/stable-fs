use std::collections::HashMap;

use crate::storage::types::Node;

static CACHE_CAPACITY: usize = 100;

#[derive(Debug)]
pub(crate) struct FilenameCache {
    nodes: HashMap<(Node, String), Node>,
}

impl FilenameCache {
    pub fn new() -> FilenameCache {
        let nodes: HashMap<(Node, String), Node> = HashMap::with_capacity(CACHE_CAPACITY);

        FilenameCache { nodes }
    }

    // add new cache pointer
    pub fn add(&mut self, key: (Node, String), value: Node) {
        
        if self.nodes.len() + 1 > CACHE_CAPACITY {
            self.clear();
        }
        
        self.nodes.insert(key, value);
    }

    // Clear cache completely
    pub fn clear(&mut self) {
        self.nodes.clear();
    }

    // Get a Node from the cache by its (Fd, String) key
    pub fn get(&self, key: &(Node, String)) -> std::option::Option<Node> {
        self.nodes.get(key).copied()
    }

    #[cfg(test)]
    pub fn get_nodes(&self) -> Vec<((Node, String), Node)> {
        let mut ret = Vec::new();

        for (k, v) in self.nodes.iter() {
            ret.push((k.clone(), *v));
        }

        ret
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::types::Node;

    use super::*;

    #[test]
    fn test_cache_add_and_get() {
        let mut cache = FilenameCache::new();

        let fd = 1 as Node;
        let filename = "test_file".to_string();
        let node = 35 as Node;

        cache.add((fd, filename.clone()), node.clone());

        let retrieved_node = cache.get(&(fd, filename));
        assert_eq!(retrieved_node, Some(node));
    }

    #[test]
    fn test_cache_clear() {
        let mut cache = FilenameCache::new();

        let parent_node = 1 as Node;
        let filename = "test_file".to_string();
        let node = 35 as Node;

        cache.add((parent_node, filename.clone()), node);

        cache.clear();
        let retrieved_node = cache.get(&(parent_node, filename));
        assert_eq!(retrieved_node, None);
    }

    #[test]
    fn test_cache_capacity_limit() {
        let mut cache = FilenameCache::new();

        for i in 0..CACHE_CAPACITY + 7 {
            let parent = i as Node;
            let filename = format!("file_{}", i);
            let node = i as Node + 5;

            cache.add((parent, filename.clone()), node);
        }

        // Since the cache clears when it reaches capacity + 1, the first file should be absent
        assert_eq!(cache.get(&(0, "file_0".to_string())), None);

        // Later file is present in the cache
        assert_eq!(cache.get(&(103, "file_103".to_string())), Some(108));

        assert_eq!(cache.nodes.len(), 7);
    }
}
