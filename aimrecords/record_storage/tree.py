
from bisect import bisect_right

class Node:
    """
    Internal node object, order = n. Internal nodes point
    to other nodes in the tree. Can have n children.
    * Order
    * List of keys
    * List of pointers to other nodes
    - No add function/remove function
    """
    def __init__(self, order, keys=None, children=None, next=None, leaf=None):
        self.order = order
        self.keys = keys or []
        self.children = children or []
        self.next = next
        self.leaf = bool(leaf)

    def full(self):
        """
        Returns whether a node is full or not.
        """
        return self.order == len(self.keys)
    
    def is_leaf(self):
        """
        Checks if leaf node.
        """
        return self.leaf

    def search(self, key):
        """
        Binary search to see where a key should be inserted.
        Also finds index of given key.
        """
        index = bisect_right(self.keys, key)
        return index

    def add(self, key, value):
        """
        Add a key-value pair to the node. O(n)
        Switch to binary search (still O(n))
        """
        index = self.search(key)
        if key in self.keys:
            if type(self.children[index - 1]) == set:
                self.children[index - 1].add(value)
            else:
                s = set()
                s.add(self.children[index - 1])
                s.add(value)
                self.children[index - 1] = s
        else:
            self.keys.insert(index, key)
            self.children.insert(index, value)

    def split(self):
        """
        Split node into two nodes and move self
        up as a parent.
        """
        middle = self.order // 2
        new_key = self.keys[middle]
        next = self.next
        
        if self.is_leaf():
            left = Node(self.order, self.keys[:middle],
                    self.children[:middle], leaf=self.leaf)
            right = Node(self.order, self.keys[middle:],
                            self.children[middle:], next, self.leaf)
        else:
            left = Node(self.order, self.keys[:middle],
                    self.children[:middle+1], leaf=self.leaf)
            right = Node(self.order, self.keys[middle+1:],
                            self.children[middle+1:], next, self.leaf)

        self.leaf = False
        self.keys = [new_key]
        self.children = [left, right]
        if left.is_leaf() and right.is_leaf():
            left.next = right

class BPTree():
    """
    B+ Tree object, order = n. All nodes must be at least half full.
    Root must have >= 2 entries. Search starts at root -> leaf.
    * Order
    * Root
    - search, insert, delete
    """
    def __init__(self, order):
        self.root = Node(order, leaf=True)

    def find_path(self, key):
        """
        Find path of nodes that leads to leaf node containing key.
        Or, finds path of nodes to where key should be inserted.
        """
        current = self.root
        path = []
        while not current.is_leaf():
            index = current.search(key)
            path.append([current, index])
            current = current.children[index]
        path.append([current, 0])
        return path

    def merge_nodes(self, parent, child, index):
        """
        Merge a child node and parent node. Occurs when a node
        splits and the parent node isn't full.
        """
        parent.children[index-1].next = child.children[0]
        if index != len(parent.children) - 1:
            child.children[1].next = parent.children[index+1]

        parent.children.pop(index)
        pivot = child.keys[0]
        parent.keys.insert(index, pivot)
        parent.children.insert(index, child.children[0])
        parent.children.insert(index+1, child.children[1])

    def insert(self, key, value):
        """
        Insert a key-value pair into the tree.
        """
        path = self.find_path(key)
        child = path[-1][0]

        child.add(key, value)
        
        for i in range(len(path) - 1, 0, -1):
            if path[i][0].full():
                path[i][0].split()
                self.merge_nodes(path[i-1][0], path[i][0], path[i-1][1])
        
        if self.root.full():
            self.root.split()

    def __getitem__(self, key):
        node = self.root
        
        while not node.is_leaf():
            index = node.search(key)
            node = node.children[index]
        
        if key in node.keys:
            return node.children[node.search(key) - 1]
        else:
            print("Key does not exist in tree")