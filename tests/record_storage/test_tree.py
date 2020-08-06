import unittest
from random import shuffle

from aimrecords.record_storage.tree import BPTree


class TreeTests(unittest.TestCase):

    def test_add_get(self):
        tree = BPTree(20)

        for i in range(2000):
            tree.insert(i, str(i))

        for i in range(2000):
            self.assertEqual(str(i), tree[i])
        
        self.assertIsNone(tree[-1])

    def test_add_get_rand(self):
        tree = BPTree(20)
        l = list(range(2000))
        shuffle(l)

        for i in l:
            tree.insert(i, str(i))
        
        for i in l:
            self.assertEqual(str(i), tree[i])

    def test_structure(self):
        tree = BPTree(4)
        nums = [1, 4, 7, 10, 17, 21, 31, 25, 19, 20, 28, 42]
        for num in nums:
            tree.insert(num, str(num))

        for num in nums:
            self.assertEqual(str(num), tree[num])

        self.assertEqual([20], tree.root.keys)

        left = tree.root.children[0]
        right = tree.root.children[1]

        self.assertEqual([7, 17], left.keys)
        self.assertEqual([25, 31], right.keys)

        self.assertEqual([1, 4], left.children[0].keys)
        self.assertEqual([7, 10], left.children[1].keys)
        self.assertEqual([17, 19], left.children[2].keys)

        self.assertEqual([20, 21], right.children[0].keys)
        self.assertEqual([25, 28], right.children[1].keys)
        self.assertEqual([31, 42], right.children[2].keys)

    def test_links(self):
        tree = BPTree(20)

        for i in range(2000):
            tree.insert(i, str(i))
        
        node = tree.find_path(1)[-1][0]
        total = []
        while node:
            total += node.keys
            node = node.next
        self.assertEqual(list(range(2000)), total)

    def test_duplicates(self):
        tree = BPTree(4)

        nums = [1, 2, 3, 4, 5, 1, 2, 3, 4, 5]

        for i in range(len(nums)):
            tree.insert(nums[i], str(i))

        for i in range(len(nums)):
            self.assertTrue(str(i) in tree[nums[i]])

if __name__ == "__main__":
    unittest.main()
