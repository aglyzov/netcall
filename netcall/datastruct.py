# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0

from heapq import heapify, heappush, heappop


class priority_dict(dict):
    """Dictionary that can be used as a priority queue.

    Keys of the dictionary are items to be put into the queue, and values
    are their respective priorities. All dictionary methods work as expected.
    The advantage over a standard heapq-based priority queue is
    that priorities of items can be efficiently updated (amortized O(1))
    using code as 'thedict[item] = new_priority.'

    The 'smallest' method can be used to return the object with lowest
    priority, and 'pop_smallest' also removes it.

    The 'sorted_iter' method provides a destructive sorted iterator.
    """

    def __init__(self, *args, **kwargs):
        super(priority_dict, self).__init__(*args, **kwargs)
        self._rebuild_heap()

    def _rebuild_heap(self):
        self._heap = [(v, k) for k, v in self.iteritems()]
        heapify(self._heap)

    def smallest(self):
        """Return the item with the lowest priority.

        Raises IndexError if the object is empty.
        """

        heap = self._heap
        v, k = heap[0]
        while k not in self or self[k] != v:
            heappop(heap)
            v, k = heap[0]
        return k

    def pop_smallest(self):
        """Return the item with the lowest priority and remove it.

        Raises IndexError if the object is empty.
        """

        heap = self._heap
        v, k = heappop(heap)
        while k not in self or self[k] != v:
            v, k = heappop(heap)
        del self[k]
        return k

    def __setitem__(self, key, val):
        # We are not going to remove the previous value from the heap,
        # since this would have a cost O(n).

        super(priority_dict, self).__setitem__(key, val)

        if len(self._heap) < 2 * len(self):
            heappush(self._heap, (val, key))
        else:
            # When the heap grows larger than 2 * len(self), we rebuild it
            # from scratch to avoid wasting too much memory.
            self._rebuild_heap()

    def setdefault(self, key, val):
        if key not in self:
            self[key] = val
            return val
        return self[key]

    def update(self, *args, **kwargs):
        # Reimplementing dict.update is tricky -- see e.g.
        # http://mail.python.org/pipermail/python-ideas/2007-May/000744.html
        # We just rebuild the heap from scratch after passing to super.

        super(priority_dict, self).update(*args, **kwargs)
        self._rebuild_heap()

    def sorted_iter(self):
        """Sorted iterator of the priority dictionary items.

        Beware: this will destroy elements as they are returned.
        """

        while self:
            yield self.pop_smallest()

class LRU_cache(object):
    """ O(1) LRU cache implementation

        >>> lru = LRU_cache(max_size=3)
        >>> for k in [1,2,3,1,1,4]: lru[k] = str(k)
        >>> sorted(lru._cache.keys())
        [1, 3, 4]
        >>> lru._assert()
        >>> list(lru)
        [3, 1, 4]
        >>> del lru[2]
        >>> lru._assert()
        >>> lru.pop(1)
        '1'
        >>> lru._assert()
        >>> lru[2] = '2'
        >>> lru._assert()
        >>> lru[3] = '3'
        >>> lru._assert()
        >>> 1 in lru
        False
        >>> 4 in lru
        True
        >>> sorted(lru._cache.keys())
        [2, 3, 4]
        >>> list(lru)
        [4, 2, 3]
        >>> sorted(n[-1] for n in lru._cache.values())
        ['2', '3', '4']

        >>> lru = LRU_cache(max_size=10)
        >>> lru['a'] = 1
        >>> lru['b'] = 2
        >>> lru['c'] = 3
        >>> lru['d'] = 4
        >>> (lru.get('a'), lru.pop('a'))
        (1, 1)
        >>> lru._assert()
        >>> (lru.get('b'), lru.pop('b'))
        (2, 2)
        >>> lru._assert()
        >>> (lru.get('d'), lru.pop('d'))
        (4, 4)
        >>> lru._assert()
        >>> (lru.get('c'), lru.pop('c'))
        (3, 3)
        >>> lru._assert()
    """
    def __init__(self, max_size=1024):
        self._cache  = {}   # {<key> : <val>}
        self._cur_size = 0
        self._max_size = max_size
        self._oldest = None  # oldest node [<older>, <newer>, <key>, <value>]
        self._newest = None  # newest node

    def __len__(self):
        return self._cache

    def __iter__(self):
        node = self._oldest
        while node:
            yield node[-2]
            node = node[1]

    def __contains__(self, key):
        return key in self._cache

    def _refresh_node(self, node):
        newest = self._newest

        if node is not newest:
            older, newer = node[:2]
            if older:
                older[1] = newer
            else:
                self._oldest = newer
            newer[0]  = older
            newest[1] = node
            node[0] = newest
            node[1] = None
            self._newest = node

    def _assert(self):
        if self._cache:
            assert self._oldest
            assert self._newest
            if self._oldest is not self._newest:
                assert self._newest[0] is not None, self._newest
                assert self._oldest[1] is not None, self._oldest
                assert self._newest is not self._oldest
            node = self._oldest
            seen = []
            while node:
                seen.append(node)
                if node[0] is not None or node[1] is not None:
                    assert node[0] is not node[1], node
                assert node[0] is not node
                assert node[1] not in seen, node
                if node[1]:
                    assert node is node[1][0]
                node = node[1]
        else:
            assert self._oldest == None
            assert self._newest == None

    def __setitem__(self, key, val):
        cache = self._cache
        node  = cache.get(key)

        if node:
            node[-1] = val
            self._refresh_node(node)
        else:
            newest = self._newest
            node = [newest, None, key, val]
            if newest:
                newest[1] = node
            else:
                self._oldest = node
            self._newest = node

            self._cur_size += 1

            if self._cur_size > self._max_size:
                del self[self._oldest[-2]]

        cache[key] = node

    def get(self, key, default=None):
        node = self._cache.get(key, default)

        if node is not default:
            self._refresh_node(node)
            val = node[-1]
        else:
            val = default

        return val

    def pop(self, key, default=None):
        node = self._cache.pop(key, default)

        if node is not default:
            older, newer = node[:2]
            if older:
                older[1] = newer
            else:
                assert node is self._oldest, node
                self._oldest = newer
            if newer:
                newer[0] = older
            else:
                assert node is self._newest, node
                self._newest = older
            self._cur_size -= 1
            val = node[-1]
        else:
            val = default

        return val

    __getitem__ = get
    __delitem__ = pop
