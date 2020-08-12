# Copyright 2020 Mathew Odden <mathewrodden@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import bisect
import hashlib


def default_hash(key):
    h = hashlib.sha1(key.encode()).hexdigest()
    return int(h, 16)


def nodekey(node, idx):
    return "%s-%d" % (node, idx)


class Ring(object):
    """
    A hashring implementation using node anchors and range lookups.

    The hash function is pluggable.

    A quick example for assigning keys to nodes with keys replicated 3 times::

        >>> ring = Ring()
        >>> ring.add(["server01", "server02", "server03", "server04", "server05"])
        >>> ring.mget("my-object-key", 3)
        ['server01', 'server04', 'server03']

    """

    def __init__(self, vnodes=10, hash_fn=None):
        self._vnodes = vnodes

        if hash_fn is None:
            hash_fn = default_hash

        self.hash_fn = hash_fn

        self.keys = []
        self.hmap = {}

    def add(self, nodes):
        """Add a set of nodes to the ring."""

        for node in nodes:
            for idx in range(self._vnodes):
                h = self.hash_fn(nodekey(node, idx))
                self.keys.append(h)
                self.hmap[h] = node

        self.keys = sorted(self.keys)

    def remove(self, nodes):
        """Remove a set of nodes to the ring."""
        for node in nodes:
            for idx in range(self._vnodes):
                h = self.hash_fn(nodekey(node, idx))
                self.keys.remove(h)
                del self.hmap[h]

        self.keys = sorted(self.keys)

    def get(self, key):
        h = self.hash_fn(key)
        idx = bisect.bisect_left(self.keys, h)

        if idx == len(self.keys):
            idx = 0

        return self.hmap[self.keys[idx]]

    def mget(self, key, n):
        h = self.hash_fn(key)
        idx = bisect.bisect_left(self.keys, h)

        if idx == len(self.keys):
            idx = 0

        nodes = []
        # walk the ring to get enough replicas
        while len(nodes) < n:
            cur = self.hmap[self.keys[idx]]
            if cur not in nodes:
                nodes.append(cur)

            idx += 1

            # handle wrapping around
            if idx >= len(self.keys):
                idx = 0

        return nodes
