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

from pprint import pprint

import hashring


def add_remove():

    ring = hashring.Ring(vnodes=30)

    ring.add([
        "server01",
        "server02",
        "server03",
        "server04",
        "server05",
    ])

    keys = ["abc123", "def123", "fasdf", "things", "and", "stuff"]

    before = {}
    after = {}

    for key in keys:
        before[key] = sorted(ring.mget(key, 3))

    ring.remove(["server05"])

    for key in keys:
        after[key] = sorted(ring.mget(key, 3))

    for key in keys:
        print("%-10s before: %s\n%-10s after:  %s" % (key, before[key], "", after[key]))


def main():

    ring = hashring.Ring(vnodes=300)

    ring.add([
        "server01",
        "server02",
        "server03",
        "server04",
        "server05",
    ])

    count_keys(ring)

    ring.remove(["server05"])

    count_keys(ring)

    ring.remove(["server02"])

    count_keys(ring)


def count_keys(ring):
    server_counts = {
        "server01": 0,
        "server02": 0,
        "server03": 0,
        "server04": 0,
        "server05": 0,
    }

    for key in range(1000 * 1000):
        nodes = ring.mget(str(key), 3)

        for node in nodes:
            server_counts[node] = server_counts[node] + 1

    pprint(server_counts)


if __name__ == "__main__":
    main()
