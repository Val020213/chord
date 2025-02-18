import hashlib
import random

HASH_SIZE = 3
ID = 0
VERBOSE = True


class Node:
    def __init__(self, id, m=HASH_SIZE):
        self.id = id
        self.predecessor = self
        self.successor = self
        self.finger = [self] * HASH_SIZE
        self.data = {}
        self.m = m
        self.alive = True

    def is_alive(self):
        return self.alive

    def reset(self):
        self = Node(self.id)

    def __getattribute__(self, name):
        if (
            name != "reset"
            and name != "is_alive"
            and name != "alive"
            and not super().__getattribute__("alive")
        ):
            raise Exception("Node is dead")
        return super().__getattribute__(name)

    def kill(self):
        self.alive = False

    def find(self, id):

        VERBOSE and print(
            f"Node {self.id}: Finding successor of {id}, is between {self.id} and {self.successor.id}? {betweenRightInclusive(id, self.id, self.successor.id)}"
        )
        if betweenRightInclusive(id, self.id, self.successor.id):
            return self

        n = self.closest_preceding_finger(id)
        return n.find(id)

    def closest_preceding_finger(self, id):
        for i in range(self.m - 1, -1, -1):
            if self.finger[i] is not None and betweenRightInclusive(
                self.finger[i].id, self.id, id
            ):
                VERBOSE and print(
                    f"Node {self.id}: Closest preceding finger of {id} is {self.finger[i].id} in table {[n.id for n in self.finger]}"
                )
                return self.finger[i]
        return self

    def join(self, n: "Node"):
        self.predecessor = self
        self.successor = n.find(self.id).successor
        VERBOSE and print(f"Node {self.id}: Joining network with {n.id}")
        VERBOSE and print(f"Node {self.id}: Successor is {self.successor.id}")
        self.stabilize()
        self.fix_finger_table()

    def stabilize(self):
        x = self.successor.predecessor
        if betweenRightInclusive(x.id, self.id, self.successor.id):
            self.successor = x
        self.successor.notify(self)

    def notify(self, n: "Node"):
        if self.predecessor is None or betweenRightInclusive(
            n.id, self.predecessor.id, self.id
        ):
            self.predecessor = n

    def fix_finger_table(self):
        for i in range(self.m):
            self.finger[i] = self.find((self.id + 2**i) % 2**self.m).successor
            VERBOSE and print(
                f"Node {self.id}: Fixing finger {i}, result {(self.id + 2**i) % 2**self.m}, successor {self.finger[i].id}"
            )
            VERBOSE and print("-----------------------------------")

    def check_predecessor(self):
        if self.predecessor is not None and not self.predecessor.is_alive():
            self.predecessor = None
            

    def store(self, value):
        key = hash(value)
        self.find_successor(key).data[key] = value

    def retrieve(self, value):
        key = hash(value)
        return self.find_successor(key).data.get(key, None)

    def delete(self, value):
        key = hash(value)
        return self.find_successor(key).data.pop(key, None)

    def print_state(self):
        print(f"Node {self.id}")
        print(f"Predecessor: {self.predecessor.id}")
        print(f"Successor: {self.successor.id}")
        print(f"Finger table: {[f.id for f in self.finger]}")
        print(f"Data: {self.data}")


def hash(value):
    return int(hashlib.sha1(str(value).encode()).hexdigest(), 16) % (2**HASH_SIZE)


def betweenRightInclusive(x, a, b):
    if a < b:
        return a < x <= b
    return a < x or x <= b


def between(x, a, b):
    if a < b:
        return a < x < b
    return a < x or x < b


def reload(node):
    node.stabilize()
    node.fix_finger_table()
    node.check_predecessor()


def print_states(nodes):
    for node in nodes:
        try:
            node.print_state()
        except Exception as e:
            print(f"Error printing state for node: {e}")
        print()


def reload_all(nodes):
    for node in nodes:
        reload(node)


def main():

    nodes = [Node(2), Node(5)]
    nodes[0].join(nodes[1])
    # reload_all(nodes)
    # print_states(nodes)
    # reload_all(nodes)
    print_states(nodes)

    node = Node(7)
    node.join(nodes[0])
    nodes.append(node)
    print_states(nodes)

    reload_all(nodes)
    print_states(nodes)

    nodes[2].kill()

    node = Node(4)
    node.join(nodes[0])
    nodes.append(node)

    reload_all(nodes)
    print_states(nodes)


if __name__ == "__main__":
    main()
