import hashlib
import random

HASH_SIZE = 3
ID = 0
VERBOSE = False
TOLERANCE = 2  # Número de sucesores a mantener


class Node:
    def __init__(self, id, m=HASH_SIZE):
        self.id = id
        self.finger = []
        self.data = {}
        self.m = m
        self.alive = True

        self.predecessor = self
        self.successor = [self]  # Lista de sucesores, máximo TOLERANCE + 1
        self.successors_cache = []  # Cache de sucesores verificados
        self.finger = [self] * m

    def is_alive(self):
        return self.alive

    def reset(self):
        self.__init__(self.id)

    def __getattribute__(self, name):
        if (
            name != "reset"
            and name != "is_alive"
            and name != "alive"
            and not super().__getattribute__("alive")
        ):
            return None
        return super().__getattribute__(name)

    def kill(self):
        self.alive = False

    def find_best_successor(self):
        for succ in self.successor:
            if succ.is_alive():
                return succ
        return None

    def find(self, id):
        current_successor = self.find_best_successor()
        if not current_successor:
            raise Exception("No live successors")

        VERBOSE and print(
            f"Node {self.id}: Finding successor of {id}, is between {self.id} and {current_successor.id}? {betweenRightInclusive(id, self.id, current_successor.id)}"
        )
        if betweenRightInclusive(id, self.id, current_successor.id):
            return current_successor

        n = self.closest_preceding_finger(id)
        return n.find(id)

    def closest_preceding_finger(self, id):
        # Buscar en la finger table el nodo vivo más cercano
        for i in range(self.m - 1, -1, -1):
            if self.finger[i].is_alive() and between(self.finger[i].id, self.id, id):
                VERBOSE and print(
                    f"Node {self.id}: Closest preceding finger of {id} is {self.finger[i].id} in table {[n.id for n in self.finger]}"
                )
                return self.finger[i]
        # Si no hay fingers válidos, usar el sucesor vivo más cercano
        for succ in self.successor:
            if succ.is_alive():
                return succ
        return self  # Último recurso, aunque podría estar muerto

    def join(self, bootstrap_node: "Node"):
        self.predecessor = None
        succ = bootstrap_node.find(self.id)
        print(f"Node {self.id}: Joining with {succ.id}")
        self.successor = [succ] + succ.successor[:TOLERANCE]
        self.stabilize()
        self.fix_finger_table()

    def stabilize(self):
        current_successor = self.find_best_successor()
        if not current_successor:
            return
        VERBOSE and print(f"Node {self.id}: Stabilizing with {current_successor.id}")
        x = current_successor.predecessor

        if x and x.is_alive() and between(x.id, self.id, current_successor.id):
            print(f"Node {self.id}: Updating successor to {x.id}")
            self.successor = [x] + self.successor[:-1]
            print(f"Node {self.id}: New successors: {[s.id for s in self.successor]}")

        try:
            current_successor.notify(self)
        except:
            self.successor.remove(current_successor)
            self.successor.append(self.find(self.id))

        alive_successors = [s for s in self.successor if s.is_alive()]

        while len(alive_successors) < TOLERANCE + 1:
            last_alive = alive_successors[-1] if alive_successors else self
            new_succ = last_alive.find(last_alive.id)
            if new_succ not in alive_successors:
                alive_successors.append(new_succ)
            else:
                break  # Evitar bucle infinito
        self.successor = alive_successors[: TOLERANCE + 1]

    def notify(self, n: "Node"):
        if not self.predecessor or (
            n.is_alive() and between(n.id, self.predecessor.id, self.id)
        ):
            self.predecessor = n

    def fix_finger_table(self):
        for i in range(self.m):
            # Encontrar sucesor para (id + 2^i) mod 2^m
            try:
                self.finger[i] = self.find((self.id + 2**i) % 2**self.m)
            except:
                # Si falla, mantener el valor anterior si está vivo
                if not self.finger[i].is_alive():
                    self.finger[i] = self.find_best_successor() or self

    def check_predecessor(self):
        if self.predecessor and not self.predecessor.is_alive():
            self.predecessor = None

    def store(self, value):
        key = hash(value)
        node = self.find(key)
        if node.is_alive():
            node.data[key] = value

    def retrieve(self, value):
        key = hash(value)
        node = self.find(key)
        return node.data.get(key, None) if node.is_alive() else None

    def delete(self, value):
        key = hash(value)
        node = self.find(key)
        if node.is_alive():
            return node.data.pop(key, None)
        return None

    def print_state(self):
        print(f"Node {self.id} (Alive: {self.alive})")
        print(f"Predecessor: {self.predecessor.id if self.predecessor else None}")
        print(f"Successors: {[s.id for s in self.successor]}")
        print(f"Finger table: {[f.id for f in self.finger]}")
        print(f"Data: {self.data}")
        print()


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


def reload(node: Node):
    if node.is_alive():
        node.check_predecessor()
        node.stabilize()
        node.fix_finger_table()


def print_states(nodes):
    for node in nodes:
        try:
            node.print_state()
        except Exception as e:
            print(f"Error printing state for node {node.id}: {e}")
        print()
    print("* * " * 20)


def reload_all(nodes):
    for node in nodes:
        reload(node)


def main():
    # Ejemplo de uso
    nodes = [Node(2), Node(5)]
    nodes[0].join(nodes[1])
    # reload_all(nodes)
    # reload_all(nodes)
    # reload_all(nodes)
    # print_states(nodes)

    # node7 = Node(7)
    # node7.join(nodes[0])
    # nodes.append(node7)
    # print_states(nodes)

    # reload_all(nodes)
    # reload_all(nodes)
    # reload_all(nodes)
    # reload_all(nodes)
    # print_states(nodes)

    # nodes[1].kill()

    # reload_all(nodes)
    # reload_all(nodes)
    # print_states(nodes)


if __name__ == "__main__":
    main()
