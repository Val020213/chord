import hashlib
import time
import threading
from bisect import bisect_right

HASH_SIZE = 3  # Mayor espacio para distribución
ID_SPACE = 2**HASH_SIZE
TOLERANCE = 3
STABILIZE_INTERVAL = 2
FIX_FINGERS_INTERVAL = 3
CHECK_PRED_INTERVAL = 5

VERBOSE = True
TABS = 0


def AddTab():
    global TABS
    TABS += 1


def RemoveTab():
    global TABS
    TABS -= 1


class Node:
    def __init__(self, id, m=HASH_SIZE):
        self.id = id
        self.predecessor = None
        self.successors = []  # Lista circular ordenada
        self.finger = []
        self.data = {}
        self.m = m
        self.alive = True
        # self.lock = threading.RLock()
        # Inicialización de finger table
        self.known_nodes = set()  # Cache de nodos conocidos
        self.successors_cache = []  # Cache de sucesores verificados

        self.finger = [self] * m
        self.predecessor = None
        self.successors = []  # Lista circular ordenada
        # self.start_background_tasks()

    # def start_background_tasks(self):
    #     def stabilizer():
    #         while self.alive:
    #             self.stabilize()
    #             time.sleep(STABILIZE_INTERVAL)

    #     def fix_fingers():
    #         while self.alive:
    #             self.fix_finger_table()
    #             time.sleep(FIX_FINGERS_INTERVAL)

    #     def check_predecessor():
    #         while self.alive:
    #             self.check_predecessor()
    #             time.sleep(CHECK_PRED_INTERVAL)

    #     threading.Thread(target=stabilizer, daemon=True).start()
    #     threading.Thread(target=fix_fingers, daemon=True).start()
    #     threading.Thread(target=check_predecessor, daemon=True).start()

    # --- Métodos de consistencia mejorados ---
    def update_successors(self, new_successors):
        merged = []
        seen = set()

        # Merge y deduplicación
        for node in set([self] + new_successors + self.successors):
            if node.id != self.id and node.id not in seen and node.is_alive():
                merged.append(node)
                seen.add(node.id)
                if len(merged) >= TOLERANCE + 1:
                    break

        # Mantener orden circular

        self.successors = sorted(
            merged[: TOLERANCE + 1], key=lambda n: (n.id - self.id) % ID_SPACE
        )
        self.successors_cache = [n for n in self.successors if n.is_alive()]

    def get_first_alive_successor(self):
        for node in self.successors + [self]:
            if node.is_alive():
                return node
        return self  # Fallback

    # --- Búsqueda optimizada con cache ---
    def find_successor(self, key, hops=0, visited=None):
        print("\t" * TABS, f"Node {self.id}:", "finding successor of", key)
        if visited is None:
            visited = set()

        if hops > self.m:  # Prevenir bucles infinitos
            print("\t" * TABS, f"Node {self.id}:", "too many hops")
            return None

        # Verificación de cache local
        if self.id == key:
            print(
                "\t" * TABS,
                f"Node {self.id}:",
                "found itself as the successor for",
                key,
            )
            return self

        successor = self.get_first_alive_successor()
        print("\t" * TABS, f"Node {self.id}:", "first alive successor is", successor.id)
        if between_right_incl(key, self.id, successor.id):
            print(
                "\t" * TABS,
                f"Node {self.id}:",
                "is between",
                self.id,
                "and",
                successor.id,
            )
            return successor

        # Buscar en finger table optimizada
        closest = self.closest_preceding_finger(key)
        print(
            "\t" * TABS, f"Node {self.id}:", "closest preceding finger is", closest.id
        )
        if closest.id == self.id or closest.id in visited:
            print("\t" * TABS, f"Node {self.id}:", "no closer finger found for", key)
            return successor

        visited.add(self.id)
        print(
            "\t" * TABS,
            f"Node {self.id}:",
            "visiting",
            closest.id,
            "to find successor for",
            key,
        )
        AddTab()
        sol = closest.find_successor(key, hops + 1, visited)
        RemoveTab()
        return sol

    # --- Finger table optimizada ---
    def closest_preceding_finger(self, key):
        print(f"Node {self.id}:", "finding closest preceding finger for", key)
        for i in range(self.m - 1, -1, -1):
            node = self.finger[i]
            if node.is_alive() and between(node.id, self.id, key):
                print(f"Node {self.id}:", "is between", self.id, "and", node.id)
                return node
        print(f"Node {self.id}:", "no closer finger found for", key)
        return self.get_first_alive_successor()

    # --- Join mejorado con bootstrap optimizado ---
    def join(self, bootstrap_node: "Node"):

        if bootstrap_node:
            print(f"Node {self.id}:", "joining network through", bootstrap_node.id)

            AddTab()
            successor = bootstrap_node.find_successor(self.id)
            VERBOSE and print(
                "\t" * TABS, f"Node {self.id}:", "found successor", successor.id
            )
            RemoveTab()
            predecessors = (
                successor.predecessor.get_successors() if successor.predecessor else []
            )
            VERBOSE and print(
                "\t" * TABS,
                f"Node {self.id}:",
                "found predecessors",
                [p.id for p in predecessors],
            )

            AddTab()
            self.update_successors([successor] + predecessors)
            RemoveTab()
            VERBOSE and print(
                "\t" * TABS,
                f"Node {self.id}:",
                "updated successors",
                [s.id for s in self.successors],
            )
        else:
            self.update_successors([self])
        AddTab()
        self.stabilize()
        self.fix_finger_table()
        self.replicate_data()
        RemoveTab()

    # --- Replicación de datos automática ---
    def replicate_data(self):
        for key in list(self.data.keys()):
            for successor in self.successors[:TOLERANCE]:
                if successor.is_alive() and successor.id != self.id:
                    successor.store_replica(key, self.data[key])

    def store(self, key, value):

        self.data[key] = value
        self.replicate_data()

    def store_replica(self, key, value):

        self.data[key] = value

    # --- Stabilization mejorada con transferencia de datos ---
    def stabilize(self):

        successor = self.get_first_alive_successor()
        if successor:
            try:
                x = successor.predecessor
                if x and x.is_alive() and between(x.id, self.id, successor.id):
                    self.update_successors([x] + x.get_successors())
            except:
                pass

            successor.notify(self)
            self.transfer_data(successor)

    def transfer_data(self, successor):

        to_transfer = {}
        for key in list(self.data.keys()):
            if not between_right_incl(key, self.predecessor.id, self.id):
                to_transfer[key] = self.data.pop(key)
        if to_transfer and successor.is_alive():
            successor.bulk_store(to_transfer)

    # --- Métodos auxiliares optimizados ---
    def notify(self, node):

        if not self.predecessor or (
            node.is_alive() and between(node.id, self.predecessor.id, self.id)
        ):
            self.predecessor = node
            self.update_successors([node] + node.get_successors())

    def fix_finger_table(self):

        for i in range(self.m):
            finger_key = (self.id + 2**i) % ID_SPACE
            node = self.find_successor(finger_key)
            if node:
                self.finger[i] = node
            else:
                self.finger[i] = self.get_first_alive_successor()

    def check_predecessor(self):

        if self.predecessor and not self.predecessor.is_alive():
            self.predecessor = None
            self.replicate_data()  # Recuperar datos

    # --- Métodos de ayuda ---
    def get_successors(self):
        return self.successors_cache.copy()

    def is_alive(self):
        return self.alive

    def kill(self):

        self.alive = False
        self.transfer_data(self.get_first_alive_successor())

    def __repr__(self):
        return f"Node {self.id}"


# --- Funciones de ayuda optimizadas con pre-cálculo ---
def between_right_incl(x, a, b):
    if a < b:
        return a < x <= b
    return a < x or x <= b


def between(x, a, b):
    if a < b:
        return a < x < b
    return a < x or x < b


def hash_value(value):
    return int(hashlib.sha256(str(value).encode()).hexdigest(), 16) % ID_SPACE


def reload(node: Node):
    if node.is_alive():
        node.check_predecessor()
        node.stabilize()
        node.fix_finger_table()


def reload_network(nodes):
    for node in nodes:
        reload(node)
    print(" - - - - - - - - - - - - - Network reloaded - -- - - -- - - - - ")


def print_network(nodes):
    for node in nodes:
        try:
            print(f"Node {node.id} (Alive: {node.alive})")
            print(f"Predecessor: {node.predecessor.id if node.predecessor else None}")
            print(f"Successors: {[s.id for s in node.successors]}")
            print(f"Finger table: {[f.id for f in node.finger]}")
            print(f"Data: {node.data}")
            print()
        except Exception as e:
            print(f"Error printing state for node {node.id}: {e}")
    print("* * " * 20)


def main():
    # Ejemplo de uso
    nodes = [Node(2), Node(5)]
    nodes[0].join(None)
    nodes[1].join(nodes[0])
    reload_network(nodes)
    print_network(nodes)
    # print_network(nodes)

    node = Node(7)
    node.join(nodes[0])
    nodes.append(node)
    reload_network(nodes)
    print_network(nodes)


    nodes[0].kill()
    nodes[1].kill()
    reload_network(nodes)
    print_network(nodes)


if __name__ == "__main__":
    main()
