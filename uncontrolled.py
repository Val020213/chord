import hashlib
import time
import threading
from bisect import bisect_right

HASH_SIZE = 8
ID_SPACE = 2**HASH_SIZE
TOLERANCE = 3
STABILIZE_INTERVAL = 2
CHECK_INTERVAL = 1
FAILURE_TIMEOUT = 5


class Node:
    def __init__(self, id, m=HASH_SIZE):
        self.id = id
        self.predecessor = None
        self.successors = []
        self.finger = []
        self.data = {}
        self.m = m
        self.alive = True
        self.lock = threading.RLock()
        self.last_seen = {}  # Track node liveness
        self.failure_counter = {}

        # Inicialización de finger table
        self.finger = [self] * m
        self.start_background_tasks()

    def start_background_tasks(self):
        def stabilizer():
            while self.alive:
                self.stabilize()
                time.sleep(STABILIZE_INTERVAL)

        def failure_detector():
            while self.alive:
                self.check_failures()
                time.sleep(CHECK_INTERVAL)

        threading.Thread(target=stabilizer, daemon=True).start()
        threading.Thread(target=failure_detector, daemon=True).start()

    def check_failures(self):
        with self.lock:
            now = time.time()
            dead_nodes = []

            # Verificar todos los nodos conocidos
            for node in list(self.last_seen.keys()):
                if node != self and now - self.last_seen[node] > FAILURE_TIMEOUT:
                    self.failure_counter[node] = self.failure_counter.get(node, 0) + 1
                    if self.failure_counter[node] >= 3:
                        dead_nodes.append(node)

            # Manejar nodos muertos
            for node in dead_nodes:
                self.handle_failure(node)

            # Actualizar sucesores
            self.successors = [n for n in self.successors if n.is_alive()]

    def handle_failure(self, dead_node):
        with self.lock:
            # Eliminar de sucesores
            if dead_node in self.successors:
                self.successors.remove(dead_node)
                self.update_successors()

            # Eliminar de finger table
            for i in range(len(self.finger)):
                if self.finger[i] == dead_node:
                    self.finger[i] = self.find_successor((self.id + 2**i) % ID_SPACE)

            # Replicar datos perdidos
            self.recover_data(dead_node)

            del self.last_seen[dead_node]
            del self.failure_counter[dead_node]

    def update_successors(self):
        with self.lock:
            # Buscar nuevos sucesores si es necesario
            if len(self.successors) < TOLERANCE + 1:
                try:
                    new_successors = self.find_successor(self.id).get_successors()
                    self.successors.extend(new_successors)
                    self.successors = list({n.id: n for n in self.successors}.values())[
                        : TOLERANCE + 1
                    ]
                except:
                    pass

    def record_contact(self, node):
        with self.lock:
            self.last_seen[node] = time.time()
            self.failure_counter.pop(node, None)

    def find_successor(self, key):
        visited = set()
        current = self
        attempts = 0

        while attempts < self.m:
            if current.id in visited:
                break

            visited.add(current.id)
            current.record_contact(current)

            if between_right_incl(key, current.id, current.successors[0].id):
                return current.successors[0]

            next_node = current.closest_preceding_finger(key)
            if next_node == current:
                next_node = current.successors[0]

            if not next_node.is_alive():
                current.handle_failure(next_node)
                next_node = current.closest_preceding_finger(key)

            current = next_node
            attempts += 1

        return self  # Fallback a sí mismo

    def stabilize(self):
        with self.lock:
            try:
                successor = self.successors[0]
                x = successor.predecessor

                if x and x.is_alive() and between(x.id, self.id, successor.id):
                    self.successors.insert(0, x)
                    self.successors = self.successors[: TOLERANCE + 1]

                successor.notify(self)
                successor.record_contact(self)

                # Replicar datos al sucesor
                self.replicate_data(successor)

            except Exception as e:
                self.handle_failure(successor)

    def notify(self, node):
        with self.lock:
            if not self.predecessor or between(node.id, self.predecessor.id, self.id):
                self.predecessor = node
                node.record_contact(self)

            # Actualizar lista de sucesores
            if node not in self.successors:
                self.successors.append(node)
                self.successors.sort(key=lambda n: (n.id - self.id) % ID_SPACE)
                self.successors = self.successors[: TOLERANCE + 1]

    def recover_data(self, dead_node):
        with self.lock:
            # Reconstruir datos desde réplicas
            for key in list(self.data.keys()):
                if between_right_incl(key, dead_node.predecessor.id, dead_node.id):
                    successor = self.find_successor(key)
                    successor.data[key] = self.data[key]

    def store(self, key, value):
        with self.lock:
            # Almacenar en TOLERANCE+1 nodos
            nodes = [self.find_successor(key)]
            for i in range(TOLERANCE):
                nodes.append(nodes[-1].successors[0])

            for node in nodes:
                if node.is_alive():
                    node.data[key] = value
                    node.record_contact(self)
                else:
                    self.handle_failure(node)

    def __getattr__(self, name):
        # Manejar llamadas a nodos caídos
        def handler(*args, **kwargs):
            raise NodeFailure("Node is unreachable")

        return handler


class NodeFailure(Exception):
    pass


# Funciones auxiliares optimizadas
def between_right_incl(x, a, b):
    if a < b:
        return a < x <= b
    return a < x or x <= b


def hash_value(value):
    return int(hashlib.sha256(str(value).encode()).hexdigest(), 16) % ID_SPACE


def simulation():
    # Crear red
    nodes = [Node(hash_value(i)) for i in range(10)]
    for node in nodes[1:]:
        node.join(nodes[0])

    # Almacenar datos
    nodes[0].store(hash_value("secret"), "data123")

    # Simular fallo no controlado
    import os
    import signal


    # Recuperación automática
    surviving_nodes = [n for n in nodes if n.is_alive()]
    for node in surviving_nodes:
        node.check_failures()
        node.stabilize()

    # Recuperar datos
    value = (
        surviving_nodes[0]
        .find_successor(hash_value("secret"))
        .data.get(hash_value("secret"))
    )
    print(f"Dato recuperado: {value}")


if __name__ == "__main__":
    simulation()
