import hashlib
import random
from bisect import bisect_right

HASH_SIZE = 3
ID_SPACE = 2**HASH_SIZE
TOLERANCE = 3
VERBOSE = True


class Node:
    def __init__(self, node_id):
        self.id = node_id
        self.finger = []
        self.data = {}
        self.alive = True
        self.known_dead = set()  # Registro de nodos muertos

        self.predecessor = self
        self.successors = []  # Lista de hasta TOLERANCE+1 sucesores
        # Inicializar finger table
        for i in range(HASH_SIZE):
            self.finger.append(self)

    def check_successors(self):
        """Eliminar sucesores muertos y mantener lista llena"""
        # Filtrar muertos
        self.successors = [n for n in self.successors if n.is_alive()]

        # Mantener mínimo TOLERANCE+1 sucesores
        while len(self.successors) < TOLERANCE + 1:
            try:
                next_succ = self.successors[-1].successors[0]
                if next_succ.is_alive() and next_succ not in self.successors:
                    self.successors.append(next_succ)
                else:
                    break
            except:
                break

    def find_successor(self, key):
        """Versión tolerante a fallos de búsqueda"""
        current = self
        visited = set()

        while True:
            if current.id in visited:
                break
            visited.add(current.id)

            # Saltar nodos muertos
            while not current.is_alive():
                current = current.successors[0]

            # Verificar rango directo
            if between_right_incl(key, current.id, current.successors[0].id):
                return current.successors[0]

            # Buscar en finger table
            closest = current.closest_preceding_finger(key)
            if closest == current:
                closest = current.successors[0]

            current = closest

        return self  # Fallback

    def closest_preceding_finger(self, key):
        """Encontrar el nodo vivo más cercano"""
        for i in range(HASH_SIZE - 1, -1, -1):
            node = self.finger[i]
            if node.is_alive() and between(node.id, self.id, key):
                return node
        return self.successors[0]

    def join(self, existing_node):
        """Unión con verificación de nodos muertos"""
        if not existing_node.is_alive():
            existing_node = existing_node.find_successor(existing_node.id)

        self.successors = [existing_node.find_successor(self.id)]
        self.check_successors()
        self.stabilize()
        self.fix_fingers()

    def stabilize(self):
        """Estabilización con detección de fallos"""
        if not self.successors:
            return

        # Verificar primer sucesor
        succ = self.successors[0]
        if not succ.is_alive():
            self.successors.pop(0)
            self.check_successors()
            return

        # Obtener predecesor del sucesor
        try:
            x = succ.predecessor
            if x and x.is_alive() and between(x.id, self.id, succ.id):
                self.successors.insert(0, x)
        except:
            self.successors.pop(0)

        # Notificar al sucesor
        try:
            succ.notify(self)
        except:
            self.handle_failure(succ)

        self.check_successors()

    def notify(self, node):
        """Actualizar predecesor si es válido"""
        if self.predecessor is None or (
            node.is_alive() and between(node.id, self.predecessor.id, self.id)
        ):
            self.predecessor = node

    def handle_failure(self, dead_node):
        """Manejar nodo muerto"""
        if dead_node in self.successors:
            self.successors.remove(dead_node)
        self.known_dead.add(dead_node)
        self.check_successors()
        self.replicate_data(dead_node)

    def replicate_data(self, dead_node):
        """Recuperar datos de nodos muertos"""
        for key in list(self.data.keys()):
            if between_right_incl(key, dead_node.predecessor.id, dead_node.id):
                self.find_successor(key).data[key] = self.data[key]

    def store(self, key, value):
        """Almacenamiento con replicación"""
        nodes = []
        current = self.find_successor(key)

        # Encontrar TOLERANCE+1 nodos vivos
        for _ in range(TOLERANCE + 1):
            nodes.append(current)
            current = current.successors[0]
            while not current.is_alive():
                current = current.successors[0]

        # Almacenar en todos los nodos
        for node in nodes[: TOLERANCE + 1]:
            node.data[key] = value

    def retrieve(self, key):
        """Recuperar dato con reintentos"""
        for _ in range(TOLERANCE + 1):
            node = self.find_successor(key)
            if node.is_alive() and key in node.data:
                return node.data[key]
            key = (key + 1) % ID_SPACE  # Linear probing
        return None

    def fix_fingers(self):
        """Actualizar finger table con nodos vivos"""
        for i in range(HASH_SIZE):
            finger_key = (self.id + 2**i) % ID_SPACE
            self.finger[i] = self.find_successor(finger_key)

    def is_alive(self):
        return self.alive

    def kill(self):
        """Simular fallo inesperado"""
        self.alive = False
        # Forzar actualización en otros nodos
        for node in self.successors:
            if node.is_alive():
                node.handle_failure(self)

    def __repr__(self):
        return f"Node {self.id}"


# Funciones auxiliares
def between_right_incl(x, a, b):
    if a < b:
        return a < x <= b
    return a < x or x <= b


def between(x, a, b):
    if a < b:
        return a < x < b
    return a < x or x < b


def hash(value):
    return int(hashlib.sha1(str(value).encode()).hexdigest(), 16) % (2**HASH_SIZE)


def hash_value(value):
    return int(hashlib.sha256(str(value).encode()).hexdigest(), 16) % ID_SPACE


def reload_all(nodes):
    # Simular proceso periódico de estabilización
    for node in nodes:
        if node.is_alive():
            node.check_successors()
            node.stabilize()
            node.fix_fingers()


def print_network(nodes):
    for node in nodes:
        if node.is_alive():
            print(f"Node {node.id} - Sucesores: {[s.id for s in node.successors]}")


def simulate():
    # Crear nodos
    nodes = [Node(hash_value(i)) for i in range(5)]

    # Inicializar red
    
    for i in range(1, 5):
        nodes[i].join(nodes[i - 1])
        reload_all(nodes)

    print()
    reload_all(nodes)
    print_network(nodes)
    print()
    reload_all(nodes)
    print_network(nodes)
    print()
    reload_all(nodes)
    print_network(nodes)
    # # Almacenar datos
    # data_key = hash_value("important")
    # nodes[3].store(data_key, "secret_data")

    # # Simular fallo aleatorio
    # dead_node = random.choice(nodes)
    # dead_node.kill()
    # print(f"Node {dead_node.id} murió inesperadamente")

    # # Ejecutar estabilización
    # for node in nodes:
    #     if node.is_alive():
    #         node.stabilize()
    #         node.fix_fingers()

    # # Recuperar datos
    # result = nodes[0].retrieve(data_key)
    # print(f"Dato recuperado: {result}" if result else "Dato perdido")


simulate()
