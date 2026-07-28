import asyncio
import random
import logging
import sys
from dataclasses import dataclass, field
from enum import Enum

import os
LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "logs", "raft_log.txt")

logger = logging.getLogger("RAFT")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("[t=%(relativeCreated)6dms] %(message)s")

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

file_handler = logging.FileHandler(LOG_PATH, mode="w", encoding="utf-8")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


HEARTBEAT_INTERVAL = 0.15         
ELECTION_TIMEOUT_RANGE = (0.30, 0.60)  
RPC_DELAY_RANGE = (0.01, 0.05)     


class Rol(Enum):
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"


@dataclass
class EntradaLog:
    term: int
    comando: str  


@dataclass
class Nodo:
    id: int
    cluster: "Cluster" = None
    rol: Rol = Rol.FOLLOWER
    term_actual: int = 0
    voto_otorgado_a: int = None
    log: list = field(default_factory=list)
    commit_index: int = -1
    ultimo_aplicado: int = -1
    vivo: bool = True               
    lider_id: int = None
    next_index: dict = field(default_factory=dict)
    match_index: dict = field(default_factory=dict)
    _reset_timeout: asyncio.Event = field(default_factory=asyncio.Event)
    _tarea_loop: asyncio.Task = None

    def estado_maquina(self):
        """Devuelve el estado de la máquina de estados replicada (los
        comandos ya comprometidos / committed)."""
        return [e.comando for e in self.log[: self.commit_index + 1]]

    def log_evento(self, msg):
        logger.info(f"[Nodo-{self.id} | {self.rol.value:9s} | term={self.term_actual}] {msg}")


    async def recibir_request_vote(self, term, candidato_id, ultimo_log_index, ultimo_log_term):
        if not self.vivo:
            return (self.term_actual, False)

        await asyncio.sleep(random.uniform(*RPC_DELAY_RANGE))  

        if term < self.term_actual:
            return (self.term_actual, False)

        if term > self.term_actual:
            self.term_actual = term
            self.voto_otorgado_a = None
            self.rol = Rol.FOLLOWER

        log_actualizado = self._candidato_log_al_dia(ultimo_log_index, ultimo_log_term)

        if (self.voto_otorgado_a in (None, candidato_id)) and log_actualizado:
            self.voto_otorgado_a = candidato_id
            self._reset_timeout.set()
            self.log_evento(f"-> voto concedido a Nodo-{candidato_id} (term {term})")
            return (self.term_actual, True)

        return (self.term_actual, False)

    def _candidato_log_al_dia(self, ultimo_log_index, ultimo_log_term):
        """Regla de seguridad de RAFT: solo se vota por un candidato cuyo
        log esté igual o más actualizado que el propio."""
        if not self.log:
            return True
        mi_ultimo_term = self.log[-1].term
        mi_ultimo_index = len(self.log) - 1
        if ultimo_log_term != mi_ultimo_term:
            return ultimo_log_term > mi_ultimo_term
        return ultimo_log_index >= mi_ultimo_index


    async def recibir_append_entries(self, term, lider_id, prev_log_index,
                                      prev_log_term, entries, leader_commit):
        if not self.vivo:
            return (self.term_actual, False)

        await asyncio.sleep(random.uniform(*RPC_DELAY_RANGE))

        if term < self.term_actual:
            return (self.term_actual, False)

        self.term_actual = term
        self.rol = Rol.FOLLOWER
        self.lider_id = lider_id
        self.voto_otorgado_a = lider_id
        self._reset_timeout.set()

        if prev_log_index >= 0:
            if len(self.log) <= prev_log_index or self.log[prev_log_index].term != prev_log_term:
                return (self.term_actual, False)

        if entries:
            self.log = self.log[: prev_log_index + 1] + entries
            self.log_evento(f"log replicado: {[e.comando for e in self.log]}")

        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log) - 1)
            self._aplicar_comprometidas()

        return (self.term_actual, True)

    def _aplicar_comprometidas(self):
        while self.ultimo_aplicado < self.commit_index:
            self.ultimo_aplicado += 1
            entrada = self.log[self.ultimo_aplicado]
            self.log_evento(f"APLICADO a la máquina de estados -> {entrada.comando}")

    async def bucle_principal(self):
        while True:
            if not self.vivo:
                await asyncio.sleep(0.05)
                continue

            if self.rol == Rol.LEADER:
                await asyncio.sleep(HEARTBEAT_INTERVAL)
                continue

            timeout = random.uniform(*ELECTION_TIMEOUT_RANGE)
            self._reset_timeout.clear()
            try:
                await asyncio.wait_for(self._reset_timeout.wait(), timeout)
            except asyncio.TimeoutError:
                if self.vivo:
                    await self._iniciar_eleccion()

    async def _iniciar_eleccion(self):
        self.rol = Rol.CANDIDATE
        self.term_actual += 1
        self.voto_otorgado_a = self.id
        votos = 1  
        term_eleccion = self.term_actual
        self.log_evento("temporizador expiró -> inicia ELECCIÓN")

        ultimo_log_index = len(self.log) - 1
        ultimo_log_term = self.log[-1].term if self.log else 0

        otros = [n for n in self.cluster.nodos.values() if n.id != self.id]
        respuestas = await asyncio.gather(*[
            n.recibir_request_vote(term_eleccion, self.id, ultimo_log_index, ultimo_log_term)
            for n in otros
        ])

        if self.term_actual != term_eleccion or self.rol != Rol.CANDIDATE:
            return  

        for term_resp, voto in respuestas:
            if term_resp > self.term_actual:
                self.term_actual = term_resp
                self.rol = Rol.FOLLOWER
                self.voto_otorgado_a = None
                return
            if voto:
                votos += 1

        mayoria = (len(self.cluster.nodos) // 2) + 1
        if votos >= mayoria and self.rol == Rol.CANDIDATE:
            await self._convertirse_en_lider()
        else:
            self.log_evento(f"no obtuvo mayoría ({votos}/{len(self.cluster.nodos)}), vuelve a FOLLOWER")
            self.rol = Rol.FOLLOWER

    async def _convertirse_en_lider(self):
        self.rol = Rol.LEADER
        self.lider_id = self.id
        self.cluster.lider_actual = self.id
        for n in self.cluster.nodos.values():
            self.next_index[n.id] = len(self.log)
            self.match_index[n.id] = -1
        self.log_evento(f"*** ELEGIDO LÍDER *** para el término {self.term_actual}")
        asyncio.create_task(self._enviar_heartbeats())


    async def _enviar_heartbeats(self):
        while self.rol == Rol.LEADER and self.vivo:
            await self._replicar_a_followers()
            await asyncio.sleep(HEARTBEAT_INTERVAL)

    async def _replicar_a_followers(self):
        otros = [n for n in self.cluster.nodos.values() if n.id != self.id]
        acks = 1  

        async def enviar_a(n):
            nonlocal acks
            prev_index = self.next_index.get(n.id, len(self.log)) - 1
            prev_term = self.log[prev_index].term if prev_index >= 0 else 0
            entries = self.log[prev_index + 1:]
            term_r, exito = await n.recibir_append_entries(
                self.term_actual, self.id, prev_index, prev_term,
                entries, self.commit_index
            )
            if term_r > self.term_actual:
                self.term_actual = term_r
                self.rol = Rol.FOLLOWER
                return
            if exito:
                self.match_index[n.id] = len(self.log) - 1
                self.next_index[n.id] = len(self.log)
                acks += 1
            else:
                self.next_index[n.id] = max(0, self.next_index.get(n.id, 1) - 1)

        await asyncio.gather(*[enviar_a(n) for n in otros])

        mayoria = (len(self.cluster.nodos) // 2) + 1
        if self.log and acks >= mayoria and self.rol == Rol.LEADER:
            nuevo_commit = len(self.log) - 1
            if nuevo_commit > self.commit_index:
                self.commit_index = nuevo_commit
                self._aplicar_comprometidas()


    async def proponer(self, comando):
        if self.rol != Rol.LEADER or not self.vivo:
            self.log_evento(f"RECHAZA propuesta '{comando}' (no es líder)")
            return False
        self.log.append(EntradaLog(self.term_actual, comando))
        self.log_evento(f"CLIENTE propone '{comando}' -> agregado al log en índice {len(self.log)-1}")
        await self._replicar_a_followers()
        return True


class Cluster:
    def __init__(self, n_nodos=5):
        self.nodos = {i: Nodo(id=i) for i in range(n_nodos)}
        for n in self.nodos.values():
            n.cluster = self
        self.lider_actual = None

    def iniciar(self):
        for n in self.nodos.values():
            n._tarea_loop = asyncio.create_task(n.bucle_principal())

    def obtener_lider(self):
        lideres = [n for n in self.nodos.values() if n.rol == Rol.LEADER and n.vivo]
        return lideres[0] if lideres else None

    def matar_nodo(self, node_id):
        n = self.nodos[node_id]
        n.vivo = False
        n.log_evento("### NODO CAÍDO (simulación de fallo) — deja de responder RPCs ###")

    def revivir_nodo(self, node_id):
        n = self.nodos[node_id]
        n.vivo = True
        n.rol = Rol.FOLLOWER
        n._reset_timeout.set()
        n.log_evento(">>> nodo recuperado, reingresa como FOLLOWER <<<")


async def esperar_lider(cluster, timeout=5.0):
    """Espera activamente hasta que el clúster tenga un líder estable."""
    transcurrido = 0.0
    paso = 0.02
    while transcurrido < timeout:
        lider = cluster.obtener_lider()
        if lider:
            return lider
        await asyncio.sleep(paso)
        transcurrido += paso
    return None


async def main():
    logger.info("=" * 78)
    logger.info(" INICIO DE LA SIMULACIÓN RAFT - Clúster de 5 nodos")
    logger.info("=" * 78)

    cluster = Cluster(n_nodos=5)
    cluster.iniciar()

    logger.info("\n--- FASE 1: Elección de líder inicial ---\n")
    lider = await esperar_lider(cluster)
    if not lider:
        logger.error("No se logró elegir un líder. Fin de la simulación.")
        return
    await asyncio.sleep(0.2)  

    logger.info("\n--- FASE 2: Propuesta de valor 'A=1' al líder ---\n")
    lider = cluster.obtener_lider()
    await lider.proponer("A=1")
    await asyncio.sleep(0.3)

    logger.info("\nEstado de la máquina de estados en cada nodo tras la replicación:")
    for n in cluster.nodos.values():
        logger.info(f"  Nodo-{n.id} ({n.rol.value}): estado={n.estado_maquina()} commit_index={n.commit_index}")

    logger.info("\n--- FASE 3: Simulación de fallo del nodo líder ---\n")
    lider_caido_id = cluster.obtener_lider().id
    cluster.matar_nodo(lider_caido_id)

    logger.info("Esperando a que el clúster detecte la ausencia de latidos y elija nuevo líder...\n")
    nuevo_lider = await esperar_lider(cluster)
    if nuevo_lider:
        logger.info(f"\n*** Nuevo líder detectado: Nodo-{nuevo_lider.id} (term {nuevo_lider.term_actual}) ***\n")
    else:
        logger.error("El clúster no logró recuperar el consenso.")
        return
    await asyncio.sleep(0.2)

    logger.info("\n--- FASE 4: Nueva propuesta 'B=2' tras la recuperación ---\n")
    await nuevo_lider.proponer("B=2")
    await asyncio.sleep(0.3)

    logger.info("\nEstado final de la máquina de estados en cada nodo vivo:")
    for n in cluster.nodos.values():
        vivo_str = "VIVO" if n.vivo else "CAÍDO"
        logger.info(f"  Nodo-{n.id} [{vivo_str}] ({n.rol.value}): estado={n.estado_maquina()} commit_index={n.commit_index}")

    logger.info("\n--- FASE 5: El nodo caído se reincorpora al clúster ---\n")
    cluster.revivir_nodo(lider_caido_id)
    await asyncio.sleep(0.5)  

    logger.info("\nEstado final de TODO el clúster (incluye nodo recuperado):")
    for n in cluster.nodos.values():
        logger.info(f"  Nodo-{n.id} ({n.rol.value}): estado={n.estado_maquina()} commit_index={n.commit_index}")

    logger.info("\n" + "=" * 78)
    logger.info(" FIN DE LA SIMULACIÓN")
    logger.info("=" * 78)


if __name__ == "__main__":
    asyncio.run(main())
