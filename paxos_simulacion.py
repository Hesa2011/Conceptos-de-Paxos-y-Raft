import asyncio
import logging
import os
import random
import sys
from dataclasses import dataclass

LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "logs", "paxos_log.txt")

logger = logging.getLogger("PAXOS")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("[t=%(relativeCreated)6dms] %(message)s")
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
file_handler = logging.FileHandler(LOG_PATH, mode="w", encoding="utf-8")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

RPC_DELAY_RANGE = (0.01, 0.05)


@dataclass
class Propuesta:
    numero: int
    valor: str


class Acceptor:
    """Nodo que participa como Acceptor (y también actúa como Learner al
    anunciar cuando aprende el valor consensuado)."""

    def __init__(self, id_):
        self.id = id_
        self.vivo = True
        self.promesa_maxima = -1    
        self.aceptada: Propuesta = None  

    def log_evento(self, msg):
        estado = "VIVO" if self.vivo else "CAÍDO"
        logger.info(f"[Acceptor-{self.id} | {estado}] {msg}")

    async def recibir_prepare(self, n):
        if not self.vivo:
            return None  
        await asyncio.sleep(random.uniform(*RPC_DELAY_RANGE))
        if n > self.promesa_maxima:
            self.promesa_maxima = n
            self.log_evento(f"PROMISE({n}) concedida" +
                             (f" | ya tenía aceptada n={self.aceptada.numero} valor='{self.aceptada.valor}'"
                              if self.aceptada else " | sin propuesta previa aceptada"))
            return ("PROMISE", n, self.aceptada)
        self.log_evento(f"RECHAZA prepare({n}); ya prometió a n={self.promesa_maxima}")
        return ("REJECT", self.promesa_maxima, None)

    async def recibir_accept(self, n, valor):
        if not self.vivo:
            return None
        await asyncio.sleep(random.uniform(*RPC_DELAY_RANGE))
        if n >= self.promesa_maxima:
            self.promesa_maxima = n
            self.aceptada = Propuesta(n, valor)
            self.log_evento(f"ACCEPTED(n={n}, valor='{valor}')")
            return ("ACCEPTED", n, valor)
        self.log_evento(f"RECHAZA accept(n={n}); ya prometió a n={self.promesa_maxima}")
        return ("REJECT", self.promesa_maxima, None)


class Proposer:
    def __init__(self, id_, acceptors):
        self.id = id_
        self.acceptors = acceptors
        self.contador_propuesta = 0

    def log_evento(self, msg):
        logger.info(f"[Proposer-{self.id}] {msg}")

    def _siguiente_numero(self):
        self.contador_propuesta += 1
        return self.contador_propuesta * 100 + self.id

    async def proponer(self, valor_deseado):
        n = self._siguiente_numero()
        mayoria = (len(self.acceptors) // 2) + 1

        self.log_evento(f"--- FASE 1 (PREPARE) --- envía PREPARE(n={n}) a {len(self.acceptors)} acceptors")
        respuestas = await asyncio.gather(*[a.recibir_prepare(n) for a in self.acceptors])
        promesas = [r for r in respuestas if r is not None and r[0] == "PROMISE"]

        if len(promesas) < mayoria:
            self.log_evento(f"NO alcanzó mayoría en PREPARE ({len(promesas)}/{len(self.acceptors)}); propuesta abortada")
            return None

        propuestas_previas = [p[2] for p in promesas if p[2] is not None]
        if propuestas_previas:
            mas_reciente = max(propuestas_previas, key=lambda p: p.numero)
            valor_final = mas_reciente.valor
            self.log_evento(f"detecta valor previamente aceptado ('{valor_final}'); lo adopta por seguridad")
        else:
            valor_final = valor_deseado

        self.log_evento(f"--- FASE 2 (ACCEPT) --- envía ACCEPT(n={n}, valor='{valor_final}')")
        respuestas2 = await asyncio.gather(*[a.recibir_accept(n, valor_final) for a in self.acceptors])
        aceptados = [r for r in respuestas2 if r is not None and r[0] == "ACCEPTED"]

        if len(aceptados) >= mayoria:
            self.log_evento(f"*** VALOR CONSENSUADO (CHOSEN): '{valor_final}' "
                             f"({len(aceptados)}/{len(self.acceptors)} acceptors) ***")
            return valor_final
        else:
            self.log_evento(f"NO alcanzó mayoría en ACCEPT ({len(aceptados)}/{len(self.acceptors)}); propuesta abortada")
            return None


async def main():
    logger.info("=" * 78)
    logger.info(" INICIO DE LA SIMULACIÓN PAXOS (single-decree) - 3 Acceptors")
    logger.info("=" * 78)

    acceptors = [Acceptor(i) for i in range(3)]

    logger.info("\n--- Ronda 1: los 3 acceptors están disponibles ---\n")
    proposer1 = Proposer(id_=1, acceptors=acceptors)
    valor1 = await proposer1.proponer("A=1")
    logger.info(f"\nResultado ronda 1: valor consensuado = {valor1}\n")

    logger.info("--- Simulación de fallo: Acceptor-2 deja de responder ---\n")
    acceptors[2].vivo = False
    acceptors[2].log_evento("### NODO CAÍDO (simulación de fallo) ###")

    logger.info("\n--- Ronda 2: nueva propuesta con un acceptor caído (se mantiene el quórum 2/3) ---\n")
    proposer2 = Proposer(id_=2, acceptors=acceptors)
    valor2 = await proposer2.proponer("A=1")  
    logger.info(f"\nResultado ronda 2: valor consensuado = {valor2}\n")

    logger.info("=" * 78)
    logger.info(" FIN DE LA SIMULACIÓN PAXOS")
    logger.info("=" * 78)


if __name__ == "__main__":
    asyncio.run(main())
