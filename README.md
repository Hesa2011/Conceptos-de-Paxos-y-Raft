# Conceptos-de-Paxos-y-Raft
Simulación de consenso distribuido con Raft y Paxos en Python (asyncio): elección de líder, replicación de log y tolerancia a fallos.
# Consenso Distribuido — Paxos y Raft (Actividad Semana 12)

Prototipo que simula el problema del **consenso distribuido**
mediante dos implementaciones independientes en Python (asyncio):

- `prototipo/raft_simulacion.py` — Simulación principal: clúster de **5 nodos**
  ejecutando RAFT (elección de líder + replicación de log), incluyendo la
  simulación de la caída del nodo líder y su posterior recuperación.
- `prototipo/paxos_simulacion.py` — Simulación complementaria: **3 nodos
  Acceptor** ejecutando Paxos de decreto único (fases *Prepare/Promise* y
  *Accept/Accepted*), incluyendo la caída de un Acceptor y el consenso con
  quórum reducido (2/3).

## Requisitos

- Python 3.10 o superior (no requiere librerías externas, solo `asyncio` de
  la biblioteca estándar).

## Cómo ejecutar

```bash
# Clonar el repositorio
git clone <URL_DEL_REPOSITORIO>
cd consenso-distribuido-paxos-raft

# Ejecutar la simulación de RAFT (5 nodos)
python3 prototipo/raft_simulacion.py

# Ejecutar la simulación de PAXOS (3 acceptors)
python3 prototipo/paxos_simulacion.py
```

Cada script imprime el log de ejecución en consola y además lo guarda en
`logs/raft_log.txt` y `logs/paxos_log.txt` respectivamente.

Para regenerar las capturas de pantalla (imágenes tipo terminal) usadas en
el informe, con los logs ya generados:

```bash
python3 generar_capturas.py
```

## Estructura del repositorio

```
consenso-distribuido-paxos-raft/
├── README.md
├── informe_consenso_distribuido.docx   # Investigación, comparativa y reporte
├── generar_capturas.py                 # Utilidad para generar capturas
├── prototipo/
│   ├── raft_simulacion.py
│   └── paxos_simulacion.py
├── logs/
│   ├── raft_log.txt
│   ├── console_output.txt
│   ├── paxos_log.txt
│   └── paxos_console_output.txt


## Resumen de lo que demuestra el prototipo

**RAFT (5 nodos):**
1. Los 5 nodos arrancan como `FOLLOWER`; al expirar el temporizador aleatorio
   de elección, un nodo se postula `CANDIDATE` y gana la mayoría de votos,
   convirtiéndose en `LEADER`.
2. El líder recibe la propuesta del cliente `A=1`, la agrega a su log y la
   replica a los followers mediante `AppendEntries`. Al confirmar la mayoría,
   la entrada se marca como *committed* y se aplica en todos los nodos.
3. Se simula la caída del líder (deja de responder RPCs). Los followers
   dejan de recibir heartbeats, expira su temporizador y se dispara una
   nueva elección; un nuevo líder es elegido automáticamente.
4. El nuevo líder recibe una segunda propuesta `B=2` y el clúster sigue
   funcionando con normalidad.
5. El nodo caído se reincorpora, recibe los `AppendEntries` pendientes y se
   pone al día con el resto del clúster (log = `['A=1', 'B=2']`).

**PAXOS (3 acceptors):**
1. Un *Proposer* ejecuta `PREPARE(n)` → `PROMISE` con la mayoría de
   acceptors, y luego `ACCEPT(n, valor)` → `ACCEPTED`, logrando el consenso
   sobre `A=1`.
2. Se simula la caída de un Acceptor. Un segundo *Proposer* repite el
   proceso y logra consenso igualmente, dado que el quórum de 2/3 acceptors
   vivos sigue siendo mayoría.

