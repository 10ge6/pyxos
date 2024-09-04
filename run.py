import time
from multiprocessing import Process
from random import randint
from src import *

bridge = Bridge()
nodes = []

time.sleep(1)  # Small delay to allow the bridge to set up

for _ in range(randint(1, 1)):
    nodes.append(Proposer(bridge.port, "value1"))

for _ in range(randint(1, 5)):
    nodes.append(Acceptor(bridge.port))

for _ in range(randint(1, 10)):
    nodes.append(Learner(bridge.port))

for n in nodes:
    p = Process(target=n.run, daemon=True)
    p.start()
