from multiprocessing import Process
from random import randint
from src import *

bridge = Bridge()
nodes = []

for _ in range(randint(1, 1)):
    nodes.append(Proposer(bridge.port))

for _ in range(randint(1, 5)):
    nodes.append(Acceptor(bridge.port))

for _ in range(randint(1, 10)):
    nodes.append(Learner(bridge.port))

for n in nodes:
    p = Process(target=n.run, daemon=True)
    p.start()