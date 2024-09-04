from threading import Thread
import socket

class Bridge:
    def __init__(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.bind(("localhost"), 0)
        self._addr:tuple[str, int] = self._sock.getsockname()
        self._listner = Thread(target=self.listner_requests, daemon=True)

        # Lista dos participantes do paxos. Cada lista recebe as portas dos acceptors, proposers e learners, respectivamente
        self._acceptors:list[int] = []
        self._proposers:list[int] = []
        self._learners:list[int] = []

        self._paths = {
            "reg": self.register,
            "spp": self.send_prepare,
            "spm": self.send_promise,
            "sat": self.send_accept,
            "sad": self.send_accepted,
            "qrm": self.quorum_size
        }

    def listner_requests(self):
        '''
        Escuta todas as requisições que chegam
        '''

        self._sock.listen()
        while True:
            skt, addr = self._sock.accept()
            msg = b''
            for b in iter(lambda: skt.recv(1), b'!'):
                msg += b
            data = msg.decode().split(';')
            self._paths[data[0]](addr[1], *data[1:])

    def send_message(self, port:int, reqtype:str, *args:str):
        '''
        Envia mensagens para os outros nós
        '''

        msg = (";".join((reqtype,)+args)+"!").encode()
        Thread(target=self._sock.sendto, args=(msg, ("localhost", port)), daemon=True).start()

    def register(self, port:int, node_type:str):
        '''
        Recebe informações de um nó indicando se o mesmo é um acceptor, proposer ou learner
        '''

        types = {
            "ACCEPTOR": self._acceptors,
            "PROPOSER": self._proposers,
            "LEARNER": self._learners
        }
        
        types[node_type].append(port)

    def send_prepare(self, port:int, id_proposal: str):
        '''
        Envia para todos os acceptors uma mensagem de preparação
        '''

        for acc_port in self._acceptors:
            self.send_message(acc_port, "prp", str(port), str(id_proposal))

    def send_promise(self, port:int, prop_port: int, id_proposal:str, previous_id:str, accepted_value:str):
        '''
        Envia uma promessa para um propositor específico
        '''
        self.send_message(prop_port, "prm", port, id_proposal, previous_id, accepted_value)

    def send_accept(self, port:int, id_proposal, proposal_value):
        '''
        Envia uma mensagem de aceitação para todos os acceptors
        '''

        for acc_port in self._acceptors:
            self.send_message(acc_port, "act", id_proposal, proposal_value)

    def send_accepted(self, port:int, id_proposal:str, accepted_value: str):
        '''
        Envia uma mensagem de aceitação para todos os Learners
        '''

        for lrn_port in self._learners:
            self.send_message(lrn_port, "acd", port, id_proposal, accepted_value)
    
    def quorum_size(self, port:int):
        q = len(self._acceptors) // 2
        self.send_message(port, "qrm", str(q))

    # def on_resolution(self, port:int, proposal_id, value):
    #     '''
    #     Called when a resolution is reached
    #     '''
    
    @property
    def port(self):
        return self._addr[1]
    
    def run(self):
        self._listner.start()
        self._listner.join()