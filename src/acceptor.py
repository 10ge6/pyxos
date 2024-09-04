from threading import Thread
from .id_proposta import IdProposta
import socket


class Acceptor:
    def __init__(self, bridge_port:int):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.bind(("localhost"), 0)
        self._addr:tuple[str, int] = self._sock.getsockname()
        self._listner = Thread(target=self.listner_requests, daemon=True)
        self.bridge = bridge_port

        self.messenger      = None    
        self.promised_id    = None
        self.accepted_id    = None
        self.accepted_value = None

        self._paths = {
            "prp": self.recv_prepare,
            "act": self.recv_accept_request
        }

        self.send_message_to_bridge("spp", "ACCEPTOR")
    
    def send_message_to_bridge(self, reqtype:str, *args:str):
        '''
        Envia mensagens para o bridge
        '''

        msg = (";".join((reqtype,)+args)+"!").encode()
        Thread(target=self._sock.sendto, args=(msg, ("localhost", self.bridge)), daemon=True).start()
    
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
            self._paths[data[0]](*data[1:])
            
    def recv_prepare(self, from_port: int, proposal_id: str):
        '''
        Called when a Prepare message is received from a Proposer
        Chamado quando um Prepare é enviado de um proposer
        '''
        proposal_id = IdProposta(*map(int, proposal_id.split(':')))
        
        if proposal_id > self.promised_id:
            self.promised_id = proposal_id
        
        self.send_message_to_bridge("spm", from_port, str(proposal_id), str(self.accepted_id), self.accepted_value)
                    
    def recv_accept_request(self, proposal_id: str, value:str):
        '''
        Chamado quando um accept é recebido de um proposer
        '''
        proposal_id = IdProposta(*map(int, proposal_id.split(':')))

        if proposal_id >= self.promised_id:
            self.promised_id     = proposal_id
            self.accepted_id     = proposal_id
            self.accepted_value  = value
            self.send_message_to_bridge("sad", str(proposal_id), value)
    
    def run(self):
        self._listner.start()
        self._listner.join()
