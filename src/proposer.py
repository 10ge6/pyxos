from .id_proposta import IdProposta
from threading import Thread
import socket


class Proposer:
    def __init__(self, bridge_port:int, value_to_propose:str):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.bind(("localhost", 0))  # Bind the acceptor socket
        self._addr:tuple[str, int] = self._sock.getsockname()

        self.bridge_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.bridge_socket.connect(("localhost", bridge_port))  # Connect to the bridge
            print(f"Proposer connected to bridge on port {bridge_port}")
        except Exception as e:
            print(f"Error connecting to bridge: {e}")
        
        self._listner = Thread(target=self.listner_requests, daemon=True)
        self.bridge = bridge_port

        self.proposer_uid         = None
        self.quorum_size          = None

        self.proposed_value       = value_to_propose
        self.proposal_id          = None 
        self.last_accepted_id     = None
        self.next_proposal_number = 1
        self.promises_rcvd        = None

        self._paths = {
            "prm": self.recv_promise,
            "qrm": self.set_quorum
        }

        self.send_message_to_bridge("spp", "PROPOSER")

    def send_message_to_bridge(self, reqtype:str, *args:str):
        '''
        Envia mensagens para o bridge
        '''
        msg = (";".join((reqtype,)+args)+"!").encode()
        print(f"Proposer sending message to bridge: {msg}")

        def send():
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect(("localhost", self.bridge))
                    s.sendall(msg)
                    print(f"Proposer message sent to bridge: {msg}")
            except (BrokenPipeError, OSError) as e:
                print(f"Failed to send message to bridge at port {self.bridge}: {e}")
        
        Thread(target=send, daemon=True).start()
    
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
            print(f"Proposer received: {data} from {addr}")
            self._paths[data[0]](*data[1:])

    def prepare(self):
        '''
        Sends a prepare request to all Acceptors as the first step in attempting to
        acquire leadership of the Paxos instance. 
        '''
        #self.send_message_to_bridge("qrm")
        self.promises_rcvd = set()
        self.proposal_id   = IdProposta(self.next_proposal_number, self._addr[1])
        self.next_proposal_number += 1
        print(f"Proposer sending prepare request with proposal ID: {self.proposal_id}")
        self.send_message_to_bridge("prp", str(self.proposal_id))

    def recv_promise(self, from_port:str, proposal_id:str, prev_accepted_id:str, prev_accepted_value:str):
        '''
        Chamado quando uma promessa chega de um acceptor
        '''
        
        # Ignora mensagens antigas ou já recebidas do mesmo acceptor
        proposal_id = IdProposta(*map(int, proposal_id.split(':')))
        prev_accepted_id = IdProposta(*map(int, prev_accepted_id.split(':')))
        
        print(f"Proposer received promise from Acceptor at port {from_port} for proposal {proposal_id}")

        if proposal_id != self.proposal_id or from_port in self.promises_rcvd:
            return

        self.promises_rcvd.add(from_port)
        
        if prev_accepted_id > self.last_accepted_id:
            self.last_accepted_id = prev_accepted_id
            # Se o acceptor já aceitou um valor, o propositor deve mudar seu valor para o tal
            if prev_accepted_value:
                self.proposed_value = prev_accepted_value

        if len(self.promises_rcvd) >= self.quorum_size:
            if self.proposed_value is not None:
                print(f"Proposer sending accept request for proposal {self.proposal_id}")
                self.send_message_to_bridge("act", str(self.proposal_id), self.proposed_value)

    def set_quorum(self, value:str):
        self.quorum_size = int(value)
    
    def run(self):
        self._listner.start()
        self.prepare()
        self._listner.join()
