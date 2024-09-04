from threading import Thread
from .id_proposta import IdProposta
import socket


class Acceptor:
    def __init__(self, bridge_port:int):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.bind(("localhost", 0))  # Bind the acceptor socket
        self._addr:tuple[str, int] = self._sock.getsockname()
        
        # Immediately start listening for incoming connections
        self._sock.listen()  
        print(f"Acceptor listening for messages on {self._addr}")

        self.bridge_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.bridge_socket.connect(("localhost", bridge_port))  # Establish a connection to the bridge
        
        self._listner = Thread(target=self.listner_requests, daemon=True)
        self.bridge = bridge_port
        
        self._listner.start()

        self.messenger      = None    
        self.promised_id    = None
        self.accepted_id    = None
        self.accepted_value = None

        self._paths = {
            "prp": self.recv_prepare,
            "act": self.recv_accept_request
        }

        self.send_message_to_bridge("spp", "ACCEPTOR", str(self._addr[1]))
    
    def send_message_to_bridge(self, reqtype:str, *args:str):
        '''
        Envia mensagens para o bridge
        '''
        msg = (";".join((reqtype,)+args)+"!").encode()
        print(f"Acceptor sending message to bridge: {msg}")

        def send():
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect(("localhost", self.bridge))
                    s.sendall(msg)
                    print(f"Acceptor message sent to bridge: {msg}")
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
            print(f"Acceptor received: {data} from {addr}")
            self._paths[data[0]](*data[1:])
            
    def recv_prepare(self, from_port: int, proposal_id: str):
        '''
        Called when a Prepare message is received from a Proposer
        Chamado quando um Prepare é enviado de um proposer
        '''
        
        proposal_id = IdProposta(*map(int, proposal_id.split(':')))
        
        print(f"Acceptor received prepare request: {proposal_id} from Proposer at port {from_port}")
        
        if proposal_id > self.promised_id:
            self.promised_id = proposal_id
        
        print(f"Acceptor promising to proposal: {proposal_id}")
        
        self.send_message_to_bridge("spm", from_port, str(proposal_id), str(self.accepted_id), self.accepted_value)
                    
    def recv_accept_request(self, proposal_id: str, value:str):
        '''
        Chamado quando um accept é recebido de um proposer
        '''
        proposal_id = IdProposta(*map(int, proposal_id.split(':')))

        print(f"Acceptor received accept request for proposal {proposal_id} with value {value}")
        
        if proposal_id >= self.promised_id:
            self.promised_id     = proposal_id
            self.accepted_id     = proposal_id
            self.accepted_value  = value
            print(f"Acceptor accepted proposal {proposal_id} with value {value}")
            self.send_message_to_bridge("sad", str(proposal_id), value)
    
    def run(self):
        '''
        Runs the acceptor to listen for messages from the bridge
        '''
        self._sock.listen()
        print(f"Acceptor listening for messages on {self._addr}")

        while True:
            skt, addr = self._sock.accept()  # Keep accepting new messages
            msg = b''
            for b in iter(lambda: skt.recv(1), b'!'):
                msg += b
            data = msg.decode().split(';')
            print(f"Acceptor received: {data} from {addr}")

            # Handle Prepare message
            if data[0] == 'prp':
                self.recv_prepare(data[1], data[2])  # Process Prepare message
            skt.close()
        
