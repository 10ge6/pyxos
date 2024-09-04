from threading import Thread
from .id_proposta import IdProposta
import socket


class Learner:
    def __init__(self, bridge_port: int):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.bind(("localhost", 0))  # Bind the acceptor socket
        self._addr:tuple[str, int] = self._sock.getsockname()

        self.bridge_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.bridge_socket.connect(("localhost", bridge_port))  # Establish a connection to the bridge
        
        self._listner = Thread(target=self.listner_requests, daemon=True)
        self.bridge = bridge_port

        quorum_size       = None
        proposals         = None # maps proposal_id => [accept_count, retain_count, value]
        acceptors         = None # maps from_uid => last_accepted_proposal_id
        final_value       = None
        final_proposal_id = None

        self._paths = {
            "acd": self.recv_accepted,
            "qrm": self.set_quorum
        }

        self.send_message_to_bridge("spp", "LEARNER")

    def send_message_to_bridge(self, reqtype:str, *args:str):
        '''
        Envia mensagens para o bridge
        '''
        msg = (";".join((reqtype,)+args)+"!").encode()
        print(f"Learner sending message to bridge: {msg}")

        def send():
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect(("localhost", self.bridge))
                    s.sendall(msg)
                    print(f"Learner message sent to bridge: {msg}")
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
            print(f"Learner received: {data} from {addr}")
            self._paths[data[0]](*data[1:])

    @property
    def complete(self):
        return self.final_proposal_id is not None


    def recv_accepted(self, from_port, proposal_id, accepted_value):
        '''
        Called when an Accepted message is received from an acceptor
        '''
        
        print(f"Learner received accepted value {accepted_value} for proposal {proposal_id} from Acceptor at port {from_port}")
        
        if self.final_value is not None:
            return # already done

        if self.proposals is None:
            self.proposals = dict()
            self.acceptors = dict()
        
        last_pn = self.acceptors.get(from_port)

        if not proposal_id > last_pn:
            return # Old message

        self.acceptors[ from_port ] = proposal_id
        
        if last_pn is not None:
            oldp = self.proposals[ last_pn ]
            oldp[1] -= 1
            if oldp[1] == 0:
                del self.proposals[ last_pn ]

        if not proposal_id in self.proposals:
            self.proposals[ proposal_id ] = [0, 0, accepted_value]

        t = self.proposals[ proposal_id ]

        assert accepted_value == t[2], 'Value mismatch for single proposal!'
        
        t[0] += 1
        t[1] += 1

        if t[0] == self.quorum_size:
            self.final_value       = accepted_value
            self.final_proposal_id = proposal_id
            self.proposals         = None
            self.acceptors         = None
            
            print(f"Learner reached consensus on value {accepted_value} for proposal {proposal_id}")

            # self.messenger.on_resolution( proposal_id, accepted_value )
    
    def set_quorum(self, value:str):
        self.quorum_size = int(value)

    def run(self):
        self._listner.start()
        self._listner.join()
