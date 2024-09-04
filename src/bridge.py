from threading import Thread
import socket

class Bridge:
    def __init__(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.bind(("localhost", 0))
        self._addr:tuple[str, int] = self._sock.getsockname()
        self._sock.listen()  # Make sure the socket is listening for connections
        print(f"Bridge is listening on {self._addr}")
        self._listner = Thread(target=self.listner_requests, daemon=True)
        self._listner = Thread(target=self.listner_requests, daemon=True)

        # Lista dos participantes do paxos. Cada lista recebe as portas dos acceptors, proposers e learners, respectivamente
        self._acceptors:list[int] = []
        self._proposers:list[int] = []
        self._learners:list[int] = []

        self._paths = {
            "reg": self.register,
            "spp": self.send_prepare,
            "spm": self.send_promise,
            "act": self.send_accept,
            "sad": self.send_accepted,
            "qrm": self.quorum_size
        }
    
    def close(self):
        '''
        Close the socket when done
        '''
        if self._sock:
            self._sock.close()
            print(f"Closed socket at {self._addr}")
        
    def handle_client(self, client_socket):
        '''
        Handle communication with a single client
        '''
        msg = b''
        for b in iter(lambda: client_socket.recv(1), b'!'):
            msg += b
        data = msg.decode().split(';')
        print(f"Bridge received: {data} from {client_socket.getpeername()}")
        # Add logic to handle received messages
        if data[0] == 'prp':  # If it's a prepare request
            self.send_prepare(client_socket.getpeername()[1], data[1])
        elif data[0] == 'prm':  # Promise message
            self.send_promise(client_socket.getpeername()[1], *data[1:])
        elif data[0] == 'act':  # Accept request
            self.send_accept(client_socket.getpeername()[1], *data[1:])
        elif data[0] == 'sad':  # Accepted message
            self.send_accepted(client_socket.getpeername()[1], *data[1:])
        else:
            print(f"Unknown message type: {data}")
    
        client_socket.close()
    
    def listner_requests(self):
        '''
        Escuta todas as requisições que chegam
        '''

        self._sock.listen()
        while True:
            skt, addr = self._sock.accept()
            print(f"Accepted connection from {addr}")
            Thread(target=self.handle_client, args=(skt,), daemon=True).start()

    def send_message(self, port:int, reqtype:str, *args:str):
        '''
        Envia mensagens para os outros nós
        '''
        msg = (";".join((reqtype,)+args)+"!").encode()
        print(f"Bridge sending message: {msg} to port {port}")

        def send():
            try:
                if self._sock.fileno() == -1:
                    print(f"Socket is closed, cannot send message: {reqtype}")
                    return

                self._sock.sendto(msg, ("localhost", port))
                print(f"Message sent to node at port {port}: {reqtype}")
            except (BrokenPipeError, OSError) as e:
                print(f"Failed to send message to node at port {port}: {e}")
            
        Thread(target=send, daemon=True).start()


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
        print(f"Bridge routing prepare request from Proposer at port {port} with proposal ID {id_proposal}")
        for acc_port in self._acceptors:
            self.send_message(acc_port, "prp", str(port), str(id_proposal))

    def send_promise(self, port:int, prop_port: int, id_proposal:str, previous_id:str, accepted_value:str):
        '''
        Envia uma promessa para um propositor específico
        '''
        print(f"Bridge routing promise from Acceptor at port {port} to Proposer at port {prop_port}")
        self.send_message(prop_port, "prm", port, id_proposal, previous_id, accepted_value)

    def send_accept(self, port:int, id_proposal, proposal_value):
        '''
        Envia uma mensagem de aceitação para todos os acceptors
        '''
        print(f"Bridge routing accept request for proposal {id_proposal} with value {proposal_value}")
        for acc_port in self._acceptors:
            self.send_message(acc_port, "act", id_proposal, proposal_value)

    def send_accepted(self, port:int, id_proposal:str, accepted_value: str):
        '''
        Envia uma mensagem de aceitação para todos os Learners
        '''
        print(f"Bridge routing accepted notification for proposal {id_proposal} with value {accepted_value}")
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
