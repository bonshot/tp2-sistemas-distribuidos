import socket
from communications import read_socket, write_socket
from serialization import Message, is_EOF, get_EOF_client_id, split_message_info
from middleware import Middleware
import os
import signal
import queue
from multiprocessing import Process, Queue, Lock
from logger import Logger


SEND_COORDINATOR_QUEUE = 'query_coordinator'
RECEIVE_COORDINATOR_QUEUE = 'server' 
EOF_MSG = "EOF"
TITLES_FILE_IDENTIFIER = 't'
REVIEWS_FILE_IDENTIFIER = 'r'
CLIENTS = 'clients'
CLIENTS_QUANTITY = 'clients_quantity'
NO_STATE = -1
SENDING_DATA_STATE = 0
WAITING_RESULTS_STATE = 1
IP_INDEX = 0
ID_INDEX = 0
STATE_INDEX = 1


class Server:

    def __init__(self, host, port, listen_backlog, log):
        signal.signal(signal.SIGTERM, self.handle_signal)

        self.clients = {}
        self.results_p = None
        self._stop_server= False
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind((host, port))
        self._server_socket.listen(listen_backlog)
        self.clients_accepted = 1
        self.active_clients = {}                            # Key = client ip, Value = (client id, client state)
        self.sockets_queue = Queue()
        self.log = Logger(log, '0')
        self.log_lock = Lock()

    def handle_signal(self, *args):
        self._stop_server = True
        self.join_processes()
        self._server_socket.close()

    def join_processes(self):
        for process in self.clients.values():
            process.terminate()
            process.join()
            process.close()

        self.results_p.terminate()
        self.results_p.join()
        self.results_p.close()

    def persist_state(self):
        curr_state = {}
        curr_state[CLIENTS] = self.active_clients
        curr_state[CLIENTS_QUANTITY] = self.clients_accepted
        
        self.log_lock.acquire()
        self.log.persist(curr_state)
        self.log_lock.release()

    def initialize_state(self):
        prev_state = self.log.read_persisted_data()
        print('The prev state was: ', prev_state, flush=True)
        if prev_state != None:
            self.active_clients = prev_state[CLIENTS]
            self.clients_accepted = prev_state[CLIENTS_QUANTITY]

    def run(self):
        # Get the previous state if there was one
        self.initialize_state()

        # Create a process that will be to send the results back to the client
        self.results_p = Process(target=initiate_result_fordwarder, args=(self.sockets_queue, self.log, self.log_lock,))
        self.results_p.start()

        # Receive new clients and create a process that will handle them
        while not self._stop_server:
            try: 
                conn, addr = self._server_socket.accept()
            except OSError:
                break

            print("A new client has connected: ", addr)

            if addr[IP_INDEX] in self.active_clients:
                # The ip was already in our state, meaning the client had been already received before
                client_id = self.active_clients[addr[IP_INDEX]][ID_INDEX]
                client_state = self.active_clients[addr[IP_INDEX]][STATE_INDEX]
            else:
                # Send the new socket with its id through the Queue
                client_id = self.clients_accepted
                self.active_clients[addr[IP_INDEX]] = (str(client_id), SENDING_DATA_STATE)
                self.clients_accepted += 1
                self.persist_state()
                client_state = NO_STATE
            
            self.sockets_queue.put((conn, str(client_id)))
            if client_state != WAITING_RESULTS_STATE:
                # Start the process responsible for receiving the data from the client
                p = Process(target=initiate_data_fordwarder, args=(conn, client_id, self.log, self.log_lock,))
                p.start()
                self.clients[client_id] = p

def initiate_data_fordwarder(socket, client_id, log, log_lock):
    data_fordwarder = DataFordwarder(socket, client_id, log, log_lock)
    data_fordwarder.handle_client()

def initiate_result_fordwarder(sockets_queue, log, log_lock):
    result_fordwarder = ResultFordwarder(sockets_queue, log, log_lock)
    result_fordwarder.run()

class ResultFordwarder:

    def __init__(self, sockets_queue, log, log_lock):
        signal.signal(signal.SIGTERM, self.handle_signal)

        self.clients = {}
        self.sockets_queue = sockets_queue
        self.log = log
        self.log_lock = log_lock
        self.middleware = None
        self.queue = queue.Queue()
        
        try:
            middleware = Middleware(self.queue)
        except Exception as e:
            raise e
        self.middleware = middleware
        print("Middleware established the connection")

        self.results = Message("")

    def handle_signal(self, *args):
        self.queue.put('SIGTERM')
        try:
            if self.middleware != None:
                self.middleware.close_connection()
        except OSError:
            pass

    def run(self):
        self._receive_results()

    def read_results(self, method, body):
        """
        Each message contains the results of every query for
        a specific client.
        """
        client_id, result_msg = split_message_info(body)
        self._send_result(client_id, result_msg)

        self.update_state(client_id)

        self.middleware.ack_message(method)
   
    def _receive_results(self):
        callback_with_params = lambda ch, method, properties, body: self.read_results(method, body)
        self.middleware.receive_messages(RECEIVE_COORDINATOR_QUEUE, callback_with_params)
        self.middleware.consume()

    def _send_result(self, client_id, result_msg):
        # If the id is not in our clients dictionary, it MUST be on the sockets_queue
        while not self.sockets_queue.empty():
            new_client_socket, new_client_id = self.sockets_queue.get()
            self.clients[new_client_id] = new_client_socket

        client_socket = self.clients[client_id]

        e = write_socket(client_socket, result_msg)
        if e != None:
            # If the writing fails, its because the client crashed.
            # In this case we will do nothing and forget about 
            # the client. 
            return
        
        write_socket(client_socket, EOF_MSG)
        client_socket.close()
        del self.clients[client_id]

    def update_state(self, client_id):
        self.log_lock.acquire()
        prev_state = self.log.read_persisted_data()
        ip_to_delete = None
        for ip, tuple in prev_state[CLIENTS].items():
            if tuple[ID_INDEX] == client_id:
                ip_to_delete = ip
                break
        if ip_to_delete != None:
            del prev_state[CLIENTS][ip_to_delete]
            self.log.persist(prev_state)
        self.log_lock.release()

class DataFordwarder:

    def __init__(self, socket, id, log, log_lock):
        signal.signal(signal.SIGTERM, self.handle_signal)
        
        self.id = str(id)
        self._client_socket = socket
        self.log = log
        self.log_lock = log_lock
        self._stop_fordwarder = False
        
        self.middleware = None
        self.queue = queue.Queue()
        try:
            middleware = Middleware(self.queue)
        except Exception as e:
            raise e
        self.middleware = middleware
        print("Middleware established the connection")
        self.message_parser = Message()

    def handle_client(self):
        """
        Reads the client data and fordwards it to the corresponding parts of the system
        """
        self._receive_and_forward_data()
        try:
            self.middleware.close_connection()
        except OSError:
            pass

    def update_state(self):
        self.log_lock.acquire()
        prev_state = self.log.read_persisted_data()
        ip_to_update = None
        for ip, tuple in prev_state[CLIENTS].items():
            if tuple[ID_INDEX] == self.id:
                ip_to_update = ip
                break
        if ip_to_update != None:
            prev_value = prev_state[CLIENTS][ip_to_update]
            new_value = (prev_value[ID_INDEX], WAITING_RESULTS_STATE) 
            prev_state[CLIENTS][ip_to_update] = new_value
            print(f'Updating state of client [{self.id}]: ', prev_state, flush=True)
            self.log.persist(prev_state)
        self.log_lock.release()

    def _receive_and_forward_data(self):
        while not self._stop_fordwarder:
            socket_content, e = read_socket(self._client_socket)
            if e != None:
                raise e
            
            if not self.message_parser.is_EOF(socket_content):
                # Add the id to the message and send it
                msg_id, _, message = split_message_info(socket_content)
                self.message_parser.push(message)
                self.message_parser.add_ids(self.id, msg_id)
                self.middleware.send_message(SEND_COORDINATOR_QUEUE, self.message_parser.encode())
                # Send the ack
                write_socket(self._client_socket, 'ACK')
                # Clean it so the content of the next message doesnt mix with the current one
                self.message_parser.clean()
            else:
                # Add the file identifier to the EOF and send it
                self.message_parser.create_EOF(self.id, socket_content)
                self.middleware.send_message(SEND_COORDINATOR_QUEUE, self.message_parser.encode())
                # Update the state of the active client
                if self.message_parser.get_file_identifier(socket_content) ==  REVIEWS_FILE_IDENTIFIER:
                    self.update_state()
                    break
        
        self.message_parser.clean()
        print(f'CLIENT_{self.id} HAS FINISHED')

    def handle_signal(self, *args):
        print("Gracefully exit")
        self.queue.put('SIGTERM')
        self._stop_fordwarder = True
        try:
            if self.middleware != None:
                self.middleware.stop_consuming()
                self.middleware._connection.close()
        except:
            pass
        if self._client_socket != None:
            self._client_socket.close()


def main():    
    HOST, PORT, LISTEN_BACKLOG, LOG = os.getenv('HOST'), os.getenv('PORT'), os.getenv('LISTEN_BACKLOG'), os.getenv('LOG')
    server = Server(HOST, int(PORT), int(LISTEN_BACKLOG), LOG)
    server.run()

main()
    