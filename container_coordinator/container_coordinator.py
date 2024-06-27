import socket
from multiprocessing import Process, Manager, Queue
from communications import read_socket, write_socket
import random
import os
import docker
import time
import signal
from healthchecking import HealthChecker, HealthCheckHandler

CONNECTION_TRIES = 20
LOOP_CONNECTION_PERIOD = 2
HOST_INDEX = 0
PORT_INDEX = 1
RECONNECTION_SLEEP = 5
QUEUE_SIZE = 10
END_MSG = 'END'
CONTAINERS_LIST = "containers_list.config"
WORKERS_PORT = 4321
HC_PORT = 2345

class ProcessCreator:
    """
    Classes that create other process need some simple and common functionality.
    They will inherit this from ProcessCreator which will have the fundamentals
    for the handling of new process.
    For example, joining the processes, handling their messages, or adding their
    connection info.
    """

    def __init__(self):
        self.processes = []

    def add_connection(self, connections, conn_identifier, conn):
        """
        Adds a connection to a process shared dict. This dict will have the 
        necessary info for processes to communicate with any socket they need.
        """
        connections[conn_identifier] = conn
    
    def initiate_connection(self, id, sock, connections, current_connection=None, leader=None, should_close=False):
        """
        Handler of messages of a connection. This connection can be another ContainerCoordinator
        or a worker from the system.
        """
        try:
            while True:
                # Use the sock here (messages between coords)
                if current_connection:
                    sock = connections[current_connection]
                msg, err = read_socket(sock)
                if err:
                    raise err
                print(f"Message received from {id}: {msg}")
                if msg.startswith('ELECTION'):
                    # Bully leader election
                    # Propagate the message to the other coordinators
                    count = 0
                    for name, conn in connections.items():
                        print(f'Connections: {connections}')
                        if not name.isdigit() or int(name) < int(id.split('_')[-1]):
                            continue
                        try:
                            print(f'Propagating election message to {name}')
                            print(id)
                            err = write_socket(conn, f"ELECTION {id.split('_')[-1]}")
                            if err:
                                raise err
                            count += 1 # If write_socket doesn't raise an exception, the message was sent
                        except:
                            print(f"Error sending election message to {name}, most probably died, skipping")
                            continue
                    if count == 0: # If no one could receive the message, I am the new leader
                        print("I am the new leader because no one could receive the election message")
                        for name, conn in connections.items():
                            if not name.isdigit():
                                continue
                            try:
                                write_socket(conn, f"COORDINATOR {id.split('_')[-1]}")
                            except:
                                print(f"Error sending coordinator message to {name}, most probably died, skipping")
                                continue
                        leader.value = True
                        # break
                elif msg.startswith('COORDINATOR'):
                    # There is a new coordinator in the network I have to begin listening to his healthchecks
                    print(f"Message received from a new coordinator with id {msg.split(' ')[1]}")
                    leader.value = False
                    should_close.value = True
                        
                # if leader.value:
                #     # I am the new leader, break this loop to begin healthchecking outside this connection
                #     break
            # if health_check_handler:
            #     health_check_handler.close()
                    
        except Exception as e:
            print(f"Exception in initiate_connection for id {id}: {e}")
            print(current_connection)
            if current_connection in connections:
                connections.pop(current_connection)
    

    def join_processes(self):
        """
        Every class that manages processes needs to join them and close them.
        """
        
        for p in self.processes:
            try:
                p.join()
                p.close()
            except Exception as e:
                print(f"Error joining process: {e}")
                continue


class Connector(ProcessCreator):
    """
    Responsible for connecting either to other ContainerCoordinators or workers.
    Every ContainerCoodinator needs to be connected to every other ContainerCoodinator.
    This means that some of them will wait for connections, and others will be responsible for connecting.
    
    Reconnection to the network is necessary if a ContainerCoordinator or worker wants to get back to the network.
    If this is the case, he will need to connect back to every other ContainerCoordinator. 
    In case of being a worker the healthchecker will be responsible for restarting the container.
    """

    def __init__(self, id, connections, coordinators_list = []):
        self.id = id
        self.connections = connections
        self.coordinators_list = coordinators_list
        # List to have all the created processes to join them later
        self.processes = []

    def connect_to_coordinators(self, reconnection, port, leader=False, should_close=False):
        """
        Connect to the other ContainerCoordinators.
        
        If reconnection = False, he will need to connect to every ContainerCoordinator
        that has a bigger id. (This is how the connection is configured)

        If reconnection = True, he will need to connect back to all the ContainerCoordinators.
        """
        if reconnection: # Check if all the connectionss are already done to avoid reconnection
            time.sleep(RECONNECTION_SLEEP)
            print(f"INICIO RECONECCION, connections: {len(self.connections)}  list: {len(self.coordinators_list) - 1}")
            print(f"Connections: {self.connections}")
            if len(self.connections) >= len(self.coordinators_list) - 1:
                print('NO SE HACE LA RECONEXION')
                return
            coordinators_list = self.coordinators_list
        else:
            coordinators_list = self.coordinators_list[self.id + 1:]

        for host, id in coordinators_list:
            if str(id) not in self.connections and self.id != id:
                for _ in range(CONNECTION_TRIES):            
                    try:
                        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        print(f'Soy {self.id} y me voy a conectar a: {id} con host: {host} y port: {port}')
                        s.connect((host, port))
                        print(f'Soy {self.id} y me CONECTE a: ', id)
                        self.add_connection(self.connections, str(id), s)
                        # We send our id and create a process to handle the connection
                        err = write_socket(s, str(self.id))
                        if err != None:
                            print('Error in socket write:', err)
                            raise err
                        print("Soy un coordinador mas, paso el id: ", host)
                        p = Process(target=self.initiate_connection, args=(f'container_coordinator_{self.id}', s, self.connections, str(id), leader, should_close))
                        p.start()
                        self.processes.append(p)
                        if id > self.id:
                            print(f'checking for greater id while connecting --> {id} > {self.id}')
                            leader.value = False
                            should_close.value = True
                            print("I have to begin a new election, because I connected to a coordinator with a bigger id")
                            for name, conn in self.connections.items():
                                if not name.isdigit() or int(name) < self.id:
                                    continue
                                try:
                                    print(self.id)
                                    write_socket(conn, f"ELECTION {self.id}")
                                except:
                                    print(f"Error sending election message to {name}, most probably died, skipping")
                                    continue
                        break   
                    except Exception as e:
                        print(f"No se pudo conectar al coordinator {id}. Error: ", e)
                        time.sleep(LOOP_CONNECTION_PERIOD)
                        continue
        self.join_processes()
    

class ContainerCoordinator(ProcessCreator):
    """
    Responsible for handling connections from other ContainerCoordinators or
    workers from the system. It has a leader in his network. If he is the leader,
    and one of his connection shutdowns, it needs to get that container back and running.
    """

    def __init__(self, id, address, listen_backlog, coordinators_list, containers_list, coords_port, workers_port):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.bind(address)
        self._socket.listen(listen_backlog)
        self.container_coordinators = {}
        self.containers = {}
        self.stop = False
        self.manager = Manager()
        self.connections = self.manager.dict() # { identifier: TCPsocket, ...}
        self.id = id
        # List of containers that the coordinator is responsible for
        self.containers_list = containers_list
        self.coordinators_list = coordinators_list
        self.workers_port = workers_port
        self.coords_port = coords_port
        # This list of tuples has the address of the other coordinators with their id [(host1, port1, id1), (host2, port2, id2), ...]
        self.health_checking_processes = []
        # List to have all the created processes to join them later
        self.processes = []
        self.health_check_sockets = self.manager.dict()
        self.hc_socket = None
        self.hc_conn = None
        self.should_close = self.manager.Value('b', True)
        self.leader = self.manager.Value('b', False)

    def im_last_coord(self):
        """
        Returns True if the coord is the one with the biggest id
        """
        return self.id == len(self.coordinators_list) - 1
    
    def create_connector(self, coord_id, connections, coordinators_list, reconnection, leader=None, should_close=False):
        connector = Connector(coord_id, connections, coordinators_list)
        connector.connect_to_coordinators(reconnection, self.coords_port, leader, should_close) 
    
    def initiate_reconnection(self):
        """
        For a reconnection, a new process needs to run Connector instance with
        reconnection parameter set to True.
        """
        self.initiate_connector(True, self.leader, self.should_close)

    def initiate_connector(self, reconnection, leader=None, should_close=False):
        """
        Creates, starts and saves a process that runs a Connector instance.
        """
        p = Process(target=self.create_connector, args=(self.id, self.connections, self.coordinators_list, reconnection, leader, should_close))
        p.start()
        self.processes.append(p)
    
    def close_resources(self):
        """
        Close the socket and the processes
        """
        self.join_processes()
        for conn in self.health_check_sockets.values():
            conn.close()
        self._socket.close()

    def create_health_check_handling(self):
        self.hc_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.hc_socket.bind((f'container_coordinator_{self.id}', HC_PORT)) 
        self.hc_socket.listen(1)
        self.health_check_handler = HealthCheckHandler(socket=self.hc_socket, conn=None)
        hc_process = Process(target=self.health_check_handler.handle_health_check_with_timeout, args=(5,str(self.id), self.connections,))
        hc_process.start()
        self.processes.append(hc_process)

    def create_health_check(self, identifier, health_checker, coord = True, health_check_sockets = {}, should_close = False):
        signal.signal(signal.SIGTERM, health_checker.handle_signal)
        hostname = f'container_coordinator_{identifier}' if coord else identifier
        port = HC_PORT if coord else self.workers_port
        conn = None 
        try:
            conn = health_checker.reconnect_with_backoff(hostname, port, max_retries= 3 if coord else 5)
        except:
            print(f"Could not connect to {hostname} for healthchecking")
        if conn:
            health_check_sockets[hostname] = conn
        health_checker.check_connection(hostname, port, conn, coord, should_close, health_check_sockets)

    def setup_new_leader(self):
        print("I became the new leader, starting healthchecking with coords and workers")
        # Start the healthchecking process with the other coordinators
        self.should_close.value = False
        for name in self.coordinators_list:
            current_coord = name[0]
            current_coord_id = name[1]
            if current_coord_id == self.id:
                continue
            try:
                health_checker = HealthChecker()
                p = Process(target=self.create_health_check, args=(current_coord_id, health_checker, True, self.health_check_sockets, self.should_close))
                self.health_checking_processes.append(p)
                p.start()
                print(f"Started healthchecking with coord {current_coord}")
            except Exception as e:
                print(f"Could not connect to {current_coord} for healthchecking, restarting and reconnecting to the coordinator, error: {e}")

        # Also I will connect to the workers and start the healthchecking process with them
        for container in self.containers_list:
            if "container_coordinator" in container:
                continue
            try:
                health_checker = HealthChecker()
                p = Process(target=self.create_health_check, args=(container, health_checker, False, self.health_check_sockets, self.should_close))
                self.health_checking_processes.append(p)
                p.start()
                print(f"Started healthchecking with worker {container}")
            except Exception as e:
                print(f"Could not connect to {container} for healthchecking, restarting and reconnecting to the container, error: {e}")
        

    def run(self):
        """
        Here everything starts. 
        -   A reconnection process is crated for later and will be used if the container was restarted
            and needs to connect back to everybody.
        -   Connections are also accepted. If a new ContainerCordinator or worker wants to connect, he will
            be accepted here.
        -   When everything finishes, all process are joined and closed 
        """
        # Process that will reconnect to the network if it crashed before
        print("Voy a iniciar el proceso de reconexion")
        self.initiate_reconnection()
        print("Termine de iniciar el proceso de reconexion")

        if not self.leader.value:
            self.create_health_check_handling()
            # Create the process that will send the id to the other
            self.initiate_connector(False, self.leader, self.should_close)

        # Receive new connections and create a process that will handle them
        # print("Voy a entrar al while")
        while True:
            try:
                if self.leader.value and not self.health_check_sockets: # Never was the leader but now I am
                    self.setup_new_leader()
                            
                elif not self.leader.value and self.health_check_sockets: # I was the leader, but now I am not
                    # I have to stop the healthchecking process with the other coordinators
                    print("I am not the leader anymore, stopping healthchecking with coords and workers")
                    self.should_close.value = True
                    for name, conn in self.health_check_sockets.items():
                        print("Closing connection with ", name)
                        conn.shutdown(socket.SHUT_RDWR)
                        conn.close()
                    for p in self.health_checking_processes:
                        p.terminate()
                        p.join()
                    self.health_checking_processes = []
                    self.health_check_sockets = self.manager.dict()
                    print("Killed all HC processes")
                # print(f'Soy lider: {self.leader.value}')
                # print(f'Valor should close: {self.should_close.value}')
                self._socket.settimeout(1)
                # print("Entre")
                conn, addr = self._socket.accept()
                # print("Nueva conexion")
                # Once the connection is done. We need to hear for the id or name of the connected one.
                identifier, err = read_socket(conn)
                if err:
                    raise err
                print(f"Soy {self.id} y se me conecto: container_coordinator_{identifier}")
                # Start the process responsible for receiving the data from the new connection
                # if not self.leader.value:
                # Put in the dict the identifier with the TCP socket, if it's already added, it will be replaced
                self.add_connection(self.connections, identifier, conn)
                p = Process(target=self.initiate_connection, args=(f'container_coordinator_{self.id}', conn, self.connections, identifier, self.leader, self.should_close))
                p.start()
                if int(identifier) > self.id:
                    self.leader.value = False
                    self.should_close.value = True
                    # print(f'checking for greater id while connecting --> {id} > {self.id}')
                    # If the new connection has a bigger id I have to begin a new election
                    print("I have to begin a new election, because a new coordinator with a bigger id connected")
                    print(f"Connections: {self.connections}")
                    print(f"Coordinators list: {self.coordinators_list}")
                    for name, conn in self.connections.items():
                        if not name.isdigit() or int(name) < self.id:
                            continue
                        try:
                            print(self.id)
                            write_socket(conn, f"ELECTION {self.id}")
                        except:
                            print(f"Error sending election message to {name}, most probably died, skipping")
                            continue
                # if not self.leader.value:
                self.processes.append(p)
                # self.processes.append(p)

            except Exception as e:
                pass

# HOW TO START A CONTAINER AGAIN:
# To start again a container, the command "docker start <container_name>" can be used.

# Other way could be:
# os.system('docker build -f ./workers_dockerfiles/filter_review_quantity_worker.dockerfile -t "test_container:latest" .')
# os.system('docker run --rm --name test_container "test_container:latest"')

def parse_string_to_list(input_string):
    elements = input_string.split(',')
    
    result = [(element, int(element[-1])) for element in elements]
    
    return result

def read_workers_file(file_path):
    with open(file_path, 'r') as file:
        return [line.strip() for line in file.readlines()]

def main():
    coordinators_list = os.getenv('COORDS_LIST')                      # host1, port1, id1, host2, port2, id2, ...
    coordinators_list = parse_string_to_list(coordinators_list)             # [(host1, port1, id1), (host2, port2, id2), ...]
    coord_id = int(os.getenv('ID'))
    coords_port = int(os.getenv('COORDS_PORT'))
    workers_port = int(os.getenv('WORKERS_PORT'))
    listen_backlog = int(os.getenv('LISTEN_BACKLOG'))
    containers_list = read_workers_file(CONTAINERS_LIST)

    coord_info = coordinators_list[coord_id]                                # (host, port, id)
    coord_addr = (coord_info[HOST_INDEX], coords_port)
    print("Mi addr es: ", coord_addr)
    container_coord = ContainerCoordinator(coord_id, coord_addr, listen_backlog, coordinators_list, containers_list, coords_port, workers_port)
    container_coord.run()


main()

        