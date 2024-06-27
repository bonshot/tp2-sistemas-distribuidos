import socket
import time
import random
import docker
from communications import read_socket, write_socket

class HealthChecker():
    """
    Responsible for checking the health of a certain connection. If the connection
    is not healthy, it will restart the container. To determine if the connection is
    healthy, a simple message will be sent to the other side with a timeout of fixed
    time. If an ACK is not received, the container will be restarted after waiting 
    for a little bit.
    """

    def check_connection(self, container_id, port, conn, coords = False, should_close = None, health_check_sockets = None):
        """
        Check the health of the connection with the container_id.
        """
        self.conn = conn
        self.cont_id = container_id
        while True:
            try:
                time.sleep(1)
                if should_close and should_close.value:
                    if self.conn:
                        self.conn.close()
                    break
                #print(f"Checking connection with container {container_id}", flush=True)
                err = write_socket(self.conn, "HEALTH_CHECK")
                if err:
                    print(f"Error in container {container_id}, err was: {err}", flush=True)
                    raise err
                msg, err = read_socket(self.conn, timeout=5)
                if err:
                    print(f"Error in container {container_id}, err was: {err}", flush=True)
                    raise err
                elif msg == "ACK":
                    continue
                else:
                    raise Exception(f"Unexpected message from container: {msg}", flush=True)
            except Exception as e:
                print(f"REINICIO DE CONTAINER {container_id} POR TIMEOUT O ERROR, EL ERROR FUE {e}", flush=True, end="\n")
                self.restart_container(container_id)
                self.conn = self.reconnect_with_backoff(container_id, port)
                if health_check_sockets:
                    health_check_sockets[container_id] = self.conn
    
    def restart_container(self, container_id):
        """
        Restart the container with the container_id.
        """
        docker_client = docker.from_env()
        try:
            print(f"Restarting container {container_id}", flush=True)
            container = docker_client.containers.get(container_id)
            container.restart()
            print(f"Container {container_id} has been restarted", flush=True)
        except:
            print(f"Exception occurred while restarting container {container_id}, error:", flush=True)
            raise

    def reconnect_with_backoff(self, container_id, port, max_retries=5):
        print(f"Reconnecting to {container_id} with backoff", flush=True)
        retries = 0
        while retries < max_retries:
            try:
                conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                conn.settimeout(5)
                # print(f"Reconnecting to {(container_id, port)}", flush=True)
                conn.connect((container_id, port))
                print(f"Reconnected to {container_id}:{port}", flush=True)
                return conn
            except Exception as e:
                wait_time = (2 ** retries) + random.uniform(0, 1)
                # print(f"Reconnection failed (attempt {retries + 1}/{max_retries}): {e}. Retrying in {wait_time:.2f} seconds.")
                conn.close()  # Ensure the socket is closed before retrying
                time.sleep(wait_time)
                retries += 1
        raise Exception(f"Failed to reconnect to {container_id} after {max_retries} attempts")
    
    def handle_signal(self, signum, frame):
        self.conn.close()

class HealthCheckHandler():

    def __init__(self, socket, conn=None):
        self.socket = socket
        self.conn = conn

    def handle_health_check(self):
        print("Listening for incoming connections")
        if not self.conn:
            try:
                self.conn, addr = self.socket.accept()
                print("Received connection from {addr}, beginning healthcheck handling", addr)
            except:
                print("Error accepting initial connection")
        while True:
            try:
                time.sleep(1)
                msg, err = read_socket(self.conn)
                if err:
                    print("Error reading from socket: ", err)
                    raise err
                if msg == "HEALTH_CHECK":
                    err = write_socket(self.conn, "ACK")
                    if err:
                        print("Error writing to socket: ", err)
                        raise err
            except:
                print("Error reading from socket, listening for new connections")
                self.conn, addr = self.socket.accept()

    def handle_health_check_with_timeout(self, timeout, self_id, connections):
        print("Listening for incoming connections")
        if not self.conn:
            self.conn, addr = self.socket.accept()
            print("Received connection from, beginning healthcheck handling", addr)
        while True:
            try: 
                time.sleep(1)
                msg, err = read_socket(self.conn, timeout=timeout)
                if err:
                    print("Error reading from socket: ", err)
                    raise err
                if msg == "HEALTH_CHECK":
                    #print("Received health check message, sending ACK")
                    write_socket(self.conn, "ACK")
            except:
                print("Timeout reached for the health check, beginning leader election")
                self.begin_leader_election(self_id, connections)
                self.conn, addr = self.socket.accept()

    def begin_leader_election(self, self_id, connections):
        print("Beginning leader election")
        dead_connections = []
        # Bully leader election start  
        for name, conn in connections.items():
            if not name.isdigit() or name == self_id or int(name) < int(self_id):
                continue
            try:
                write_socket(conn, f"ELECTION {self_id}")
            except:
                print(f"Error sending election message to {name}, most probably died, skipping")
                dead_connections.append(name)
                continue
        for name in dead_connections:
            del connections[name]

    def close(self):
        self.socket.close()