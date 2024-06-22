from csv import DictReader
import socket
import time
from serialization import serialize_dict, serialize_message
from communications import write_socket, read_socket

TITLES_IDENTIFIER = 't'
REVIEWS_IDENTIFIER = 'r'
CONNECT_TRIES = 10
CONN_LOOP_LAPSE_START = 1


class BooksAnalyzer:

    def __init__(self, titles_filepath, reviews_filepath, server_addr):
        self.titles_filepath = titles_filepath
        self.reviews_filepath = reviews_filepath
        self.server_addr = server_addr
        self.server_socket = None
        
    def start_service(self):
        self._connect_server()

        self.send_data()

    def _connect_server(self):
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        loop_lapse = CONN_LOOP_LAPSE_START
        for i in range(CONNECT_TRIES):
            # if self.stop_client: TODO: Add a queue so it can read if the client signaled to stop
            #     raise OSError
            try:
                print("Connecting to server. Attempt: ", i)
                conn.connect(self.server_addr)
                break
            except Exception as e:
                time.sleep(loop_lapse)
                loop_lapse = loop_lapse * 2
                if i == CONNECT_TRIES - 1:
                    print("Could not connect to server")
                    raise Exception('Could not connect to server.')
                
        self.server_socket = conn

    def receive_results(self):
        # Listen the server response and print it
        msg = None
        while msg != "EOF":
            msg, e = read_socket(self.server_socket)
            if e != None:
                print("Reconnecting to server...")
                self._connect_server()
                print("Reconnection succesful")

            print(msg)

        self.server_socket.close()


    ########### SEND MANAGMENT FUNCTIONS ###########

    def send_file(self, file_path, file_identifier):
        msg_id = 0
        file, file_reader = self.create_file_reader(file_path)
        file_batch = self.read_csv_batch(file_reader)

        while file_batch:
            serialized_message = serialize_message(file_batch, '', str(msg_id))

            try:
                # Send the batch
                e = write_socket(self.server_socket, serialized_message)
                if e != None:
                    raise e
                # Receive the ack
                _, e = read_socket(self.server_socket)
                if e != None:
                    raise e
                file_batch = self.read_csv_batch(file_reader)
                msg_id += 1
            except:
                # If there was an exception reading or writing, we need to reconnect to the server
                print("Reconnecting to server...")
                self._connect_server()
                print("Reconnection succesful")


        # Send the corresponding EOF
        eof_msg = 'EOF_' + file_identifier
        write_socket(self.server_socket, eof_msg)
        file.close()

    def send_data(self):
        # Send the titles dataset
        self.send_file(self.titles_filepath, TITLES_IDENTIFIER)

        # Send the reviews dataset
        self.send_file(self.reviews_filepath, REVIEWS_IDENTIFIER)

    ################################################

    ########### FILE MANAGMENT FUNCTIONS ###########

    def create_file_reader(self, file_path):
        """
        Create a file reader object
        """
        try:
            file = open(file_path, 'r')
        except Exception as e:
            raise e
        
        reader = DictReader(file)
        return file, reader

    def read_csv_batch(self, file_reader, threshold=200):
        """
        Read a batch of rows from a CSV file
        """
        batch = []
        # EOF reached
        if file_reader is None:
            raise Exception("FileReader is None")
        
        for i, dictionary in enumerate(file_reader):
            serialized_dict = serialize_dict(dictionary)
            batch.append(serialized_dict)

            if i >= threshold:
                break
            
        return batch

    ################################################
