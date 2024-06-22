from communications import write_socket, sendEOF, read_socket
from files import create_file_reader, read_csv_batch
import socket
from csv import DictReader
from serialization import serialize_message
import os
import time
import signal
from client_lib import BooksAnalyzer



class Client:

    def __init__(self, host, port, titles_filepath, reviews_filepath):
        self.server_host = host
        self.server_port = int(port)
        self.titles_filepath = titles_filepath
        self.reviews_filepath = reviews_filepath

        signal.signal(signal.SIGTERM, self.handle_signal)

    # def _connect_server(self):
    #     conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #     loop_lapse = CONN_LOOP_LAPSE_START
    #     for i in range(CONNECT_TRIES):
    #         if self.stop_client:
    #             raise OSError
    #         try:
    #             print("Connecting to server. Attempt: ", i)
    #             conn.connect((self.server_host, self.server_port))
    #             break
    #         except Exception as e:
    #             time.sleep(loop_lapse)
    #             loop_lapse = loop_lapse * 2
    #             if i == CONNECT_TRIES - 1:
    #                 print("Could not connect to server")
    #                 raise e
    #     self._server_socket = conn

    # def scrape_and_send_file(self, file_reader):
    #     """
    #     Gets batches of info from reading a file and sends it through the socket
    #     """
    #     while not self.stop_client:
    #         file_batch = read_csv_batch(file_reader)
    #         serialized_message = serialize_message(file_batch)
    #         if not file_batch:
    #             break
    #         write_socket(self._server_socket, serialized_message)

    
    # def receive_results(self):
    #     # Listen the server response and print it
    #     msg = None
    #     while msg != "EOF":
    #         msg, e = read_socket(self._server_socket)
    #         if e != None:
    #             # TODO: Maybe raise the exception or just print it and return
    #             print(f"Hubo un error en la lectura del socket del server. El error fue: {e}")
    #             return

    #         print(msg)

    def run(self):

        try:
            addr = (self.server_host, self.server_port)
            analyzer = BooksAnalyzer(self.titles_filepath, self.reviews_filepath, addr)
            analyzer.start_service()
            # start_service(addr, self.titles_filepath, self.reviews_filepath)
            # Send the files to the system
            #send_data(self.titles_filepath, self.reviews_filepath, self._server_socket)

            # Receive the queries results
            analyzer.receive_results()
            # self.receive_results()
        except Exception as e:
            print(f"Error with the server. [{e}]")
            return

    def handle_signal(self, *args):
        # TODO: Put a queue to inform the client lib that a SIGTERM was received
        pass


def main():
    host = os.getenv('HOST')
    port = os.getenv('PORT')
    titles_filepath = os.getenv('TITLES_FILEPATH')
    reviews_filepath = os.getenv('REVIEWS_FILEPATH')

    client = Client(host, port, titles_filepath, reviews_filepath)
    client.run()
    
    
    
main()
        
        
    