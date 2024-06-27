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

    def run(self):

        try:
            addr = (self.server_host, self.server_port)
            analyzer = BooksAnalyzer(self.titles_filepath, self.reviews_filepath, addr)
            analyzer.start_service()

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
        
        
    