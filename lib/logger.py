import pickle
import os
from serialization import create_log_file_name

VOLUME_PATH = './persisted_data/'

class Logger:

    def __init__(self, log_name, worker_id):
        self.log_filepath = VOLUME_PATH + create_log_file_name(log_name, worker_id, False)
        self.temp_log_filepath = VOLUME_PATH + create_log_file_name(log_name, worker_id, True)

    def persist(self, content):
        # First write on the temp file
        f = open(self.temp_log_filepath, 'wb')
        pickle.dump(content, f)
        f.flush()
        os.fsync(f.fileno()) 
        f.close()

        # Then change names, so the temp file becomes the original file
        os.replace(self.temp_log_filepath, self.log_filepath)
    
    def read_persisted_data(self):
        try:
            f = open(self.log_filepath, 'rb')
        except FileNotFoundError:
            return None
        
        prev_state = pickle.load(f)
        
        f.close()

        return prev_state