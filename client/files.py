from csv import DictReader
from serialization import serialize_dict

def create_file_reader(file_path):
    """
    Create a file reader object
    """
    try:
        file = open(file_path, 'r')
    except:
        return None
    
    reader = DictReader(file)
    return file, reader

def read_csv_batch(file_reader, threshold=200):
    """
    Read a batch of rows from a CSV file
    """
    batch = []
    # EOF reached
    if file_reader is None:
        return batch
    
    for i, dictionary in enumerate(file_reader):
        serialized_dict = serialize_dict(dictionary)
        batch.append(serialized_dict)

        if i >= threshold:
            break
        
    return batch


