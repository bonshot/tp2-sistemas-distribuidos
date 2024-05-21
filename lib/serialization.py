"""
Client reads from csv file with DictReader and serializes the dicts by
transforming them into a string, with the fields separated by FIELD_SEPARATOR = "@|@"

All the serialized dicts that enter in one batch are joined, separated by the ROW_SEPARATOR = "$|$"

To deserialize, first thhee rows have to be splitted by the ROW_SEPARATOR = "$|$".
Then each row has to be splitted by the FIELD_SEPARATOR = "@|@" 
"""

FIELD_SEPARATOR = "@|@"
ROW_SEPARATOR = "$|$"
KEY_VAL_SEPARATOR = "#|#"
VALUES_SEPARATOR = ","
ID_FIELD = "ID"
RESULT_SLICE_FIELD = "RESULT_SLICE"
LAST_EOF_INDEX = 4
EOF_ID_INDEX = 5

def serialize_item(item):
    """
    Serialize a file item by adding a separator 
    and deleting the newline character
    """
    return item.rstrip('\n') + '|'

def serialize_message(message_items):
    """
    Serialize a message (list of items) by adding a separator
    on each item and deleting the newline character
    """
    return ROW_SEPARATOR.join(message_items)

def deserialize_item(item):
    """
    Deserialize a file item by splitting 
    it using the separator
    """
    return item.split(FIELD_SEPARATOR)

def deserialize_titles_message(bytes):
    """
    Deserialize a message (list of items) by splitting
    it using the separator
    """
    message = bytes.decode('utf-8')
    
    return [deserialize_into_titles_dict(row) for row in message.split(ROW_SEPARATOR)] 

def serialize_dict(dict_to_serialize):
    msg = ''
    for key, value in dict_to_serialize.items():
        if isinstance(value, set):
            value = serialize_set(value)
        elif isinstance(value, list):
            value = serialize_list(value)

        msg += key + KEY_VAL_SEPARATOR + value + FIELD_SEPARATOR
        
    return msg[:-len(FIELD_SEPARATOR)]

def serialize_list(list_to_serialize):
    return VALUES_SEPARATOR.join(map(str,list_to_serialize))

def serialize_set(set_to_serialize):
    serialized_set = ''
    for element in set_to_serialize:
        serialized_set += element + VALUES_SEPARATOR
    
    return serialized_set[:-len(VALUES_SEPARATOR)]

def deserialize_into_titles_dict(row):
    splitted_row = row.split(FIELD_SEPARATOR)
    title_dict = {}
    for field in splitted_row:
        try:
            key, value = field.split(KEY_VAL_SEPARATOR, 1)
            title_dict[key] = value
        except Exception as e:
            print(f'El error es: {e} con la row: {row}')
            raise e

    return title_dict

class Message:

    def __init__(self, client_id=None, msg = ""):
        if client_id:
            self.client_id = client_id
            self.msg = ID_FIELD + KEY_VAL_SEPARATOR + str(client_id) + FIELD_SEPARATOR + msg
        else:
            self.msg = msg
        self.end_flag = False

    def push(self, msg):
        if self.end_flag:
            raise Exception("Message already ended")
        self.msg += msg

    def set_end_flag(self):
        self.end_flag = True
    
    def is_ended(self):
        return self.end_flag

    def get_message(self):
        return self.msg
    
    def encode(self):
        return self.msg.encode('utf-8')
    
    def decode(self):
        try:
            msg = self.msg.decode('utf-8')
        except:
            msg = self.msg
        return msg

    def __str__(self):
        return self.msg
