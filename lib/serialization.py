"""
Client reads from csv file with DictReader and serializes the dicts by
transforming them into a string, with the fields separated by FIELD_SEPARATOR = "@|@"

All the serialized dicts that enter in one batch are joined, separated by the ROW_SEPARATOR = "-|-"

To deserialize, first thhee rows have to be splitted by the ROW_SEPARATOR = "-|-".
Then each row has to be splitted by the FIELD_SEPARATOR = "@|@" 
"""

FIELD_SEPARATOR = "@|@"
ROW_SEPARATOR = "-|-"

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
    #return (''.join([serialize_item(item) for item in message_items]))[:-1]

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

def serialize_dict(dict):
    msg = ''
    for key, value in dict.items():
        msg += key + ':' + value + FIELD_SEPARATOR
    return msg[:-len(FIELD_SEPARATOR)]

def deserialize_into_titles_dict(row):
    splitted_row = row.split(FIELD_SEPARATOR)
    title_dict = {}
    for field in splitted_row:
        try:
            key, value = field.split(':', 1)
            title_dict[key] = value
        except Exception as e:
            print(f'El error es: {e} con la row: {row}')

    return title_dict



#def deserialize_into_reviews_dict(row):


