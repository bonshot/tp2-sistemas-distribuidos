""" Lenght of the communication headers protcol"""
HEADER_LENGHT = 10
MSG_SIZE_LENGTH = 9


def read_socket(socket, timeout=None):
    """
    Reads from the received socket. It supports short-read.
    """
    try: 
        # Read header
        header = _handle_short_read(socket, HEADER_LENGHT, timeout)

        # Read message
        # print(header[:-1], flush=True)
        # print(header[-1], flush=True)
        msg_len = int(header[:-1])
        end_flag = int(header[-1])
        if end_flag == 1:
            return "EOF", None
        bet_msg = _handle_short_read(socket, msg_len, timeout)

        return bet_msg, None
    
    except Exception as e:
        return None, e

def _handle_short_read(socket, bytes_to_read, timeout):
    """
    Handler of the short-read. Called by read_socket().
    """
    bytes_read = 0
    bytes_array = bytearray()
    while bytes_read < bytes_to_read:
        if timeout:
            socket.settimeout(timeout)
        msg_bytes = socket.recv(bytes_to_read - bytes_read)
        if msg_bytes == b'':
            raise Exception("Empty read")
        bytes_read += len(msg_bytes)
        bytes_array.extend(msg_bytes)
    
    msg = bytes_array.decode('utf-8')
    return msg

def write_socket(socket, msg):
    """
    Writes into the received socket. It supports short-write.
    """
    try: 
        # Add header
        header = get_header(msg, "0")
        complete_msg = header + msg
        
        _handle_short_write(socket, complete_msg)

        return None
    
    except Exception as e:
        return e
    

def _handle_short_write(socket, msg):
    """
    If the socket.send() call does not write the whole message, 
    it sends again from the first byte it did not sent.
    """
    msg_bytes = msg.encode("utf-8")
    bytes_to_write = len(msg_bytes)
    sent_bytes = socket.send(msg_bytes)
    while sent_bytes < bytes_to_write:
        sent_bytes += socket.send(msg_bytes[sent_bytes:])

def get_header(msg, end_flag):
    """
    Returns the protocols header for a message
    """
    header = str(len(bytes(msg, 'utf-8')))
    msg_len_bytes = len(header)

    for _ in range(0, MSG_SIZE_LENGTH - msg_len_bytes):
        header = '0' + header

    header += end_flag

    return header

def sendEOF(socket):
    """
    Sends a short meesage to the client. Only the header with the end_flag set to True
    """
    try: 
        header = get_header("", "1")        
        _handle_short_write(socket, header)
        return None
    except Exception as e:
        return e
