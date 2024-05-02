from middleware import Middleware
from serialization import deserialize_titles_message, serialize_message, serialize_dict
from filters import filter_by, category_condition
import os
import time

def handle_data(method, body, category, data_output_name, middleware, counter):
    if body == b'EOF':
        middleware.stop_consuming()
        middleware.ack_message(method)
        return
    data = deserialize_titles_message(body)
    
    desired_data = filter_by(data, category_condition, category)
    if not desired_data:
        middleware.ack_message(method)
        return
    counter[0] = counter[0] + len(desired_data)
    serialized_data = serialize_message([serialize_dict(filtered_dictionary) for filtered_dictionary in desired_data])
    middleware.send_message(data_output_name, serialized_data)

    middleware.ack_message(method)

def handle_eof(method, body, eof_counter, worker_quantity, data_output_name, next_worker_quantity, middleware):
    if body != b'EOF':
        print("[ERROR] Not an EOF on handle_eof(), system BLOCKED!. Received: ", body)
    
    eof_counter[0] += 1
    print('Me llego un eof, llevo ', eof_counter[0])
    if eof_counter[0] == worker_quantity:
        for _ in range(next_worker_quantity):
            print("MANDO UN EOF")
            middleware.send_message(data_output_name, 'EOF')
        middleware.stop_consuming()

    middleware.ack_message(method)

    
def main():
    time.sleep(15)

    middleware = Middleware()

    category_to_filter = os.getenv('CATEGORY')
    data_source_name = os.getenv('DATA_SOURCE_NAME')
    source_queue = os.getenv('SOURCE_QUEUE')
    data_output_name = os.getenv('DATA_OUTPUT_NAME')
    worker_id = os.getenv('WORKER_ID')
    eof_queue = os.getenv('EOF_QUEUE')
    worker_quantity = int(os.getenv('WORKER_QUANTITY'))
    next_worker_quantity = int(os.getenv('NEXT_WORKER_QUANTITY'))
    counter = [0]

    # Define a callback wrapper
    callback_with_params = lambda ch, method, properties, body: handle_data(method, body, category_to_filter, data_output_name, middleware, counter)

    # Declare and subscribe to the titles exchange
    middleware.define_exchange(data_source_name, {source_queue: [source_queue]})
    middleware.subscribe(data_source_name, source_queue, callback_with_params)
    middleware.consume()

    print(f"La cantidad de libros con la category {category_to_filter} es: [{counter[0]}]")

    # Once received the EOF, if I am the leader (WORKER_ID == 0), propagate the EOF to the next filter
    # after receiving WORKER_QUANTITY EOF messages.
    if worker_id == '0':
        if worker_quantity == 1:
            for _ in range(next_worker_quantity):
                print("MANDO UN EOF")
                middleware.send_message(data_output_name, 'EOF')
            return
        eof_counter = [0]
        eof_callback = lambda ch, method, properties, body: handle_eof(method, body, eof_counter, worker_quantity - 1, data_output_name, next_worker_quantity, middleware)
        middleware.receive_messages(eof_queue, eof_callback)
        middleware.consume()
    else:
        middleware.send_message(eof_queue, 'EOF')
    

main()
