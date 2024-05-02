from middleware import Middleware
from serialization import deserialize_titles_message, serialize_message, serialize_dict
from filters import filter_by, category_condition, eof_manage_process
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

    
def main():
    time.sleep(30)

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

    # Once received the EOF, if I am the leader (WORKER_ID == 0), propagate the EOF to the next filter
    # after receiving WORKER_QUANTITY EOF messages.
    eof_manage_process(worker_id, worker_quantity, next_worker_quantity, data_output_name, middleware, eof_queue)

    middleware.close_connection()

main()