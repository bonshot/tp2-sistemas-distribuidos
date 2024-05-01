from middleware import Middleware
from serialization import deserialize_titles_message, serialize_message, serialize_dict
from filters import get_top_n
import os
import time

def handle_data(body, middleware, top, top_n, last, eof_counter, workers_quantity):
    print(body)
    if body == b'EOF':
        eof_counter[0] += 1
        if last and eof_counter[0] == workers_quantity:
            middleware.stop_consuming()
        else:
            middleware.stop_consuming()
        return
    data = deserialize_titles_message(body)

    new_top = get_top_n(data, top[0], top_n, last)
    #print("El new top es: ", new_top)
    top[0] = get_top_n(data, top[0], top_n, last)
    #print("Se modifico el top y quedo: ", top)
    
    
    
def main():
    time.sleep(15)

    middleware = Middleware()

    top_n = int(os.getenv('TOP_N'))
    data_source_name = os.getenv('DATA_SOURCE_NAME')
    data_output_name = os.getenv('DATA_OUTPUT_NAME')
    workers_quantity = int(os.getenv('WORKERS_QUANTITY'))
    last = True if os.getenv('LAST') == '1' else False
    top = [[]]
    eof_counter = [0]

    # Define a callback wrapper
    callback_with_params = lambda ch, method, properties, body: handle_data(body, middleware, top, top_n, last, eof_counter, workers_quantity)
    
    middleware.receive_messages(data_source_name, callback_with_params)
    middleware.consume()

    if not last:
        if len(top[0]) != 0:
            dict_to_send = {title:str(mean_rating) for title,mean_rating in top[0]}
            serialized_data = serialize_message([serialize_dict(dict_to_send)])
            middleware.send_message(data_output_name, serialized_data)

        middleware.send_message(data_output_name, 'EOF')
    else:
        # Send the results to the query_coordinator
        print(top)


main()
