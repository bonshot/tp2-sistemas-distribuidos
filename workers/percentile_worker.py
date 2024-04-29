from middleware import Middleware
from serialization import deserialize_titles_message, serialize_message, serialize_dict
from filters import titles_in_the_n_percentile
import os
import time

# titulo:cant_reviews,sumatoria_ratings,autores
def handle_data(body, data_output2_name, middleware, minimum_quantity, titles_with_sentiment, eof_counter, workers_quantity):
    if body == b'EOF':
        eof_counter[0] += 1
        if eof_counter[0] == workers_quantity:
            middleware.stop_consuming()
            middleware.send_message(data_output2_name, "EOF")
        return
    
    data = deserialize_titles_message(body)
    
    for key, value in data[0].items():
        titles_with_sentiment[key] = float(value)

    
def main():
    time.sleep(30)

    middleware = Middleware()

    minimum_quantity = os.getenv('MIN_QUANTITY')
    data_source_name = os.getenv('DATA_SOURCE_NAME')
    data_output_name = os.getenv('DATA_OUTPUT_NAME').split(',')
    workers_quantity = os.getenv('WORKERS_QUANTITY')
    percentile = os.getenv('PERCENTILE')
    titles_with_sentiment = {}
    eof_counter = [0]

    # Define a callback wrapper
    callback_with_params = lambda ch, method, properties, body: handle_data(body, data_output_name, middleware, int(minimum_quantity), titles_with_sentiment, eof_counter, int(workers_quantity))
    
    # Read the titles with their sentiment
    middleware.receive_messages(data_source_name, callback_with_params)
    middleware.consume()

    titles = titles_in_the_n_percentile(titles_with_sentiment, percentile)
    print(f"Los titulos en el percetil {percentile} son [{titles}]")

    #serialized_data = serialize_message([serialize_dict(filtered_titles)])
    #middleware.send_message('top_10', serialized_data)
    #middleware.send_message('top_10', 'EOF')



main()
