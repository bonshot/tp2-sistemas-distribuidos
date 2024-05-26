from middleware import Middleware
from filters import filter_by, eof_manage_process, accumulate_authors_decades, different_decade_counter, titles_in_the_n_percentile, get_top_n, calculate_review_sentiment, review_quantity_value, COUNTER_FIELD
from serialization import serialize_message, serialize_dict, deserialize_titles_message, hash_title, serialize_batch, is_EOF, get_EOF_id, create_EOF
import signal
import queue

YEAR_CONDITION = 'YEAR'
TITLE_CONDITION = 'TITLE'
CATEGORY_CONDITION = 'CATEGORY'
QUERY_5 = 5
QUERY_3 = 3
BATCH_SIZE = 100
PREFETCH_COUNT = 200

class FilterWorker:

    def __init__(self, id, input_name, output_name, eof_queue, workers_quantity, next_workers_quantity, iteration_queue, eof_quantity, last):
        signal.signal(signal.SIGTERM, self.handle_signal)

        self.id = id
        self.last = last
        self.eof_queue = eof_queue
        self.input_name = input_name + '_' + id
        self.iteration_queue = iteration_queue
        self.eof_counter = 0
        self.eof_quantity = eof_quantity
        self.output_name = output_name
        self.workers_quantity = workers_quantity
        self.next_workers_quantity = next_workers_quantity
        self.middleware = None
        self.queue = queue.Queue()
        try:
            middleware = Middleware(self.queue)
        except Exception as e:
            raise e
        self.middleware = middleware
        
        self.stop_worker = False
        self.filtered_results_quantity = 0

    def handle_signal(self, *args):
        print("Gracefully exit")
        self.queue.put('SIGTERM')
        self.stop_worker = True
        if self.middleware != None:
            self.middleware.close_connection()
    
    def set_filter_type(self, type, filtering_function, value):
        self.filtering_function = filtering_function
        self.filter_condition = type
        self.filter_value = value

    # def handle_eof(self, method, body):
    #     if body != b'EOF':
    #         print("[ERROR] Not an EOF on handle_eof(), system BLOCKED!. Received: ", body)
        
    #     self.eof_counter += 1
    #     if self.eof_counter == self.workers_quantity - 1:
    #         # Send the EOFs to the next filter stage
    #         for _ in range(self.next_workers_quantity):
    #             self.middleware.send_message(self.output_name, 'EOF')
    #         self.middleware.stop_consuming()

    #         # Notify the workers in my filter stage that they can start another loop
    #         for _ in range(self.workers_quantity - 1):
    #             self.middleware.send_message(self.iteration_queue, 'OK')

    #         self.eof_counter = 0

    #     self.middleware.ack_message(method)

    # def handle_ok(self, method, body):
    #     """
    #     If an 'OK' was received, it means we can continue to the next iteration 
    #     """
    #     self.middleware.ack_message(method)
    #     self.middleware.stop_consuming()

    # def eof_manage_process(self):
    #     if self.id == '0':
    #         if self.workers_quantity == 1:
    #             for _ in range(self.next_workers_quantity):
    #                 self.middleware.send_message(self.output_name, 'EOF')
    #             return
    #         eof_callback = lambda ch, method, properties, body: self.handle_eof(method, body)
    #         self.middleware.receive_messages(self.eof_queue, eof_callback)
    #         self.middleware.consume()
    #     else:
    #         self.middleware.send_message(self.eof_queue, 'EOF')
    #         callback = lambda ch, method, properties, body: self.handle_ok(method, body)
    #         self.middleware.receive_messages(self.iteration_queue, callback)
    #         self.middleware.consume()

    def create_and_send_batches(self, batch, client_id, output_queue=None):
        workers_batches = {}
        for row in batch:
            hashed_title = hash_title(row['Title'])
            choosen_worker = str(hashed_title % self.next_workers_quantity)
            if choosen_worker not in workers_batches:
                workers_batches[choosen_worker] = []
            workers_batches[choosen_worker].append(row)

        if output_queue == None:
            output_queue = self.output_name
        for worker_id, batch in workers_batches.items():
            serialized_batch = serialize_batch(batch)
            serialized_message = serialize_message(serialized_batch, client_id)
            worker_queue = output_queue + '_' + worker_id
            self.middleware.send_message(worker_queue, serialized_message)
    
    def send_EOFs(self, client_id):
        eof_msg = create_EOF(client_id)
        for worker_id in range(self.next_workers_quantity):
            worker_queue = self.output_name + '_' + str(worker_id)
            self.middleware.send_message(worker_queue, eof_msg)

    def handle_data(self, method, body):
        if is_EOF(body):
            self.eof_counter += 1
            if self.eof_quantity == self.eof_counter:
                client_id = get_EOF_id(body)
                self.send_EOFs(client_id)
                self.middleware.stop_consuming()
                
            self.middleware.ack_message(method)
            return
        client_id, data = deserialize_titles_message(body)

        desired_data = filter_by(data, self.filtering_function, self.filter_value)
        if not desired_data:
            self.middleware.ack_message(method)
            return
        
        self.filtered_results_quantity += len(desired_data)

        # Create batches for each worker in the next stage and send those batches
        self.create_and_send_batches(desired_data, client_id)
        # serialized_data = serialize_message([serialize_dict(filtered_dictionary) for filtered_dictionary in desired_data])
        # self.middleware.send_message(self.output_name, serialized_data)

        self.middleware.ack_message(method)

    def run(self):
        callback_with_params = lambda ch, method, properties, body: self.handle_data(method, body)
        try:
            # Declare the output
            print("Voy a leer titulos")

            # Read the data
            self.middleware.receive_messages(self.input_name, callback_with_params)

            self.middleware.consume()

            print(f'El worker se quedo con {self.filtered_results_quantity} cantidad de titulos')
            self.filtered_results_quantity = 0
            # Once received the EOF, if I am the leader (WORKER_ID == 0), propagate the EOF to the next filter
            # after receiving WORKER_QUANTITY EOF messages.
            # self.eof_manage_process()

        except Exception as e:
            if self.stop_worker:
                print("Gracefully exited")
            else:
                print("An errror ocurred: ", e)
                return


class HashWorker:

    def __init__(self, id, input_titles_q3, input_titles_q5, input_reviews_q3, input_reviews_q5, output, hash_modulus, q3_quantity, q5_quantity, workers_quantity, eof_queue, iteration_queue):
        signal.signal(signal.SIGTERM, self.handle_signal)
        self.id = id
        self.stop_worker = False
        self.input_titles_q3 = input_titles_q3
        self.input_titles_q5 = input_titles_q5
        self.input_reviews_q3 = input_reviews_q3
        self.input_reviews_q5 = input_reviews_q5
        self.output = output
        self.hash_modulus = hash_modulus
        self.eof_queue = eof_queue
        self.iteration_queue = iteration_queue
        self.q3_quantity = q3_quantity
        self.q5_quantity = q5_quantity
        self.workers_quantity = workers_quantity
        self.eof_counter = 0
        self.middleware = None
        self.queue = queue.Queue()
        try:
            middleware = Middleware(self.queue)
        except Exception as e:
            raise e
        self.middleware = middleware

    def handle_signal(self, *args):
        print("Gracefully exit")
        self.queue.put('SIGTERM')
        self.stop_worker = True
        if self.middleware != None:
            self.middleware.close_connection()

    def handle_data(self, method, body, dataset_and_query):
        if body == b'EOF':
            routing_key = 'EOF' + '_' + dataset_and_query
            print('Me llego un EOF para la routing_key ', routing_key)
            self.middleware.publish_message(self.output, 'direct', routing_key, "EOF")
            self.middleware.stop_consuming(method)
            self.middleware.ack_message(method)

            return
        data = deserialize_titles_message(body)

        hash_title(data)

        for row_dictionary in data:
                    
            worker_id = str(row_dictionary['hashed_title'] % self.hash_modulus)

            row_dictionary.pop('hashed_title')
            serialized_message = serialize_message([serialize_dict(row_dictionary)])
            routing_key = worker_id + '_' + dataset_and_query
            self.middleware.publish_message(self.output, 'direct', routing_key, serialized_message)
        
        self.middleware.ack_message(method)

    def handle_eof(self, method, body):
        """
        Only notify the workers in the same stage they can continue with the next iteration,
        after receiving that they all finished with their current iteration.
        """
        if body != b'EOF':
            print("[ERROR] Not an EOF on handle_eof(), system BLOCKED!. Received: ", body)
        
        self.eof_counter += 1
        if self.eof_counter == self.workers_quantity - 1:
            # Notify the workers in my filter stage that they can start another loop
            for _ in range(self.workers_quantity - 1):
                self.middleware.send_message(self.iteration_queue, 'OK')

            self.middleware.stop_consuming()

            self.eof_counter = 0

        self.middleware.ack_message(method)

    def handle_ok(self, method, body):
        """
        If an 'OK' was received, it means we can continue to the next iteration.
        """
        self.middleware.ack_message(method)
        self.middleware.stop_consuming()

    def eof_manage_process(self):
        if self.id == '0':
            eof_callback = lambda ch, method, properties, body: self.handle_eof(method, body)
            self.middleware.receive_messages(self.eof_queue, eof_callback)
            self.middleware.consume()
        else:
            self.middleware.send_message(self.eof_queue, 'EOF')
            callback = lambda ch, method, properties, body: self.handle_ok(method, body)
            self.middleware.receive_messages(self.iteration_queue, callback)
            self.middleware.consume()
    
    def _define_exchange(self):
        queues_dict = {}
        # Titles and reviews for Q3
        for i in range(self.q3_quantity):
            key_titles = f'{i}_titles_Q3'
            key_reviews = f'{i}_reviews_Q3'
            value_titles = [key_titles, 'EOF_titles_Q3']
            value_reviews = [key_reviews, 'EOF_reviews_Q3'] 
            queues_dict[key_titles] = value_titles
            queues_dict[key_reviews] = value_reviews
        # Titles and reviews for Q5
        for i in range(self.q5_quantity):
            key_titles = f'{i}_titles_Q5'
            key_reviews = f'{i}_reviews_Q5'
            value_titles = [key_titles, 'EOF_titles_Q5']
            value_reviews = [key_reviews, 'EOF_reviews_Q5'] 
            queues_dict[key_titles] = value_titles
            queues_dict[key_reviews] = value_reviews

        # Declare the output exchange for query 3 and query 5
        self.middleware.define_exchange(self.output, queues_dict)

    def run(self):
        # Define a callback wrapper
        callback_with_params_titles_q3 = lambda ch, method, properties, body: self.handle_data(method, body, 'titles_Q3')
        callback_with_params_reviews_q3 = lambda ch, method, properties, body: self.handle_data(method, body, 'reviews_Q3')

        callback_with_params_titles_q5 = lambda ch, method, properties, body: self.handle_data(method, body, 'titles_Q5')
        callback_with_params_reviews_q5 = lambda ch, method, properties, body: self.handle_data(method, body, 'reviews_Q5')
        # Define the exchangge where joiners will read
        self._define_exchange()
        while not self.stop_worker:
            try:
                # Declare the source queue for the titles
                print("Voy a recibir los titulos")
                # For Q3
                self.middleware.receive_messages(self.input_titles_q3, callback_with_params_titles_q3)
                # For Q5
                self.middleware.receive_messages(self.input_titles_q5, callback_with_params_titles_q5)
                self.middleware.consume()
                        
                # Declare and subscribe to the reviews exchange
                print("Voy a recibir los reviews")
                # For Q3
                self.middleware.receive_messages(self.input_reviews_q3, callback_with_params_reviews_q3)
                # For Q5
                self.middleware.receive_messages(self.input_reviews_q5, callback_with_params_reviews_q5)
                self.middleware.consume()

                # Once received the EOF, if I am the leader (WORKER_ID == 0), propagate the EOF to the next filter
                # after receiving WORKER_QUANTITY EOF messages.
                self.eof_manage_process()
            except Exception as e:
               if self.stop_worker:
                   print("Gracefully exited")
               else:
                   print("An errror ocurred: ", e)
                   return

class JoinWorker:

    def __init__(self, id, input_titles_name, input_reviews_name, output_name, eof_quantity_titles, eof_quantity_reviews, query, iteration_queue):
        signal.signal(signal.SIGTERM, self.handle_signal)
        self.stop_worker = False

        if query != QUERY_5 and query != QUERY_3:
            raise Exception('Query not supported')

        self.id = id
        self.input_titles_name = input_titles_name + '_' + id
        self.input_reviews_name = input_reviews_name + '_' + id
        self.output_name = output_name
        self.iteration_queue = iteration_queue
        self.eof_counter_titles = 0
        self.eof_counter_reviews = 0
        self.eof_quantity_titles = eof_quantity_titles
        self.eof_quantity_reviews = eof_quantity_reviews
        self.counter_dict = {}
        self.query = query
        self.middleware = None
        self.queue = queue.Queue()
        try:
            middleware = Middleware(self.queue)
        except Exception as e:
            raise e
        self.middleware = middleware
    
    def handle_signal(self, *args):
        print("Gracefully exit")
        self.queue.put('SIGTERM')
        self.stop_worker = True
        if self.middleware != None:
            self.middleware.close_connection()

    def handle_titles_data(self, method, body):
        if is_EOF(body):
            self.eof_counter_titles += 1
            if self.eof_counter_titles == self.eof_quantity_titles:
                self.middleware.stop_consuming(method)
            self.middleware.ack_message(method)
            return
        client_id, data = deserialize_titles_message(body)

        for row_dictionary in data:
            title = row_dictionary['Title']
            if self.query == QUERY_5:
                self.counter_dict[title] = [0,0] # [reviews_quantity, review_sentiment_summation]
            else:
                self.counter_dict[title] = [0, 0, row_dictionary['authors']] # [reviews_quantity, ratings_summation, authors]
        
        self.middleware.ack_message(method)

    def handle_reviews_data(self, method, body):
        if is_EOF(body):
            self.eof_counter_reviews += 1
            if self.eof_counter_reviews == self.eof_quantity_reviews:
                self.middleware.stop_consuming(method)
            self.middleware.ack_message(method)
            return
        client_id, data = deserialize_titles_message(body)

        for row_dictionary in data:
            title = row_dictionary['Title']
            if title not in self.counter_dict:
                continue
            
            try:
                if self.query == QUERY_5:
                    parsed_value = self.parse_text_sentiment(row_dictionary['text_sentiment'])
                else:
                    parsed_value = self.parse_review_rating(row_dictionary['review/score'])
            except:
                continue

            counter = self.counter_dict[title]
            counter[0] += 1
            counter[1] += parsed_value 
            self.counter_dict[title] = counter
        
        self.middleware.ack_message(method)

    def parse_text_sentiment(self, text_sentiment):
        try:
            text_sentiment = float(text_sentiment)
        except Exception as e:
            print(f"Error: [{e}] when parsing 'text_sentiment' to float.")
            raise e
        return text_sentiment
        
    def parse_review_rating(self, rating):
        try:
            title_rating = float(rating)
        except Exception as e:
            print(f"Error: [{e}] when parsing 'review/score' to float.")
            raise e
        return title_rating
    
    def send_results(self):
        batch_size = 0
        batch = {}
        for title, counter in self.counter_dict.items():
            # Ignore titles with no reviews
            if counter[0] == 0:
                continue
            if self.query == QUERY_5:
                batch[title] = str(counter[1] / counter[0])
            else:
                batch[title] = counter

            batch_size += 1
            if batch_size == BATCH_SIZE:
                serialized_message = serialize_message([serialize_dict(batch)], '?') # TODO: THE ? IS HARD CODED. Change it when supproting parallel clients 
                self.middleware.send_message(self.output_name, serialized_message)
                batch = {}
                batch_size = 0

        if len(batch) != 0:
            serialized_message = serialize_message([serialize_dict(batch)], '?') # TODO: THE ? IS HARD CODED. Change it when supproting parallel clients 
            self.middleware.send_message(self.output_name, serialized_message)
        
        eof_msg = create_EOF('?')
        self.middleware.send_message(self.output_name, eof_msg)

    # def handle_ok(self, method, body):
    #     """
    #     If an 'OK' was received, it means we can continue to the next iteration 
    #     """
    #     self.middleware.ack_message(method)
    #     self.middleware.stop_consuming()  
            
    def run(self):

        # Define a callback wrappers
        callback_with_params_titles = lambda ch, method, properties, body: self.handle_titles_data(method, body)
        callback_with_params_reviews = lambda ch, method, properties, body: self.handle_reviews_data(method, body)

        try:
            # Titles messages
            self.middleware.receive_messages(self.input_titles_name, callback_with_params_titles)
            # Reviews messages
            self.middleware.receive_messages(self.input_reviews_name, callback_with_params_reviews)

            self.middleware.consume()

            # Once all the reviews were received, the counter_dict needs to be sent to the next stage
            self.send_results()

            # Wait for the accumulator worker in the next stage to notify
            # when to start the next iteration
            # callback = lambda ch, method, properties, body: self.handle_ok(method, body)
            # self.middleware.receive_messages(self.iteration_queue, callback)
            # self.middleware.consume()
            
        except Exception as e:
            if self.stop_worker:
                print("Gracefully exited")
            else:
                raise e

class DecadeWorker:

    def __init__(self, input_name, output_name, iteration_queue, worker_id):
        signal.signal(signal.SIGTERM, self.handle_signal)
        self.stop_worker = False
        self.input_name = input_name + '_' + worker_id
        self.output_name = output_name
        self.iteration_queue = iteration_queue
        self.middleware = None
        self.queue = queue.Queue()
        try:
            middleware = Middleware(self.queue)
        except Exception as e:
            raise e
        self.middleware = middleware

    def handle_signal(self, *args):
        print("Gracefully exit")
        self.queue.put('SIGTERM')
        self.stop_worker = True
        if self.middleware != None:
            self.middleware.close_connection()

    def handle_data(self, method, body):
        if is_EOF(body):
            self.middleware.stop_consuming()
            client_id = get_EOF_id(body)
            eof_msg = create_EOF(client_id)
            self.middleware.send_message(self.output_name, eof_msg)
            self.middleware.ack_message(method)
            return
        client_id, data = deserialize_titles_message(body)

        desired_data = different_decade_counter(data)
        if not desired_data:
            self.middleware.ack_message(method)
            return

        serialized_data = serialize_message([serialize_dict(filtered_dictionary) for filtered_dictionary in desired_data], client_id)
        self.middleware.send_message(self.output_name, serialized_data)

        self.middleware.ack_message(method)

    def handle_ok(self, method, body):
        """
        If an 'OK' was received, it means we can continue to the next iteration 
        """
        self.middleware.ack_message(method)
        self.middleware.stop_consuming()

    def run(self):
        # Define a callback wrapper
        callback_with_params = lambda ch, method, properties, body: self.handle_data(method, body)

        try:
            # Declare and subscribe to the titles exchange
            self.middleware.receive_messages(self.input_name, callback_with_params)
            self.middleware.consume()

            # Wait for the accumulator worker in the next stage to notify
            # when to start the next iteration
            callback = lambda ch, method, properties, body: self.handle_ok(method, body)
            self.middleware.receive_messages(self.iteration_queue, callback)
            self.middleware.consume()

        except Exception as e:
            if self.stop_worker:
                print("Gracefully exited")
            else:
                raise e


class GlobalDecadeWorker:

    def __init__(self, input_name, output_name, eof_quantity, iteration_queue):
        signal.signal(signal.SIGTERM, self.handle_signal)
        self.stop_worker = False
        self.input_name = input_name
        self.output_name = output_name
        self.eof_quantity = eof_quantity
        self.iteration_queue = iteration_queue
        self.eof_counter = 0
        self.counter_dict = {}
        self.middleware = None
        self.queue = queue.Queue()
        try:
            middleware = Middleware(self.queue)
        except Exception as e:
            raise e
        self.middleware = middleware

    def handle_signal(self, *args):
        print("Gracefully exit")
        self.queue.put('SIGTERM')
        self.stop_worker = True
        if self.middleware != None:
            self.middleware.close_connection()

    def handle_data(self, method, body):
        if is_EOF(body):
            self.eof_counter += 1
            if self.eof_counter == self.eof_quantity:
                self.middleware.stop_consuming()
            self.middleware.ack_message(method)
            return
        client_id, data = deserialize_titles_message(body)

        accumulate_authors_decades(data, self.counter_dict)

        self.middleware.ack_message(method)

    def run(self):
        # Define a callback wrapper
        callback_with_params = lambda ch, method, properties, body: self.handle_data(method, body)
    
        try:
            # Declare the source queue
            self.middleware.receive_messages(self.input_name, callback_with_params, PREFETCH_COUNT)
            self.middleware.consume()

            # Collect the results
            results = []
            for key, value in self.counter_dict.items():
                if len(value) >= 10:
                    results.append(key)
            # Send the results to the output queue
            serialized_message = serialize_message(results, '0')
            print(serialized_message)
            
            self.middleware.send_message(self.output_name, serialized_message)
            self.middleware.send_message(self.output_name, 'EOF')

            # Notify the workers in the previous stage they can continue
            # with the next iteration
            for _ in range(self.eof_quantity): # The eof quantity represents the quantity of workers in the previous stage
                self.middleware.send_message(self.iteration_queue, 'OK')


        except Exception as e:
            if self.stop_worker:
                print("Gracefully exited")
            else:
                raise e
        
class PercentileWorker:

    def __init__(self, input_name, output_name, percentile, eof_quantity, iteration_queue):
        signal.signal(signal.SIGTERM, self.handle_signal)
        self.stop_worker = False
        
        self.input_name = input_name
        self.output_name = output_name
        self.iteration_queue = iteration_queue
        self.percentile = percentile
        self.eof_quantity = eof_quantity
        self.eof_counter = 0
        self.titles_with_sentiment = {}
        self.middleware = None
        self.queue = queue.Queue()
        try:
            middleware = Middleware(self.queue)
        except Exception as e:
            raise e
        self.middleware = middleware

    def handle_signal(self, *args):
        print("Gracefully exit")
        self.queue.put('SIGTERM')
        self.stop_worker = True
        if self.middleware != None:
            self.middleware.close_connection()

    def handle_data(self, method, body):
        if is_EOF(body):
            self.eof_counter += 1
            if self.eof_counter == self.eof_quantity:
                self.middleware.stop_consuming()
            self.middleware.ack_message(method)
            return
        
        client_id, data = deserialize_titles_message(body)
        
        for key, value in data[0].items():
            self.titles_with_sentiment[key] = float(value)

        self.middleware.ack_message(method)

    def run(self):
        # Define a callback wrapper
        callback_with_params = lambda ch, method, properties, body: self.handle_data(method, body)
        
        try:
            # Read the titles with their sentiment
            self.middleware.receive_messages(self.input_name, callback_with_params, PREFETCH_COUNT)
            self.middleware.consume()

            titles = titles_in_the_n_percentile(self.titles_with_sentiment, self.percentile)

            serialized_data = serialize_message(titles)
            self.middleware.send_message(self.output_name, serialized_data)
            self.middleware.send_message(self.output_name, "EOF")

            print(len(titles))
            # # Send the OKs to the workers in the previous stage
            # for _ in range(self.eof_quantity): # The eof_quantity represents the quantity of workers in the previous stage 
            #     self.middleware.send_message(self.iteration_queue, 'OK')

        except Exception as e:
            if self.stop_worker:
                print("Gracefully exited")
            else:
                raise e

class TopNWorker:

    def __init__(self, id, input_name, output_name, eof_quantity, n, last, iteration_queue):
        signal.signal(signal.SIGTERM, self.handle_signal)
        self.stop_worker = False
        if not last:
            # If its not the last worker, it add the id
            self.input_name = input_name + '_' + id
        else:
            # If its the alst worker, it doesn't add the id
            self.input_name = input_name
        self.output_name = output_name
        self.top_n = n
        self.last = last
        self.eof_quantity = eof_quantity
        self.iteration_queue = iteration_queue
        self.eof_counter = 0
        self.top = []
        self.middleware = None
        self.queue = queue.Queue()
        try:
            middleware = Middleware(self.queue)
        except Exception as e:
            raise e
        self.middleware = middleware

    def handle_signal(self, *args):
        print("Gracefully exit")
        self.queue.put('SIGTERM')
        self.stop_worker = True
        if self.middleware != None:
            self.middleware.close_connection()
        print(self.eof_counter)


    def handle_data(self, method, body):
        if is_EOF(body):
            print("ME LLEGO EL EOF: ", body)
            self.eof_counter += 1
            if self.last and self.eof_counter == self.eof_quantity:
                print('Dejo de consumir siendo el lider')
                self.middleware.stop_consuming()
            elif not self.last:
                print('Dejo de consumir')
                self.middleware.stop_consuming()
            self.middleware.ack_message(method)
            return
        client_id, data = deserialize_titles_message(body)
        print(data)
        self.top = get_top_n(data, self.top, self.top_n, self.last)
        self.middleware.ack_message(method) 

    # def handle_ok(self, method, body):
    #     """
    #     If an 'OK' was received, it means we can continue to the next iteration 
    #     """
    #     self.middleware.ack_message(method)
    #     self.middleware.stop_consuming()     

    def parse_top(self):
        for title_dict in self.top:
            title_dict[COUNTER_FIELD] = str(title_dict[COUNTER_FIELD])

    def run(self):
        # Define a callback wrapper
        callback_with_params = lambda ch, method, properties, body: self.handle_data(method, body)
        
        try:
            self.middleware.receive_messages(self.input_name, callback_with_params)
            self.middleware.consume()

            #dict_to_send = {title:str(mean_rating) for title,mean_rating in self.top}
            #serialized_data = serialize_message([serialize_dict(dict_to_send)])
            self.parse_top()
            serialized_batch = serialize_batch(self.top)
            serialized_data = serialize_message(serialized_batch, '?') # TODO: THE ? IS HARD CODED. Change it when supproting parallel clients
            if not self.last:
                if len(self.top) != 0:
                    self.middleware.send_message(self.output_name, serialized_data)
                eof_msg = create_EOF('?')                                               # TODO: THE ? IS HARD CODED. Change it when supproting parallel clients 
                print("Mando el EOF a: ", self.output_name)
                self.middleware.send_message(self.output_name, eof_msg)

                # Wait for the accumulator worker in the next stage to notify
                # when to start the next iteration
                # callback = lambda ch, method, properties, body: self.handle_ok(method, body)
                # self.middleware.receive_messages(self.iteration_queue, callback)
                # self.middleware.consume()
            else:
                # Send the results to the query_coordinator
                self.middleware.send_message(self.output_name, serialized_data)
                self.middleware.send_message(self.output_name, 'EOF')

                print(serialized_data)

                # Notify the workers in the previous stage they can continue
                # with the next iteration
                # for _ in range(self.eof_quantity): # The eof qquantity represents the quantity of workers in the previous stage
                #     self.middleware.send_message(self.iteration_queue, 'OK')

            # As the iteration finished, it means a new client will arrive. So the top is emptied
            self.top = []
        except Exception as e:
            if self.stop_worker:
                print("Gracefully exited")
            else:
                raise e
                

class ReviewSentimentWorker:
    
    def __init__(self, input_name, output_name, worker_id, workers_quantity, next_workers_quantity, eof_queue, iteration_queue):
        signal.signal(signal.SIGTERM, self.handle_signal)
        self.stop_worker = False
        
        self.input_name = input_name + '_' + worker_id
        self.output_name = output_name
        self.id = worker_id
        self.workers_quantity = workers_quantity
        self.next_workers_quantity = next_workers_quantity
        self.eof_queue = eof_queue
        self.iteration_queue = iteration_queue
        self.eof_counter = 0
        self.middleware = None
        self.queue = queue.Queue()
        try:
            middleware = Middleware(self.queue)
        except Exception as e:
            raise e
        self.middleware = middleware

    def handle_signal(self, *args):
        print("Gracefully exit")
        self.queue.put('SIGTERM')
        self.stop_worker = True
        if self.middleware != None:
            self.middleware.close_connection()

    def create_and_send_batches(self, batch, client_id, output_queue=None):
        workers_batches = {}
        for row in batch:
            hashed_title = hash_title(row['Title'])
            choosen_worker = str(hashed_title % self.next_workers_quantity)
            if choosen_worker not in workers_batches:
                workers_batches[choosen_worker] = []
            workers_batches[choosen_worker].append(row)

        if output_queue == None:
            output_queue = self.output_name
        for worker_id, batch in workers_batches.items():
            serialized_batch = serialize_batch(batch)
            serialized_message = serialize_message(serialized_batch, client_id)
            worker_queue = output_queue + '_' + worker_id
            self.middleware.send_message(worker_queue, serialized_message)
    
    def send_EOFs(self, client_id):
        eof_msg = create_EOF(client_id)
        for worker_id in range(self.next_workers_quantity):
            worker_queue = self.output_name + '_' + str(worker_id)
            self.middleware.send_message(worker_queue, eof_msg)

    def handle_data(self, method, body):
        if is_EOF(body):
            self.middleware.stop_consuming()
            self.send_EOFs('?')                      # TODO: THE ? IS HARD CODED. Change it when supproting parallel clients 
            self.middleware.ack_message(method)
            return
        client_id, data = deserialize_titles_message(body)

        desired_data = calculate_review_sentiment(data)

        self.create_and_send_batches(desired_data, client_id)
        # serialized_data = serialize_message([serialize_dict(filtered_dict) for filtered_dict in desired_data])
        # self.middleware.send_message(self.output_name, serialized_data)
        
        self.middleware.ack_message(method)

    # def handle_eof(self, method, body):
    #     if is_EOF(body):
    #         print("[ERROR] Not an EOF on handle_eof(), system BLOCKED!. Received: ", body)
        
    #     self.eof_counter += 1
    #     if self.eof_counter == self.workers_quantity - 1:
    #         # Send the EOFs to the next filter stage
    #         for _ in range(self.next_workers_quantity):
    #             self.middleware.send_message(self.output_name, 'EOF')
    #         self.middleware.stop_consuming()

    #         # Notify the workers in my filter stage that they can start another loop
    #         for _ in range(self.workers_quantity - 1):
    #             self.middleware.send_message(self.iteration_queue, 'OK')
            
    #         self.eof_counter = 0

    #     self.middleware.ack_message(method)

    # def handle_ok(self, method, body):
    #     """
    #     If an 'OK' was received, it means we can continue to the next iteration 
    #     """
    #     self.middleware.ack_message(method)
    #     self.middleware.stop_consuming()

    # def eof_manage_process(self):
    #     if self.id == '0':
    #         if self.workers_quantity == 1:
    #             for _ in range(self.next_workers_quantity):
    #                 self.middleware.send_message(self.output_name, 'EOF')
    #             return
    #         eof_callback = lambda ch, method, properties, body: self.handle_eof(method, body)
    #         self.middleware.receive_messages(self.eof_queue, eof_callback)
    #         self.middleware.consume()
    #     else:
    #         self.middleware.send_message(self.eof_queue, 'EOF')
    #         callback = lambda ch, method, properties, body: self.handle_ok(method, body)
    #         self.middleware.receive_messages(self.iteration_queue, callback)
    #         self.middleware.consume()

    def run(self):
        # Define a callback wrapper
        callback_with_params = lambda ch, method, properties, body: self.handle_data(method, body)
        try:
            # Declare and subscribe to the titles exchange
            self.middleware.receive_messages(self.input_name, callback_with_params)
            self.middleware.consume()

            # self.eof_manage_process()

        except Exception as e:
            if self.stop_worker:
                print("Gracefully exited")
            else:
                raise e
        
class FilterReviewsWorker:
    
    def __init__(self, input_name, output_name1, output_name2, minimum_quantity, eof_quantity, next_workers_quantity, iteration_queue):
        signal.signal(signal.SIGTERM, self.handle_signal)

        self.input_name = input_name
        self.output_name1 = output_name1
        self.iteration_queue = iteration_queue
        self.output_name2 = output_name2
        self.eof_quantity = eof_quantity
        self.minimum_quantity = minimum_quantity
        self.next_workers_quantity = next_workers_quantity
        self.eof_counter = 0
        self.filtered_titles = []
        self.middleware = None
        self.queue = queue.Queue()
        try:
            middleware = Middleware(self.queue)
        except Exception as e:
            raise e
        self.middleware = middleware
        
        self.stop_worker = False
        self.filtered_results_quantity = 0

    def handle_signal(self, *args):
        print("Gracefully exit")
        self.queue.put('SIGTERM')
        self.stop_worker = True
        if self.middleware != None:
            self.middleware.close_connection()


    def handle_data(self, method, body):
        if is_EOF(body):
            self.eof_counter += 1
            if self.eof_counter == self.eof_quantity:
                self.middleware.stop_consuming()
            self.middleware.ack_message(method)
            return
        
        client_id, data = deserialize_titles_message(body)


        desired_data = review_quantity_value(data, self.minimum_quantity)
        for title, counter in desired_data[0].items():
            self.filtered_titles.append({'Title': title, 'counter': counter})
    
        self.middleware.ack_message(method)

    def create_and_send_batches(self, batch, client_id, output_queue=None):
        workers_batches = {}
        for row in batch:
            hashed_title = hash_title(row['Title'])
            choosen_worker = str(hashed_title % self.next_workers_quantity)
            if choosen_worker not in workers_batches:
                workers_batches[choosen_worker] = []
            workers_batches[choosen_worker].append(row)


        if output_queue == None:
            output_queue = self.output_name

        for worker_id, batch in workers_batches.items():
            serialized_batch = serialize_batch(batch)
            serialized_message = serialize_message(serialized_batch, client_id)
            worker_queue = output_queue + '_' + worker_id
            self.middleware.send_message(worker_queue, serialized_message)

    def send_EOFs(self, client_id, output_queue=None):
        eof_msg = create_EOF(client_id)
        if output_queue == None:
            output_queue = self.output_name
        for worker_id in range(self.next_workers_quantity):
            worker_queue = output_queue + '_' + str(worker_id)
            self.middleware.send_message(worker_queue, eof_msg)

    def run(self):
        # Define a callback wrapper
        callback_with_params = lambda ch, method, properties, body: self.handle_data(method, body)
        try:

            self.middleware.receive_messages(self.input_name, callback_with_params, PREFETCH_COUNT)
            self.middleware.consume()

            # Send the results to the query 4 and the QueryCoordinator
            #serialized_data = serialize_message([serialize_dict(self.filtered_titles)])
            #print(serialized_data)
            if len(self.filtered_titles) != 0:
                # First to the Query Coordinator
                #self.middleware.send_message(self.output_name1) # TODO: The way it is being sent to the QueryCoordinator has changed, so it has to be changed in that side to!
                # Then to the query 4
                self.create_and_send_batches(self.filtered_titles, '?', self.output_name2) # TODO: THE ? IS HARD CODED. Change it when supproting parallel clients 

            # Send the EOFs to the workers on the query 4
            self.send_EOFs('?', self.output_name2)                                                     # TODO: THE ? IS HARD CODED. Change it when supproting parallel clients
            # for _ in range(self.next_workers_quantity):
            #     self.middleware.send_message(self.output_name2, 'EOF')

            # Send the EOF to the QueryCoordinator
            # self.middleware.send_message(self.output_name1, 'EOF')

            # # Send the OKs to the workers in the previous stage
            # for _ in range(self.eof_quantity): # The eof_quantity represents the quantity of workers in the previous stage 
            #     self.middleware.send_message(self.iteration_queue, 'OK')

        except Exception as e:
            if self.stop_worker:
                print("Gracefully exited")
            else:
                raise e