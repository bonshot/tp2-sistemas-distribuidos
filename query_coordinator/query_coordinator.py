from middleware import Middleware
from serialization import *
from worker_class import MultipleQueueWorker
from logger import Logger
import signal
import queue
from multiprocessing import Process
from healthchecking import HealthCheckHandler
import socket

TITLES_MODE = 'titles'
REVIEWS_MODE = 'reviews'
BATCH_SIZE = 100
SEND_SERVER_QUEUE = 'server'
RECEIVE_SERVER_QUEUE = 'query_coordinator'
Q1 = '[QUERY 1]'
Q2 = '[QUERY 2]'
Q3 = '[QUERY 3]'
Q4 = '[QUERY 4]'
Q5 = '[QUERY 5]'
QUANTITY_INDEX = 0
QUEUE_INDEX = 1
Q1_KEY = '1'
Q2_KEY = '2'
Q3_TITLES_KEY = '3_titles'
Q3_REVIEWS_KEY = '3_reviews'
Q5_TITLES_KEY = '5_titles'
Q5_REVIEWS_KEY = '5_reviews'
Q1_EOF_QUANTITY_INDEX = 0
Q2_EOF_QUANTITY_INDEX = 1
Q3_EOF_QUANTITY_INDEX = 2
Q4_EOF_QUANTITY_INDEX = 3
Q5_EOF_QUANTITY_INDEX = 4
ID = '0'                    # WARNING: If this is changed, results will never get back to the QueryCoordinator 
QUERIES_QUANTITY = 5
ACUMS_KEY = 'results'
ACUM_RESULT_MSGS = 'results_msgs'
PARSE_MODES = 'parse_modes'
EOF_TYPES = 'eof_types'

class QueryCoordinator:

    def __init__(self, workers_q1, workers_q2, workers_q3_titles, workers_q3_reviews, workers_q5_titles, workers_q5_reviews, eof_quantity, address, port, log_data, log_results, max_unacked_msgs):
        """
        Initializes the query coordinator with the title parse mode
        """
        signal.signal(signal.SIGTERM, self.handle_signal)

        self.id = ID
        self.data_coordinator_p = None
        self.result_coordinator_p = None
        self.workers = {Q1_KEY: workers_q1, Q2_KEY: workers_q2, Q3_TITLES_KEY: workers_q3_titles, Q3_REVIEWS_KEY: workers_q3_reviews,
                             Q5_TITLES_KEY: workers_q5_titles, Q5_REVIEWS_KEY: workers_q5_reviews}
        self.eof_quantity = eof_quantity
        self.log_data = Logger(log_data, ID)
        self.log_results = Logger(log_results, ID)
        self.max_unacked_msgs = max_unacked_msgs   

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((address, port))
        self.socket.listen(1)
        self.health_checker = HealthCheckHandler(self.socket)
        self.health_check_handler_p = Process(target=self.health_checker.handle_health_check)
        self.health_check_handler_p.start()

    def handle_signal(self, *args):
        if self.data_coordinator_p:
            self.data_coordinator_p.terminate()
        if self.result_coordinator_p:
            self.data_coordinator_p.terminate()

        try:
            self.health_check_handler_p.terminate()    
            self.health_check_handler_p.join()
            self.health_check_handler_p.close()
        except:
            # There is a possibility that the process
            # hasn't been created yet
            pass


    def run(self):
        self.data_coordinator_p = Process(target=self.initiate_data_coordinator, args=())
        self.data_coordinator_p.start()
        self.result_coordinator_p = Process(target=self.initiate_result_coordinator, args=())
        self.result_coordinator_p.start()

        
        self.data_coordinator_p.join()
        self.result_coordinator_p.join()

        self.data_coordinator_p.close()
        self.result_coordinator_p.close()

    def initiate_data_coordinator(self):
        data_coordinator = DataCoordinator(self.id, self.workers, self.log_data)
        data_coordinator.run()

    def initiate_result_coordinator(self):
        result_coordinator = ResultsCoordinator(self.id, self.eof_quantity, self.log_results, self.max_unacked_msgs)
        result_coordinator.run()

class DataCoordinator:

    def __init__(self, id, workers, log):
        signal.signal(signal.SIGTERM, self.handle_signal)

        self.id = id
        self.log = log
        self.workers = workers
        self.clients_parse_mode = {}
        self.clients_eofs_types = {}       # This dict stores for each active client, the types of EOFs it received
        self.stop = False
        self.middleware = None
        self.queue = queue.Queue()
        try:
            middleware = Middleware(self.queue)
        except Exception as e:
            raise e
        self.middleware = middleware

    def handle_signal(self, *args):
        self.stop = True
        self.queue.put('SIGTERM')
        if self.middleware != None:
            self.middleware.close_connection()
    
    def change_parse_mode(self, mode, client_id):
        """
        Changes the parse mode to the one specified
        """
        if mode != TITLES_MODE and mode != REVIEWS_MODE:
            raise Exception("Mode not supported.")
        self.clients_parse_mode[client_id] = mode

    def initialize_state(self):
        prev_state = self.log.read_persisted_data()
        if prev_state != None:
            self.clients_parse_mode = prev_state[PARSE_MODES]
            self.clients_eofs_types = prev_state[EOF_TYPES] 

    def run(self):
        self.initialize_state()
        self.receive_and_fordward_data()

    def is_EOF_repeated(self, client_id, file_type):
        """
        For each client, 2 EOFs need to be received. The first one 
        means that all the titles info was sent and that the following
        messages will bring reviews info. The second one means that all 
        the reviews info was sent and nthe end of the info for that client.
        However, due to the failure of the server (which sends those messages),
        repeated EOFs are possible.
        Here we check if we hace already received an EOF with the same 
        'file_identifier'. If so, the message is acked and ignored.
        """
        client_eof_types = self.clients_eofs_types.get(client_id, set())
        if file_type not in client_eof_types:
            client_eof_types.add(file_type)
            self.clients_eofs_types[client_id] = client_eof_types
            return False
        return True
    
    def persist_state(self):
        curr_state = {}
        curr_state[PARSE_MODES] = self.clients_parse_mode
        curr_state[EOF_TYPES] = self.clients_eofs_types

        self.log.persist(curr_state)


    def handle_data(self, method, body):
        if is_EOF(body):
            client_id = get_EOF_client_id(body)
            file_type = get_EOF_worker_id(body)

            if not self.is_EOF_repeated(client_id, file_type):                
                self.manage_EOF(client_id)
                self.change_parse_mode(REVIEWS_MODE, client_id)
                # We received a new EOF which has changed our state. So we persit it to disk
                self.persist_state()
            
            self.middleware.ack_message(method)
            return
        
        msg_id, client_id, batch = deserialize_titles_message(body)
        if client_id not in self.clients_parse_mode:
            # Since titles are always first, every new client needs to be initialized with the TITLES_MODE
            self.clients_parse_mode[client_id] = TITLES_MODE
            # Every time a new client arrives, we persist the newe state on disk
            self.persist_state()

        self.send_to_pipelines(batch, client_id, msg_id)

        self.middleware.ack_message(method)

    def receive_and_fordward_data(self):
        callback_with_params = lambda ch, method, properties, body: self.handle_data(method, body)

        try:
            # Read the data from the server, parse it and fordward it
            self.middleware.receive_messages(RECEIVE_SERVER_QUEUE, callback_with_params)
            self.middleware.consume()
        except Exception as e:
            if self.stop:
                print("Gracefully exited")
            else:
                raise e

    def send_to_pipelines(self, batch, client_id, msg_id):
        batch = self.drop_rows_with_missing_values(batch, ['Title', 'authors', 'categories', 'publishedDate'], client_id)
        if len(batch) == 0:
            return
        
        # There isn't a parse_and_send_q4 because query 4 pipeline 
        # receives the data from the query 3 pipeline results
        self.parse_and_send_q1(batch, client_id, msg_id)
        self.parse_and_send_q2(batch, client_id, msg_id)
        self.parse_and_send_q3(batch, client_id, msg_id)
        self.parse_and_send_q5(batch, client_id, msg_id)

    def parse_and_send(self, batch, desired_keys, queue, query, client_id, msg_id):
        msg_id = int(msg_id)
        # First, we get only the columns the query needs
        new_batch = []
        for row in batch:
            row = {k: v for k, v in row.items() if k in desired_keys}
            new_batch.append(row)
        
        # Second, we create the batches for each worker
        workers_batches = {}
        workers_quantity = int(self.workers[query][QUANTITY_INDEX])
        for row in new_batch:
            hashed_title = hash_title(row['Title'])
            choosen_worker = hashed_title % workers_quantity
            
            if choosen_worker not in workers_batches:
                workers_batches[choosen_worker] = []
            workers_batches[choosen_worker].append(row)

        # Third, we send the batches 
        for worker_id, batch in workers_batches.items():
            serialized_batch = serialize_batch(batch)
            batch_msg_id = msg_id + worker_id
            serialized_message = serialize_message(serialized_batch, client_id, str(batch_msg_id))
            worker_queue = queue + '_' + str(worker_id)
            self.middleware.send_message(worker_queue, serialized_message)

    def parse_and_send_q1(self, batch, client_id, msg_id):
        """
        Parses the rows of the batch to return only
        required columns in the query 1
        """
        if self.clients_parse_mode[client_id] == REVIEWS_MODE:
            return
        desired_keys = ['Title', 'publishedDate', 'categories', 'authors', 'publisher']
        self.parse_and_send(batch, desired_keys, self.workers[Q1_KEY][QUEUE_INDEX], Q1_KEY, client_id, msg_id)

    def parse_and_send_q2(self, batch, client_id, msg_id):
        if self.clients_parse_mode[client_id] == REVIEWS_MODE:
            return
        desired_keys = ['Title','authors', 'publishedDate']
        batch = self.drop_rows_with_missing_values(batch, ['Title', 'authors', 'categories', 'publishedDate'], client_id)
        self.parse_and_send(batch, desired_keys, self.workers[Q2_KEY][QUEUE_INDEX], Q2_KEY, client_id, msg_id)
    
    def parse_and_send_q3(self, batch, client_id, msg_id):
        if self.clients_parse_mode[client_id] == TITLES_MODE:
            desired_keys = ['Title', 'authors', 'publishedDate']
            self.parse_and_send(batch, desired_keys, self.workers[Q3_TITLES_KEY][QUEUE_INDEX], Q3_TITLES_KEY, client_id, msg_id)
        else:
            desired_keys = ['Title', 'review/score']
            self.parse_and_send(batch, desired_keys, self.workers[Q3_REVIEWS_KEY][QUEUE_INDEX], Q3_REVIEWS_KEY, client_id, msg_id)
    
    def parse_and_send_q5(self, batch, client_id, msg_id):
        if self.clients_parse_mode[client_id] == TITLES_MODE:
            desired_keys = ['Title', 'categories']
            self.parse_and_send(batch, desired_keys, self.workers[Q5_TITLES_KEY][QUEUE_INDEX], Q5_TITLES_KEY, client_id, msg_id)
        else:
            desired_keys = ['Title', 'review/text']
            self.parse_and_send(batch, desired_keys, self.workers[Q5_REVIEWS_KEY][QUEUE_INDEX], Q5_REVIEWS_KEY, client_id, msg_id)

    def drop_rows_with_missing_values(self, batch, columns, client_id):
        """
        Drops the rows with missing values in the specified columns
        """
        new_batch = []
        for row in batch:
            if self.clients_parse_mode[client_id] == TITLES_MODE and not all([row.get(column) for column in columns]): # We drop the Nan values only for the titles dataset
                continue    
            new_batch.append(row)
            
        return new_batch

    def manage_EOF(self, client_id):
        """
        Sends the EOF message to the middleware
        """
        if self.clients_parse_mode[client_id] == TITLES_MODE:
            self.send_EOF(Q1_KEY, self.workers[Q1_KEY][QUEUE_INDEX], client_id)
            self.send_EOF(Q2_KEY, self.workers[Q2_KEY][QUEUE_INDEX], client_id)
            self.send_EOF(Q3_TITLES_KEY, self.workers[Q3_TITLES_KEY][QUEUE_INDEX], client_id)
            self.send_EOF(Q5_TITLES_KEY, self.workers[Q5_TITLES_KEY][QUEUE_INDEX], client_id)
        else: 
            self.send_EOF(Q3_REVIEWS_KEY, self.workers[Q3_REVIEWS_KEY][QUEUE_INDEX], client_id)
            self.send_EOF(Q5_REVIEWS_KEY, self.workers[Q5_REVIEWS_KEY][QUEUE_INDEX], client_id)
            pass

    def send_EOF(self, workers_dict_key, queue, client_id):

        eof_msg = create_EOF(client_id, self.id)
        workers_quantity = int(self.workers[workers_dict_key][QUANTITY_INDEX])
        for worker_id in range(workers_quantity):
            worker_queue = queue + '_' + str(worker_id)
            self.middleware.send_message(worker_queue, eof_msg)

class ResultsCoordinator(MultipleQueueWorker):

    def __init__(self, id, eof_quantities, log, max_unacked_msgs):
        signal.signal(signal.SIGTERM, self.handle_signal)
        self.acum = True
        self.id = id
        self.eof_quantity_queues = {   Q1: eof_quantities[Q1_EOF_QUANTITY_INDEX], Q2: eof_quantities[Q2_EOF_QUANTITY_INDEX], Q3: eof_quantities[Q3_EOF_QUANTITY_INDEX],
                                        Q4: eof_quantities[Q4_EOF_QUANTITY_INDEX], Q5: eof_quantities[Q5_EOF_QUANTITY_INDEX]
                                    }
        self.clients_acum = {}                   # This dict stores for each active client, the results of each pipeline.
        self.max_unacked_msgs = max_unacked_msgs
        self.stop = False
        self.middleware = None
        self.log = log
        self.queue = queue.Queue()
        try:
            middleware = Middleware(self.queue)
        except Exception as e:
            raise e
        self.middleware = middleware

        # This dict stores for each active client, the delivery tags of the unacked EOFs for each query.
        self.clients_unacked_queue_eofs = {Q1: {}, Q2: {}, Q3: {}, Q4: {}, Q5: {}}
        # We have to keep record of the msg_ids received for each client in each query  
        self.clients_acummulated_queue_msg_ids = {Q1: {}, Q2: {}, Q3: {}, Q4: {}, Q5: {}}
        # For each query, we save the delivery_tags of the unacked messages so they can be acked later  
        self.unacked_queue_msgs = {Q1: set(), Q2: set(), Q3: set(), Q4: set(), Q5: set()}
        # This dict stores for each active client, the worker ids that sent an EOF in each query.
        self.queue_eof_worker_ids = {Q1: {}, Q2: {}, Q3: {}, Q4: {}, Q5: {}}

        # For each query, there are different and especific filds we need for the results
        self.fields_to_print = {Q1: ['Title', 'authors', 'publisher'], Q2: ['authors'], Q3: ['Title', 'authors'], Q4: ['Title'], Q5: ['Title']}

    def handle_signal(self, *args):
        self.stop = True
        self.queue.put('SIGTERM')
        if self.middleware != None:
            self.middleware.close_connection()

    def run(self):
        self.initialize_state()
        self.manage_results()
        
    def build_result_line(self, data, query):
        """
        Builds the result line for the query
        """
        if query == Q1:
            return ' - '.join(f'{field.upper()}: {row[field]}' for row in data for field in self.fields_to_print[query])
        elif query == Q3:
            line = ''
            for result_dict in data:
                title = result_dict['Title']
                authors = result_dict['counter'].split(',', 2)[2] # The split is 2 until the second comma because the auuthors field can have comas
                line += 'TITLE: ' + title + '    ' + 'AUTHORS: ' + authors + '\n' 
            return line
        elif query  == Q4:
            line = ''
            top_position = 1
            for result_dict in data:
                title = result_dict['Title']
                mean_rating = result_dict['counter']
                line += str(top_position) +'.   TITLE: ' + title + '    ' + 'MEAN-RATING: ' +  mean_rating + '\n'
                top_position += 1
            return line
        else:
            return data[0]['results']
    
    def remove_active_client(self, client_id):
        del self.clients_acum[client_id]
        #del self.clients_results_counter[client_id]
        #del self.eof_worker_ids[client_id]

        # Persist into the log, clients_acum, clients_results_counter and eof_worker_ids
        # curr_state = self.curr_state()
        # self.log.persist(curr_state)
  
    
    def initialize_state(self):
        prev_state = self.log.read_persisted_data()
        if prev_state != None:
            self.clients_acum = prev_state[ACUMS_KEY]
            self.clients_acummulated_queue_msg_ids = dict(self.clients_acummulated_queue_msg_ids, **prev_state[ACUM_RESULT_MSGS])

    def curr_state(self):
        curr_state = {}
        curr_state[ACUMS_KEY] = self.clients_acum
        curr_state[ACUM_RESULT_MSGS] = self.clients_acummulated_queue_msg_ids

        return curr_state

    def manage_message(self, client_id, result_dict, queue, method, msg_id):
        self.acummulate_query_result(client_id, result_dict, queue)

        self.add_acummulated_message(client_id, method, msg_id, queue)
        if self.need_to_persist(queue):
            self.persist_state()
            self.ack_queue_msgs(queue)

    def acummulate_query_result(self, client_id, result_dict, query):
        if client_id not in self.clients_acum:
            self.clients_acum[client_id] = {}
        
        new_result_line = '\n' + self.build_result_line(result_dict, query)
        self.clients_acum[client_id][query] = self.clients_acum[client_id].get(query, '') + new_result_line 


    def manage_results(self):
    
        # # Use queues to receive the queries results
        q1_results_with_params = lambda ch, method, properties, body: self.handle_data(method, body, Q1)
        q2_results_with_params = lambda ch, method, properties, body: self.handle_data(method, body, Q2)
        q3_results_with_params = lambda ch, method, properties, body: self.handle_data(method, body, Q3)
        q4_results_with_params = lambda ch, method, properties, body: self.handle_data(method, body, Q4)
        q5_results_with_params = lambda ch, method, properties, body: self.handle_data(method, body, Q5)
        try:
            self.middleware.receive_messages('QUEUE_q1_results' + '_' +  self.id, q1_results_with_params)
            self.middleware.receive_messages('QUEUE_q2_results' + '_' +  self.id, q2_results_with_params)
            self.middleware.receive_messages('QUEUE_q3_results' + '_' +  self.id, q3_results_with_params)
            self.middleware.receive_messages('QUEUE_q4_results' + '_' +  self.id, q4_results_with_params)
            self.middleware.receive_messages('QUEUE_q5_results' + '_' +  self.id, q5_results_with_params)
            self.middleware.consume()
        except Exception as e:
            if self.stop:
                print("Gracefully exited")
            else:
                raise e

    def assemble_results(self, client_id):
        client_results_dict = self.clients_acum[client_id]
        results = []
        
        result1 = self.assemble_query_result(Q1, client_results_dict)
        results.append(result1)
        result2 = self.assemble_query_result(Q2, client_results_dict)
        results.append(result2)
        result3 = self.assemble_query_result(Q3, client_results_dict)
        results.append(result3)
        result4 = self.assemble_query_result(Q4, client_results_dict)
        results.append(result4)
        result5 = self.assemble_query_result(Q5, client_results_dict)
        results.append(result5)

        
        results = '\n'.join(results)
        return results
    
    def assemble_query_result(self, query, client_results_dict):
        result = query
        if query in client_results_dict:
            result += client_results_dict[query]
        return result

    def send_results(self, client_id):
        # Create the result and add the client_id
        result_msg = self.assemble_results(client_id)
        result_msg = add_id(result_msg, client_id)
        # Send the results to the server
        self.middleware.send_message(SEND_SERVER_QUEUE, result_msg)


