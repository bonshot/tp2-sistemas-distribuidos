from middleware import Middleware
from serialization import *
from worker_class import Worker
from logger import Logger
import signal
import queue
from multiprocessing import Process

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

    def __init__(self, workers_q1, workers_q2, workers_q3_titles, workers_q3_reviews, workers_q5_titles, workers_q5_reviews, eof_quantity, log_data, log_results):
        """
        Initializes the query coordinator with the title parse mode
        """
        signal.signal(signal.SIGTERM, self.handle_signal)

        self.id = ID
        self.workers = {Q1_KEY: workers_q1, Q2_KEY: workers_q2, Q3_TITLES_KEY: workers_q3_titles, Q3_REVIEWS_KEY: workers_q3_reviews,
                             Q5_TITLES_KEY: workers_q5_titles, Q5_REVIEWS_KEY: workers_q5_reviews}
        self.eof_quantity = eof_quantity
        self.log_data = Logger(log_data, ID)
        self.log_results = Logger(log_results, ID)
        self.processes = []      

    def handle_signal(self, *args):
        self.stop_coordinator = True

        for p in self.processes:
            p.terminate()
            p.join()
        

    def run(self):
        data_coordinator_p = Process(target=self.initiate_data_coordinator, args=())
        data_coordinator_p.start()
        self.processes.append(data_coordinator_p)
        result_coordinator_p = Process(target=self.initiate_result_coordinator, args=())
        result_coordinator_p.start()
        self.processes.append(result_coordinator_p)

        data_coordinator_p.join()
        result_coordinator_p.join()

    def initiate_data_coordinator(self):
        data_coordinator = DataCoordinator(self.id, self.workers, self.log_data)
        data_coordinator.run()

    def initiate_result_coordinator(self):
        result_coordinator = ResultsCoordinator(self.id, self.eof_quantity, self.log_results)
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

        # Read the data from the server, parse it and fordward it
        self.middleware.receive_messages(RECEIVE_SERVER_QUEUE, callback_with_params)
        self.middleware.consume()

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

class ResultsCoordinator:

    def __init__(self, id, eof_quantities, log):
        signal.signal(signal.SIGTERM, self.handle_signal)
        self.acum = True
        self.id = id
        #self.eof_quantity = sum(eof_quantities)     # The quantity  
        self.eof_quantity_queries = {   Q1: eof_quantities[Q1_EOF_QUANTITY_INDEX], Q2: eof_quantities[Q2_EOF_QUANTITY_INDEX], Q3: eof_quantities[Q3_EOF_QUANTITY_INDEX],
                                        Q4: eof_quantities[Q4_EOF_QUANTITY_INDEX], Q5: eof_quantities[Q5_EOF_QUANTITY_INDEX]
                                    }
        self.clients_results = {}                   # This dict stores for each active client, the results of each pipeline.
        # self.clients_results_counter = {}           # This dict stores for each active client, the quantity of pipeline results that are already complete.
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
        self.clients_unacked_query_eofs = {Q1: {}, Q2: {}, Q3: {}, Q4: {}, Q5: {}}
        # We have to keep record of the msg_ids received for each client in each query  
        self.client_acummulated_query_msgs = {Q1: {}, Q2: {}, Q3: {}, Q4: {}, Q5: {}}
        # For each query, we save the delivery_tags of the unacked messages so they can be acked later  
        self.unacked_query_msgs = {Q1: set(), Q2: set(), Q3: set(), Q4: set(), Q5: set()}
        # This dict stores for each active client, the worker ids that sent an EOF in each query.
        self.eof_worker_ids = {Q1: {}, Q2: {}, Q3: {}, Q4: {}, Q5: {}}

    def handle_signal(self, *args):
        self.queue.put('SIGTERM')
        if self.middleware != None:
            self.middleware.close_connection()

    def run(self):
        self.initialize_state()
        self.manage_results()
        
    def build_result_line(self, data, fields_to_print, query):
        """
        Builds the result line for the query
        """
        if query == Q1:
            return ' - '.join(f'{field.upper()}: {row[field]}' for row in data for field in fields_to_print)
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
        
    def is_EOF_repeated(self, eof_msg, client_id, worker_id, query):
        """
        Each pipeline sends its results through its own queue, and each pipeline has a different
        quantity of workers at the end stage. This means that if pipeline X has at his end stage
        4 workers, we will have to receive through the results queue of pipeline X 4 different EOFs.
        Since this workers can fail, there may be cases where they send the same EOF more than one time.

        Here we check if the worker id is already in the dict that saves for each client, the ids of the
        workers that already sent their EOF for each query.
        """
        if client_id not in self.eof_worker_ids[query]:
            self.eof_worker_ids[query][client_id] = set()

        if worker_id not in self.eof_worker_ids[query][client_id]:
            self.eof_worker_ids[query][client_id].add(worker_id)
            return False
        
        return True

    def client_is_active(self, client_id):
        return client_id in self.clients_results
    
    def remove_active_client(self, client_id):
        del self.clients_results[client_id]
        #del self.clients_results_counter[client_id]
        #del self.eof_worker_ids[client_id]

        # Persist into the log, clients_results, clients_results_counter and eof_worker_ids
        # curr_state = self.curr_state()
        # self.log.persist(curr_state)
    
    def received_all_query_EOFs(self, client_id, query):
        """
        We receive different quantity of EOFs from each query.
        Here we check if we received all EOFs from an especific query.
        """
        query_eof_quantity = self.eof_quantity_queries[query]
        # We can get the number of eofs received from a query by getting the
        # quantity of different workers that have already sent their EOF
        curr_eof_quantity = len(self.eof_worker_ids[client_id][query])

        if query_eof_quantity == curr_eof_quantity:
            return True
        return False

    def add_unacked_query_EOF(self, client_id, eof_method, query):
        if client_id not in self.clients_unacked_query_eofs[query]:
            self.clients_unacked_query_eofs[query][client_id] = set()

        self.clients_unacked_query_eofs[query][client_id].add(eof_method.delivery_tag)

    def ack_query_EOFs(self, client_id, query):
        unacked_eofs = self.clients_unacked_query_eofs[query][client_id]
        for tag in unacked_eofs:
            self.middleware.ack_message(tag)
        
        del self.clients_unacked_query_eofs[query][client_id]
    
    def received_all_EOFs(self, client_id):
        queries_status = []
        for query in self.client_acummulated_query_msgs.keys():
            if client_id not in self.eof_worker_ids[query]:
                eof_quantity_reached = False
            else:
                eof_quantity_reached = len(self.eof_worker_ids[query][client_id]) == self.eof_quantity_queries[query]

            if client_id not in self.client_acummulated_query_msgs[query]:
                query_closed = False
            else:
                query_closed = self.client_acummulated_query_msgs[query][client_id] == 'FINISHED'

            query_status = query_closed or eof_quantity_reached
            queries_status.append(query_status)

        return all(queries_status)
        # return self.clients_results_counter[client_id] == QUERIES_QUANTITY # Change this value according to how many queries you are running
    
    def initialize_state(self):
        prev_state = self.log.read_persisted_data()
        if prev_state != None:
            self.clients_results = prev_state[ACUMS_KEY]
            self.client_acummulated_query_msgs = dict(self.client_acummulated_query_msgs, **prev_state[ACUM_RESULT_MSGS])

    def curr_state(self):
        curr_state = {}
        curr_state[ACUMS_KEY] = self.clients_results
        curr_state[ACUM_RESULT_MSGS] = self.client_acummulated_query_msgs

        return curr_state

    def persist_results(self):
        curr_state = self.curr_state()
        self.log.persist(curr_state)

    def ack_query_msgs(self, query):
        unacked_msgs = self.unacked_query_msgs[query]
        for tag in unacked_msgs:
            self.middleware.ack_message(tag)

        self.unacked_query_msgs[query] = set()
    
    def ack_last_query_messages(self, query):
        if len(self.unacked_query_msgs[query]) > 0:
            self.ack_query_msgs(query)

    def received_all_client_query_EOFs(self, client_id, query):
        query_eof_quantity = self.eof_quantity_queries[query]
        # We can get the number of eofs received from a query by getting the
        # quantity of different workers that have already sent their EOF
        curr_eof_quantity = len(self.eof_worker_ids[query][client_id])

        if query_eof_quantity == curr_eof_quantity:
            return True
        return False
    
    def handle_results(self, method, body, fields_to_print, query):
        if is_EOF(body):
            client_id = get_EOF_client_id(body)
            worker_id = get_EOF_worker_id(body)

            if self.client_is_active(client_id):
                if not self.is_EOF_repeated(body, client_id, worker_id, query):
                    if not self.is_query_finished(client_id, query):
                        self.add_unacked_query_EOF(client_id, method, query)
                        if self.received_all_client_query_EOFs(client_id, query):
                            # Persist on disk the acums and the received msg_ids
                            self.persist_results()
                            # Ack last received messages of the queue
                            self.ack_last_query_messages(query)
                            if self.received_all_EOFs(client_id):
                                # Send the acum of the client and the EOF
                                self.send_results(client_id)
                                # Remove the acum of the client since it is not 
                                # necessary anymore
                                self.remove_active_client(client_id)
                            else:
                                self.finish_query(client_id, query)

                            # Update the state on disk and ack the EOFs for this channel and the client
                            self.persist_results()
                            self.ack_query_EOFs(client_id, query)

                        return

            self.middleware.ack_message(method)
            return

        msg_id, client_id, data = deserialize_titles_message(body)

        if not self.is_query_message_repeated(client_id, msg_id, query):
            self.manage_message(client_id, data, fields_to_print, query, method, msg_id)
            return

        self.middleware.ack_message(method)

    def is_query_message_repeated(self, client_id, msg_id, query):
        if client_id in self.client_acummulated_query_msgs[query]:
            return msg_id in self.client_acummulated_query_msgs[query][client_id]
        return False

    def finish_query(self, client_id, query):
        self.client_acummulated_query_msgs[query][client_id] = 'FINISHED'

    def is_query_finished(self, client_id, query):
        return self.client_acummulated_query_msgs[query][client_id] == 'FINISHED'
    
    def need_to_persist(self, query):
        return len(self.unacked_query_msgs[query]) == 5 # TODO: Make this a parameter for the worker! WARINIG: it always has to be lower than the prefetch count

    def manage_message(self, client_id, result_dict, fields_to_print, query, method, msg_id):
        self.acummulate_query_result(client_id, result_dict, fields_to_print, query)

        self.add_acummulated_result(client_id, method, msg_id, query)
        if self.need_to_persist(query):
            self.persist_results()
            self.ack_query_msgs(query)


    def add_acummulated_result(self, client_id, method, msg_id, query):
        if client_id not in self.client_acummulated_query_msgs[query]:
            self.client_acummulated_query_msgs[query][client_id] = set()

        # if client_id not in self.unacked_query_msgs[query]:
        #     self.unacked_query_msgs[query][client_id] = set()

        self.client_acummulated_query_msgs[query][client_id].add(msg_id)
        self.unacked_query_msgs[query].add(method.delivery_tag)


    def acummulate_query_result(self, client_id, result_dict, fields_to_print, query):
        if client_id not in self.clients_results:
            self.clients_results[client_id] = {}
        
        new_result_line = '\n' + self.build_result_line(result_dict, fields_to_print, query)
        self.clients_results[client_id][query] = self.clients_results[client_id].get(query, '') + new_result_line 


    def manage_results(self):
    
        # # Use queues to receive the queries results
        q1_results_with_params = lambda ch, method, properties, body: self.handle_results(method, body, ['Title', 'authors', 'publisher'], Q1)
        q2_results_with_params = lambda ch, method, properties, body: self.handle_results(method, body, ['authors'], Q2)
        q3_results_with_params = lambda ch, method, properties, body: self.handle_results(method, body, ['Title', 'authors'], Q3)
        q4_results_with_params = lambda ch, method, properties, body: self.handle_results(method, body, ['Title'], Q4)
        q5_results_with_params = lambda ch, method, properties, body: self.handle_results(method, body, ['Title'], Q5)
        self.middleware.receive_messages('QUEUE_q1_results' + '_' +  self.id, q1_results_with_params)
        self.middleware.receive_messages('QUEUE_q2_results' + '_' +  self.id, q2_results_with_params)
        self.middleware.receive_messages('QUEUE_q3_results' + '_' +  self.id, q3_results_with_params)
        self.middleware.receive_messages('QUEUE_q4_results' + '_' +  self.id, q4_results_with_params)
        self.middleware.receive_messages('QUEUE_q5_results' + '_' +  self.id, q5_results_with_params)
        self.middleware.consume()

    def assemble_results(self, client_id):
        client_results_dict = self.clients_results[client_id]
        results = []
        
        results1 = Q1 + client_results_dict[Q1]
        results.append(results1)
        results2 = Q2 + client_results_dict[Q2]
        results.append(results2)
        results3 = Q3 + client_results_dict[Q3]
        results.append(results3)
        results4 = Q4 + client_results_dict[Q4]
        results.append(results4)
        results5 = Q5 + client_results_dict[Q5]
        results.append(results5)
        
        results = '\n'.join(results)
        return results

    def send_results(self, client_id):
        # Create the result and add the client_id
        result_msg = self.assemble_results(client_id)
        result_msg = add_id(result_msg, client_id)
        # Send the results to the server
        self.middleware.send_message(SEND_SERVER_QUEUE, result_msg)


