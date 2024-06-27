from query_coordinator import QueryCoordinator
import os

def main():

    workers_q1 = os.getenv('WORKERS_Q1').split(',')    
    workers_q2 = os.getenv('WORKERS_Q2').split(',')
    workers_q3_titles = os.getenv('WORKERS_Q3_TITLES').split(',')
    workers_q3_reviews = os.getenv('WORKERS_Q3_REVIEWS').split(',')
    workers_q5_titles = os.getenv('WORKERS_Q5_TITLES').split(',')
    workers_q5_reviews = os.getenv('WORKERS_Q5_REVIEWS').split(',')
    eof_quantity = os.getenv('EOF_QUANTITY').split(',')
    eof_quantity = list(map(int, eof_quantity))
    log_data = os.getenv('LOG_DATA')
    log_results = os.getenv('LOG_RESULTS')
    max_unacked_msgs = int(os.getenv('MAX_UNACKED_MSGS'))
    eof_quantity = list(map(int, eof_quantity))
    address = os.getenv('ADDRESS')
    port = int(os.getenv('PORT'))

    query_coordinator = QueryCoordinator(workers_q1, workers_q2, workers_q3_titles, workers_q3_reviews, workers_q5_titles, workers_q5_reviews, eof_quantity, address, port, log_data, log_results, max_unacked_msgs)

    query_coordinator.run()

main()