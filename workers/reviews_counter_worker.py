import os
from workers import JoinWorker
            
def main():

    data_source_titles_name = os.getenv('DATA_SOURCE_TITLES_NAME')
    data_source_reviews_name = os.getenv('DATA_SOURCE_REVIEWS_NAME')
    data_output_name = os.getenv('DATA_OUTPUT_NAME')
    worker_id = os.getenv('WORKER_ID')
    eof_quantity_titles = int(os.getenv('EOF_QUANTITY_TITLES'))
    eof_quantity_reviews = int(os.getenv('EOF_QUANTITY_REVIEWS'))
    log = os.getenv('LOG')
    max_unacked_msgs = int(os.getenv('MAX_UNACKED_MSGS'))

    worker = JoinWorker(worker_id, data_source_titles_name, data_source_reviews_name, data_output_name, eof_quantity_titles, eof_quantity_reviews, 3, log, max_unacked_msgs)
    worker.run()

    
main()