import os
from workers import JoinWorker
      
            
def main():
    data_source_titles_name = os.getenv('DATA_SOURCE_TITLES_NAME')
    data_source_reviews_name = os.getenv('DATA_SOURCE_REVIEWS_NAME')
    data_output_name = os.getenv('DATA_OUTPUT_NAME')
    worker_id = os.getenv('WORKER_ID')
    eof_quantity_titles = int(os.getenv('EOF_QUANTITY_TITLES'))
    eof_quantity_reviews = int(os.getenv('EOF_QUANTITY_REVIEWS'))
    iteration_queue = os.getenv('ITERATION_QUEUE')
    log_acum = os.getenv('LOG_ACUM')
    log_leftovers = os.getenv(('LOG_LEFTOVERS'))

    worker = JoinWorker(worker_id, data_source_titles_name, data_source_reviews_name, data_output_name, eof_quantity_titles, eof_quantity_reviews, 5, iteration_queue, log_acum, log_leftovers)
    worker.run()


main()