from filters import year_range_condition
from workers import FilterWorker
import os
    
def main():

    years = [int(value) for value in os.getenv('YEAR_RANGE_TO_FILTER').split(',')]
    source_name = os.getenv('DATA_SOURCE_NAME')
    output_name = os.getenv('DATA_OUTPUT_NAME')
    worker_id = os.getenv('WORKER_ID')
    eof_queue = os.getenv('EOF_QUEUE')
    worker_quantity = int(os.getenv('WORKERS_QUANTITY'))
    next_worker_quantity = int(os.getenv('NEXT_WORKER_QUANTITY'))
    iteration_queue = os.getenv('ITERATION_QUEUE')
    eof_quantity = os.getenv('EOF_QUANTITY')
    last = True if os.getenv('LAST') == '1' else False
    log = os.getenv('LOG')

    worker = FilterWorker(worker_id, source_name, output_name, eof_queue, worker_quantity, next_worker_quantity, iteration_queue, int(eof_quantity), last, log)
    worker.set_filter_type('YEAR', year_range_condition, years)
    worker.run()


main()  