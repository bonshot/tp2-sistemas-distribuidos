from workers import GlobalDecadeWorker
import os


    
def main():

    data_source_name = os.getenv('DATA_SOURCE_NAME')
    data_output_name = os.getenv('DATA_OUTPUT_NAME')
    eof_quantity = int(os.getenv('EOF_QUANTITY'))
    iteration_queue = os.getenv('ITERATION_QUEUE')
    next_workers_quantity = int(os.getenv('NEXT_WORKERS_QUANTITY'))
    worker_id = os.getenv('WORKER_ID')
    log = os.getenv('LOG')
    
    worker = GlobalDecadeWorker(worker_id, data_source_name, data_output_name, eof_quantity, iteration_queue, next_workers_quantity, log)
    worker.run()


main()

    