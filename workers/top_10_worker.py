from workers import TopNWorker
import os 
    
    
def main():

    top_n = int(os.getenv('TOP_N'))
    data_source_name = os.getenv('DATA_SOURCE_NAME')
    data_output_name = os.getenv('DATA_OUTPUT_NAME')
    workers_quantity = int(os.getenv('EOF_QUANTITY'))
    iteration_queue = os.getenv('ITERATION_QUEUE')
    worker_id = os.getenv('WORKER_ID')
    last = True if os.getenv('LAST') == '1' else False
    next_workers_quantity = int(os.getenv('NEXT_WORKERS_QUANTITY'))
    log = os.getenv('LOG')

    
    worker = TopNWorker(worker_id, data_source_name, data_output_name, workers_quantity, top_n, last, iteration_queue, next_workers_quantity, log)
    worker.run()


main()
