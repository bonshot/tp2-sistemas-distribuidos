from workers import PercentileWorker
import os
    
def main():

    data_source_name = os.getenv('DATA_SOURCE_NAME')
    data_output_name = os.getenv('DATA_OUTPUT_NAME')
    percentile = os.getenv('PERCENTILE')
    workers_quantity = int(os.getenv('EOF_QUANTITY'))
    iteration_queue = os.getenv('ITERATION_QUEUE')
    next_workers_quanity = int(os.getenv('NEXT_WORKERS_QUANTITY'))
    worker_id = os.getenv('WORKER_ID')
    log = os.getenv('LOG')
    

    worker = PercentileWorker(worker_id, data_source_name, data_output_name, percentile, workers_quantity, iteration_queue, next_workers_quanity, log)
    worker.run()


main()
