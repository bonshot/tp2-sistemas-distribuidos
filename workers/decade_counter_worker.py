from workers import DecadeWorker
import os
    
def main():

    data_source_name = os.getenv('DATA_SOURCE_NAME')
    data_output_name = os.getenv('DATA_OUTPUT_NAME')
    eof_quantity = int(os.getenv('EOF_QUANTITY'))
    worker_id = os.getenv('WORKER_ID')
    next_workers_quantity = int(os.getenv('NEXT_WORKERS_QUANTITY'))
    log = os.getenv('LOG')

    worker = DecadeWorker(data_source_name, data_output_name, worker_id, next_workers_quantity, eof_quantity, log)
    worker.run()
    
main()

    