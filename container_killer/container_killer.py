import os
import docker
import random
import time
import signal

CONTAINERS_FILE = './containers_list.config'
SLEEP_START = 4
SLEEP_STOP = 10
QUANTITY_START = 1
QUANTITY_STOP = 4

class ContainerKiller:

    def __init__(self, mode):
        signal.signal(signal.SIGTERM, self.handle_signal)

        self.docker_client = docker.from_env()
        self.mode = mode
        self.containers = []
        self.stop = False

    def handle_signal(self, *args):
        self.docker_client.close()
        self.stop = True

    def get_containers(self):
        with open(CONTAINERS_FILE, 'r') as file:
            for line in file.readlines():
                self.containers.append(line.rstrip('\n'))

    def run(self):
        self.get_containers()
        if self.mode == 'random':
            self.random_mode()
        elif self.mode == 'coord':
            self.coord_mode()
        elif self.mode == 'gateway':
            self.gateway_mode()
        else:
            raise Exception('Unknown mode.')
        
        self.docker_client.close()

    def coord_mode(self):
        """
        This mode kills the container_coordinator 2 which is the leader,
        multiple times to show how the leader election works. In the middle, it
        also kills some workers.
        """
        time.sleep(15)

        containers_to_stop =    [   'container_coordinator_2', 'filter_category_computers_worker1', 
                                    'filter_year_worker_q1-1', 'top10_worker3', 'reviews_counter_worker2','reviews_counter_worker3'
                                ]
        self.stop_containers(containers_to_stop)

        time.sleep(3)

        containers_to_stop =    [   'filter_category_worker0', 'filter_year_worker_q3-1',
                                    'mean_review_sentiment_worker5'
                                ]
        self.stop_containers(containers_to_stop)

        time.sleep(10)

        containers_to_stop =    [   'container_coordinator_2', 'mean_review_sentiment_worker1',
                                    'review_sentiment_worker0', 'review_sentiment_worker1', 'percentile_worker0'
                                ]
        self.stop_containers(containers_to_stop)

    def gateway_mode(self):
        """
        In this mode, we will kill the QueryCoordinator and the server.
        Also some other workers will be killed.
        """
        time.sleep(10)

        containers_to_stop = ['server', 'reviews_counter_worker2', 'filter_year_worker_q1-2', 'filter_year_worker_q1-0']
        self.stop_containers(containers_to_stop)

        time.sleep(2)

        containers_to_stop = ['query_coordinator_worker0', 'top_10_worker_last0', 'filter_review_quantity_worker0']

        time.sleep(10)

        containers_to_stop = ['query_coordinator_worker0', 'server', 'mean_review_sentiment_worker1']
        self.stop_containers(containers_to_stop)

        time.sleep(10)

        containers_to_stop = ['server', 'mean_review_sentiment_worker0']
        self.stop_containers(containers_to_stop)
        

    def random_mode(self):
        """
        This mode kills random workers (it can be multiple at once) through out the 
        whole system execution. The only workers it wont kill are the container coordinator.
        """
        while not self.stop:
            sleep_time = random.randrange(SLEEP_START, SLEEP_STOP)
            time.sleep(sleep_time)

            containers_to_stop = self.get_random_containers()
            self.stop_containers(containers_to_stop)
    
    def get_random_containers(self):
        containers = []
        quantity = random.randrange(QUANTITY_START, QUANTITY_STOP)
        for _ in range(quantity):
            container_name = random.choice(self.containers)
            if 'container' not in container_name:
                containers.append(container_name)

        return containers 
    
    def stop_containers(self, containers):
        for container_name in containers:
            container = self.docker_client.containers.get(container_name)
            container.stop(timeout=10)


def main():
    mode = os.getenv('MODE')
    ck = ContainerKiller(mode)
    ck.run()


main()