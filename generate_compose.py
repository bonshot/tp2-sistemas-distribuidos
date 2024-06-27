# Script to generate docker-compose-dev.yaml according to multiple config files named config_file_Qi 
# where Qi es the query number i
# Usage: ./generate_compose.sh
# The configuration file should be in the format:
# - service_name1, dockerfile_path
# + env_vars: key1=value1,key2=value2
# - service_name2, dockerfile_path
# + env_vars: key1=value1,key2=value2
# ...
# - service_name, dockerfile_path
# + env_vars: key1=value1,key2=value2

# Define config file
config_file = "config_file_queries.config"
container_config = "containers_list.config"

# Dictionary to store the environment variables of each service
env_vars = {}

# List for storing the services and their Dockerfile paths
services = []

# Read the configuration file
with open(config_file, "r") as file:
    service_name, dockerfile_path = None, None
    last_service_name = None
    for line in file:
        line = line.strip()
        if line.startswith("-"):
            # Service name and Dockerfile path
            line = line[1:]  # Delete the prefix '-'
            service_name, dockerfile_path = line.split("$")
            services.append((service_name.strip(), dockerfile_path.strip()))
        elif line.startswith("+"):
            # Environment variables
            line = line[1:]  # Delete the prefix '+'
            env_vars[service_name] = dict(item.split("=") for item in line.split("$"))

            if "END" in env_vars[service_name]:
                current_eof_quantity = env_vars["query_coordinator_worker"]["EOF_QUANTITY"].split(",")
                index = 0
                match service_name:
                    case "filter_year_worker_q1-":
                        index = 0
                    case "global_decade_counter_worker":
                        index = 1
                    case "filter_review_counter_worker":
                        index = 2
                    case "top_10_worker_last":
                        index = 3
                    case "percentile_worker":
                        index = 4
                current_eof_quantity[index] = env_vars[service_name]["WORKERS_QUANTITY"]

            if last_service_name and "NEXT_WORKER_QUANTITY" in env_vars[last_service_name] and service_name !="mean_review_sentiment_worker":
                if not "END" in env_vars[last_service_name] and last_service_name != "review_sentiment_worker":
                    env_vars[last_service_name]["NEXT_WORKER_QUANTITY"] = env_vars[service_name]["WORKERS_QUANTITY"]
            if "ACCUMULATOR" in env_vars[service_name] and env_vars[service_name]["ACCUMULATOR"] == "True":
                env_vars[service_name]["EOF_QUANTITY"] = env_vars[last_service_name]["WORKERS_QUANTITY"]
            last_service_name = service_name
            if not "ADDRESS" in env_vars[service_name] or not "PORT" in env_vars[service_name]:
                env_vars[service_name]["ADDRESS"] = service_name
                if "container_coordinator" in service_name:
                    env_vars[service_name]["COORDS_PORT"] = 1234
                    continue
                env_vars[service_name]["PORT"] = 4321  
        else:
            continue

# Generate docker-compose-dev.yaml
with open("docker-compose-dev.yaml", "w") as outfile, open(container_config, "w") as containers_list_file:
    outfile.write("services:\n")
    # Escribir servicios predefinidos
    outfile.write("  rabbitmq:\n")
    outfile.write("    container_name: rabbit\n")
    outfile.write("    build:\n")
    outfile.write("      context: ./rabbitmq\n")
    outfile.write("      dockerfile: rabbitmq.dockerfile\n")
    outfile.write("    ports:\n")
    outfile.write("      - 15672:15672\n")
    outfile.write("    healthcheck:\n")
    outfile.write("      test: [\"CMD\", \"curl\", \"-f\", \"http://localhost:15672\"]\n")
    outfile.write("      interval: 10s\n")
    outfile.write("      timeout: 5s\n")
    outfile.write("      retries: 10\n")
    outfile.write("    environment:\n")
    outfile.write("      - RABBITMQ_LOGS=-1\n")
    outfile.write("\n")
    outfile.write("  server:\n")
    outfile.write("    container_name: server\n")
    outfile.write("    build:\n")
    outfile.write("      context: .\n")
    outfile.write("      dockerfile: ./server/server.dockerfile\n")
    outfile.write("    depends_on:\n")
    outfile.write("      - rabbitmq\n")
    outfile.write("    links:\n")
    outfile.write("      - rabbitmq\n")
    outfile.write("    environment:\n")
    outfile.write("      - PYTHONUNBUFFERED=1\n")
    outfile.write("      - HOST=server\n")
    outfile.write("      - PORT=12345\n")
    outfile.write("      - LISTEN_BACKLOG=1\n")
    outfile.write("      - HC_PORT=4321\n")
    outfile.write("      - LOG=server_log\n")
    outfile.write(f'    volumes:\n')
    outfile.write(f'      - ./persisted_data:/persisted_data\n')
    outfile.write("\n")
    outfile.write("  client:\n")
    outfile.write("    container_name: client\n")
    outfile.write("    build:\n")
    outfile.write("      context: .\n")
    outfile.write("      dockerfile: ./client/client.dockerfile\n")
    outfile.write("    depends_on:\n")
    outfile.write("      - server\n")
    outfile.write("    links:\n")
    outfile.write("      - server\n")
    outfile.write("    environment:\n")
    outfile.write("      - PYTHONUNBUFFERED=1\n")
    outfile.write("      - HOST=server\n")
    outfile.write("      - PORT=12345\n")
    outfile.write("      - TITLES_FILEPATH=./datasets/books_data.csv\n")
    outfile.write("      - REVIEWS_FILEPATH=./datasets/books_rating_sample.csv\n")
    outfile.write("    volumes:\n")
    outfile.write("      - ./datasets:/datasets\n")
    outfile.write("\n")
    containers_list_file.write("server\n")
    coords_list = []
    for service_name, dockerfile_path in services:
        if service_name in env_vars:
            # Write as many workers as specified in WORKERS_QUANTITY
            if "WORKERS_QUANTITY" in env_vars[service_name]:
                workers_quantity = int(env_vars[service_name]["WORKERS_QUANTITY"])
                if not coords_list and "container_coordinator" in service_name:
                    coords_list = [f"{service_name}{str(i)}" for i in range(workers_quantity)]
                for i in range(workers_quantity):
                    worker_name = f"{service_name}{str(i)}"
                    containers_list_file.write(f"{worker_name}\n") 
                    outfile.write(f"  {worker_name}:\n")
                    outfile.write(f"    container_name: {worker_name}\n")
                    outfile.write(f"    build:\n")
                    outfile.write(f"      context: .\n")
                    outfile.write(f"      dockerfile: {dockerfile_path}\n")
                    if service_name == "query_coordinator_worker":
                        outfile.write(f'    depends_on:\n')
                        outfile.write(f'      - rabbitmq\n')
                        outfile.write(f'    links:\n')
                        outfile.write(f'      - rabbitmq\n')
                    if "container_coordinator" not in worker_name:
                        outfile.write(f'    volumes:\n')
                        outfile.write(f'      - ./persisted_data:/persisted_data\n')
                    else:
                        outfile.write(f'    volumes:\n')
                        outfile.write(f'      - /var/run/docker.sock:/var/run/docker.sock\n')
                        outfile.write(f'    user: root\n')
                    outfile.write(f"    environment:\n")
                    for key, value in env_vars[service_name].items():
                        if key == "WORKER_ID" or key == "ID":
                            outfile.write(f"      - {key}={i}\n")
                        elif key == "ADDRESS":
                            outfile.write(f"      - {key}={worker_name}\n")
                        else:
                            outfile.write(f"      - {key}={value}\n")
                    if "container_coordinator" in worker_name:
                        coords_list_string = ",".join(coords_list)
                        outfile.write(f"      - COORDS_LIST={coords_list_string}\n")
                    outfile.write("\n")