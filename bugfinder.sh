#!/bin/bash

rm -rf debug/docker_logs/*

# Primero leemos los contenedores que chequearemos cada cierto tiempo si murieron para ver si hay
# que revivirlos
FILE="containers_list.txt"

# Verificar si el archivo existe
if [[ ! -f "$FILE" ]]; then
    echo "El archivo $FILE no existe."
    exit 1
fi

# Inicializar el array
lines=()

# Leer el archivo línea por línea y guardar cada línea en el array
while IFS= read -r line; do
    lines+=("$line")
done < "$FILE"

COUNTER=0

while true; do

    # Borra los archivos de los estados de la corrida anterior
    rm -rf persisted_data/*
    rm -rf debug/workers_logs/*

    # Ejecuta el comando docker compose en segundo plano
    nohup docker compose -f docker-compose-test.yaml up --build --remove-orphans > "debug/docker_logs/docker_output_${COUNTER}.log" 2>&1 &

    # Verifica si el contenedor rabbit está en ejecución
    # Si esta en ejecucion es porq el sistema ya empezo
    while true; do
        sleep 10
        if [ "$( docker container inspect -f '{{.State.Running}}' rabbit )" == "true" ]; then
            echo "rabbit is up and running"
            break
        else
            echo "rabbit container not found"
        fi
    done

    sleep 15
    # Como el sistema ya empezo, los clientes tambien
    # Ahora esperamos a que los clientes mueran
    while true; do
        sleep 5
        # Chequear cada contenedor para ver si esta vivo o no
        # Si no esta vivo, lo reiniciamos
        for line in "${lines[@]}"; do
            if [ line != client* ] && [ "$(docker inspect --format='{{.State.Running}}' $line)" == "false" ]; then
                docker stop $line
                docker start $line
            fi
        done
        sleep 5

        if [ "$(docker inspect --format='{{.State.Running}}' client_1)" == "false" ] && [ "$(docker inspect --format='{{.State.Running}}' client_2)" == "false" ]; then
            echo "Both clients finished"
            break
        else
            echo "Clients are still up"
        fi

    done

    sleep 2

    # Ejecutar el script de python que se fija si los resultados de los clientes dieron bien 
    python3 bugfinder.py
    # Capturar el código de salida del script de Python
    exit_code=$?
    # Verificar el código de salida
    if [ $exit_code -eq 0 ]; then
        echo "Ejecucion [$COUNTER]: OK"
    else
        echo "Ejecucion [$COUNTER]: ¡¡ERROR!! con exit_code [$exit_code]"
        nohup docker compose -f docker-compose-test.yaml stop -t 5 > "debug/docker_logs/docker_stop_${COUNTER}.log" 2>&1 &
        nohup docker compose -f docker-compose-test.yaml down > "debug/docker_logs/docker_down_${COUNTER}.log" 2>&1 &

        sleep 20
        break
    fi


    let COUNTER=COUNTER+1

    nohup docker compose -f docker-compose-test.yaml stop -t 5 > "debug/docker_logs/docker_stop_${COUNTER}.log" 2>&1 &
    nohup docker compose -f docker-compose-test.yaml down > "debug/docker_logs/docker_down_${COUNTER}.log" 2>&1 &

    sleep 20

done