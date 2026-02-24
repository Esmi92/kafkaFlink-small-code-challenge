# Coding Challenge
## Overview

Este proyecto integra un productor de Kafka (incluyendo creación de tópicos) y un job de Flink (como consumidor) para enriquecer y almacenar mensajes de notificación.

El flujo funciona así:

- Una API dentro de la misma aplicación recibe una solicitud POST y publica un mensaje en el *Topic* `transactions-events`.
- Kafka realiza una validación básica y envía el mensaje al *Topic*.
- El mensaje es consumido por:
   - Un job de **Flink**, encargado de enriquecerlo con datos provenientes de **MongoDB**.
   - Una tabla **REALTIME** de **Pinot**, donde se almacena el mensaje crudo tal como lo generó el productor.
- Si el job de Flink **no encuentra** la cuenta en MongoDB, envía el mensaje al tópico `transactions-dlq`.
- Si el mensaje sí **es enriquecido**, se envía al tópico `transactions-enriched`, que puede ser consumido por cualquier otro servicio para enviar la notificación final.

## Requisitos para correr el proyecto
- Docker Desktop
- Java (JDK): https://www.oracle.com/java/technologies/downloads/
- Maven (~3.9.12): https://maven.apache.org/install.html 

## Pasos a seguir
1. Clonar el repositorio
2. **Construir los proyectos Java**: Dentro del repositorio existen dos proyectos Java que requieren *build*:
   - flink-job
   - stream-app
   En cada uno ejecuta: `mvn clean package`
3. **Levantar la infraestructura con Docker**: Desde la raíz del proyecto (donde está `docker-compose.yml`) ejecutar el comando `docker-compose up -d`
4. **Validar contenedores**: Ejecuta `docker ps`, debes ver corriendo:
   - jobmanager
   - taskmanager
   - kafka
   - job
   - stream-app
   - mongo
   - pinot-zookeeper
   - pinot-controller
   - pinot-broker
   - pinot-server
5. **Validar servicios**: 
  - Flink UI: http://localhost:8081/#/job/running
  - Pinot UI: http://localhost:9000/
6. **Crear tabla y schema en Pinot**
```
docker exec -it pinot-controller \
  /opt/pinot/bin/pinot-admin.sh AddTable \
  -tableConfigFile /config/table.json \
  -schemaFile /config/schema.json \
  -controllerHost pinot-controller \
  -controllerPort 9000 \
  -exec

```
7. **Valida en Pinot**:
   - Tablas: http://localhost:9000/tables
   - Schemas: http://localhost:9000/schemas

## Probar flujo
1. Enviar un POST a la API: 
 ```
 curl -X POST http://localhost:8080/clientTransaction \
  -H "Content-Type: application/json" \
  -d '{"accountNumber":1,"amount":250.75,"timestamp":1771881124,"location":"Mexico City"}'
 ```
2. Ver resultados en:
  - Tabla Pinot: **Query Console** >`select * from "transactions-events" limit 10`
  - Topic `transactions-events`
  - Topic `transactions-enriched`
  Para ver mensajes en un *Topic*:
  ```
  docker exec -it kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic transactions-<name> \
  --from-beginning
  ```
3. Pobrar DLQ *Topic*, para ver mensajes en `transactions-dlq`, envía un request con un *accountNumber* que no exista en `mongo-init.js` (la base de datos demo).

**Nota**: Si quieres terminar y limpiar tus containers, puedes ejecutar `docker-compose down -v`, luego `docker container prune` y por ultimo borra tus imagenes en Docker Desktop UI. 

## Troubleshooting
Si al ejecutar `docker ps` algún contenedor no está corriendo:
1. Revisa si llego a iniciarse con `docker ps -a`
2. Si terminó con código **137**, probablemente fue por falta de memoria.
4. Intenta reiniciarlo:
 ```
 docker start <nombre>
 docker-compose up -d <nombre>
 ```

