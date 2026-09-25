# Streaming Dataplatform
This code corresponds with this demo [Building a Real-Time Chrome Event Data Platform](https://lnkd.in/p/dXp8JnaR) by Manantsoa. This shows how to run our platform:

- Run all service using docker compose
- Run our chrome extension and our python deamon in my ubuntu 20.04 
- Start our realtime dashboard to see the result

# Run all service using docker compose 
Step 1- Clone the project
```bash
git clone https://github.com/NtsoaBe/streaming_project.git
```
Step 2- Go inside the folder project
```bash
cd streaming_project
```
Step 3- Inside the docker_folder directory run all service
```bash
cd docker_folder
docker compose up
```
Step 4- After all service are started, open a new terminal , you need to stay in the docker_folder folder to perform these step, then create our kafka topic
```bash
docker compose exec kafka bash

kafka-topics \
    --bootstrap-server localhost:9092 \
    --create \
    --topic chrome-clicks \
    --partitions 4 \
    --replication-factor 1
```
Step 5- Open a new terminal, you need to stay in the docker_folder folder to perform these step, and create connect to ksqldb with ksqldb client then create all stream that we need to retrieve data from Kafka
```bash
docker compose exec ksqldb-cli  ksql http://ksqldb-server:8088

CREATE STREAM chrome_clicks (
    x INTEGER,
    y INTEGER,
    pageName VARCHAR,
    timestamp BIGINT
) WITH (
    KAFKA_TOPIC = 'chrome-clicks',
    VALUE_FORMAT = 'JSON'
);

CREATE STREAM silver_chrome_event
WITH (
    KAFKA_TOPIC = 'silver_chrome_event',
    VALUE_FORMAT = 'JSON'
) AS
SELECT
    x,
    y,
    CASE
        WHEN LCASE(pageName) LIKE '%facebook%' OR LCASE(pageName) LIKE '%gmail%' THEN 'social_network'
        WHEN LCASE(pageName) LIKE '%stack overflow%' OR LCASE(pageName) LIKE '%medium%' OR LCASE(pageName) LIKE '%trino%' THEN 'learning'
        ELSE 'other'
    END AS pageName,
    timestamp,

    TIMESTAMPTOSTRING(
        timestamp,
        'yyyy-MM-dd HH:mm:ss'
    ) AS event_datetime,

    TIMESTAMPTOSTRING(
        timestamp,
        'yyyy-MM-dd'
    ) AS event_date,

    -- 30-minute bucket
    CASE
        WHEN CAST(
            TIMESTAMPTOSTRING(timestamp, 'mm') AS INTEGER
        ) < 30
        THEN CONCAT(
            TIMESTAMPTOSTRING(timestamp, 'HH'),
            ':00'
        )
        ELSE CONCAT(
            TIMESTAMPTOSTRING(timestamp, 'HH'),
            ':30'
        )
    END AS tranche_30min,
    CONCAT(
        TIMESTAMPTOSTRING(timestamp, 'HH'),
        ':',
        LPAD(
            CAST(
                (CAST(TIMESTAMPTOSTRING(timestamp, 'mm') AS INTEGER) / 5) * 5 
                AS VARCHAR
            ),
            2,
            '0'
        )
    ) AS tranche_5min

FROM chrome_clicks
EMIT CHANGES;

```
Step 6- Define the schema and pinot table to persist the data from ksqlDB, open an new terminal always stay in the docker_folder folder
```sh
curl -X POST \
  -H "Content-Type: application/json" \
  -d @pinot/config/space_event/schema_chrome_event.json \
  http://localhost:9010/schemas

curl -X POST \
  -H "Content-Type: application/json" \
  -d @pinot/config/space_event/tabe_chrome_event.json \
  http://localhost:9010/tables


```
Step 7- Go to the main folder (streaming_project) then create a python venv, and install all requirements from requirements.txt
```bash
python3 -m venv .myvenv
source .myvenv/bin/activate

pip install -r requirements.txt
```
Step 8- Go to the **streaming_project/click_tracker/server** folder, then start our python script who listen data from chrome extension
```bash
python producer_browser.py
```
Step 9- Deploy our chrome extension
── content.js
├── manifest.json
from **streaming_project/click_tracker**

Once you're finished, tear everything down using the following command:
```sh
docker-compose down
```
