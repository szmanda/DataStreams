# Kafka, Stream Processing - US Flights (flink)

link do pliku USFlights.jar: https://drive.google.com/drive/folders/1dISE_GEEgcqyNIERNW8AfaOGDVBk15QO?usp=sharing

```
┌─────────────────┐    ┌─────────────────┐
│ Flights         │    │ Airports        │
│ CSV source      │    │ CSV source      │
└────────┬────────┘    └────────┬────────┘
         │                      │         
         │                      │         
         │                      │         
┌────────▼────────┐             │         
│ flight-records  │             │         
│ Kafka topic     │             │         
└────────┬────────┘             │         
         │                      │         
         │                      │         
         │                      │         
┌────────▼────────┐             │         
│ flights         │  map state  │         
│ DS<Flight>      ◄─────────────┘         
└────────┬────────┘                       
         │ 1. Separated by state          
         │ 2. Assigned to windows by date 
         │ 3. Transformed to CombinedDelay
┌────────▼────────┐    ┌─────────────────┐
│ aggregated      │    │ delay_etl_image │
│ DS<CombineDelay>├────► MySQL table     │
└─────────────────┘    └─────────────────┘
```

# Opis przetwarzania

## Transformacje utrzymujące obraz czasu rzeczywistego

Dane o lotach, z producenta Kafki, są ładowane do pierwszego DataStream. Napotkawszy błąd resetujących się watermarków, o którym utworzyłem wątek na forum (\[Flink\] Resetujące się watermarki?) byłem zmuszony zrezygnować z równoległości przetwarzania źródła. Na chwilę obecną nie znam przyczyny, więcej szczegółów we wspomnianym wątku. (na tę chwilę podejżewałbym moją klasę `FlightWatermarkStragety`, ale już brakuje czasu by szukać błędów)

```java
DataStream<Flight> inputStream = env.fromSource(
  Connectors.getKafkaSourceFlight(properties),
  new FlightWatermarkStrategy(),
  "FlightsKafkaInput").setParallelism(1);
```

Wykorzystany watermark pozwala na przyjęcie zdarzeń opóźnionych o maksymalnie wartość parametru `watermark.delay_ms`

Następnie dane są uzupełnione o dodatkowe informacje ze statycznego źródła `airports.csv`. Wykorzystałem do tego celu proste mapowanie (dodając dodatkowe atrybuty do klasy Flight):

```java
Airport airport = airportsMap.get(flight.getCurrentAirport());
flight.setState(airport.getState());
flight.setTimeZone(airport.getTimezone());
```

Strumień lotów jest następnie dzielony na podstawie przypisanych im stanów, oraz podzielony na okna za pomocą daty, a następnie agregowany:

```java
DataStream<CombinedDelay> aggregated = flights
  .keyBy(Flight::getState)
  .window(new DelayWindowAssigner(properties.get("update.mode")))
  .aggregate(flightAggregate)
  ;
```

Przydział do okien następuje niezależnie od trybu, zawsze na podstawie dnia. Okno otwiera się kaźdego dnia o północy, a zamyka zaraz przed kolejnym. To co róźni się dla trybu A i C to wykorzystane wyzwalacze:

## Wyzwalacz dla trybu A

Tryb A -- jak najszybciej zwracającego niekompletne dane, który je w razie potrzeby aktualizując, wymaga wywołania wyzwalacza na oknie, przy każdym nowym wydarzeniu należącym do tego okna. Dlatego jego metoda `onEvent()` zawsze zwraca `TriggerResult.FIRE`.


## Wyzwalacz dla trybu C

Tryb C -- jak najszybciej zwracający kompletne dane, wymaga wywołania wyzwalacza na oknie jedynie gdy zostanie ono przekroczone przez watermark, stąd jego metoda `onEventTime()` ma następujący kształt.

```java
if (timestamp + FlightWatermarkStrategy.MAX_DELAY >= timeWindow.maxTimestamp()) {
  return TriggerResult.FIRE;
} else {
  return TriggerResult.CONTINUE;
}
```

## Agregacja

Agregacja przekształca `Flight` w obiekt `CombinedDelay` a następnie wykonuje merge polegający na dodaniu wszystkich agregowanych elementów parami: 

```java
if (flight.getInfoType().equals("A")) {
    combined.setArrivalCount(1);
    combined.setArrivalDelay(flight.getTotalDelayInteger());
}
if (flight.getInfoType().equals("D")) {
    combined.setDepartureCount(1);
    combined.setDepartureDelay(flight.getTotalDelayInteger());
}
```

```java
a.setDelay(a.getDelay() + b.getDelay());
a.setArrivalDelay(a.getArrivalDelay() + b.getArrivalDelay());
a.setDepartureDelay(a.getDepartureDelay() + b.getDepartureDelay());
a.setArrivalCount(a.getArrivalCount() + b.getArrivalCount());
a.setDepartureCount(a.getDepartureCount() + b.getDepartureCount());
```

## Zapisanie obrazu czasu rzeczywistego

Obraz czasu rzeczywistego jest zapisany za pomocą dockerowego obrazu mysql. Tworzony jest użytkownik i baza danych -- te dane są potem parametryzowane wewnątrz `flink.parameters`, oraz wykorzystane przez aplikację do połączenia z bazą danych.

Wszystkie operacje można zamknąć w poniższych poleceniach SQL

```sql
CREATE USER 'streamuser'@'%' IDENTIFIED BY 'stream';

CREATE DATABASE IF NOT EXISTS streamdb CHARACTER SET utf8;

GRANT ALL ON streamdb.* TO 'streamuser'@'%';

CREATE TABLE delay_etl_image (
    state VARCHAR(255)
  , arrival_count INT
  , departure_count INT
  , arrival_delay INT
  , departure_delay INT
  , date VARCHAR(64)
);

INSERT INTO delay_etl_image (state, arrival_count, departure_count, arrival_delay, departure_delay, date) VALUES (?, ?, ?, ?, ?, ?)
```

### Dlaczego MySQL

MySql jako popularna i sprawdzona platforma bazodanowa, z racji dojrzałości tego rozwiązania oferuje rozsądną wydajność i niezawodność, a także szeroki wachlarz wsparcia wśród różnych bibliotek w tym dla Flinka. Pozwala przechować dane w sposób bezpieczny na awarie, nawet w przypadku wystąpienia błędów w przetwarzaniu strumieniowym, dane wynikowe pozostaną bezpiecznie zapisane.

# Uruchomienie

## Inicjalizacja

### Uruchomienie klastra

```sh
gcloud dataproc clusters create ${CLUSTER_NAME} \
--enable-component-gateway --region ${REGION} --subnet default \
--master-machine-type n1-standard-4 --master-boot-disk-size 50 \
--num-workers 2 --worker-machine-type n1-standard-2 --worker-boot-disk-size 50 \
--image-version 2.1-debian11 --optional-components FLINK,DOCKER,ZOOKEEPER \
--project ${PROJECT_ID} --max-age=2h \
--metadata "run-on-master=true" \
--initialization-actions \
gs://goog-dataproc-initialization-actions-${REGION}/kafka/kafka.sh
```



### Pobranie/Załadowanie danych:
Jeśli dane masz gdzie indziej, zmodyfikuj `flink.properties:fileInput.dir` oraz `flink.properties:airports.input`

```sh
## Załadowanie danych do bucketa
export DATA_DIR="./flights-data"
mkdir -p DATA_DIR
wget https://www.cs.put.poznan.pl/kjankiewicz/bigdata/stream_project/airports.csv -P $DATA_DIR
wget https://www.cs.put.poznan.pl/kjankiewicz/bigdata/stream_project/flights-2015.zip -P $DATA_DIR
unzip ${DATA_DIR}/flights-2015.zip -d ${DATA_DIR}/flights-2015
rm ${DATA_DIR}/flights-2015.zip
gsutil mv -r $DATA_DIR gs://pbd-24-ms/labs/flights-data/
# Można użyć gsutil cp aby od razu pozostawić dane w klastrze 
# gsutil cp -r $DATA_DIR gs://pbd-24-ms/labs/flights-data/

## Załadowanie danych z bucketa
export DATA_DIR="./flights-data"
mkdir -p $DATA_DIR
gsutil cp -r gs://pbd-24-ms/labs/flights-data/* $DATA_DIR
```

Dodanie praw do wykonania skryptów `*.sh`
```sh
find ./ -type f -iname "*.sh" -exec chmod +x {} ';'
```

Skrypt ustawiający/resetujący temat kafki: `./kafka-reset.sh`
Konfiguracja bazy danych do przechowania obrazów czasu rzeczywistego `./mysql.sh`
Uruchomienie producenta kafki: `./producer.sh`

Przy *Uruchomieniu konsumenta na klastrze GCP*, mam jakiś problem z widocznością pliku `airports.csv`, nie rozumiem dlaczego. Podejrzewam to że zadanie jest delegowane na osobną maszynę, na której nie ma dostępu do tego pliku. Dla lokalnego uruchomienia, działa mi poprawnie.

## Wyświetlenie zapisu obrazu czasu rzeczywistego

```sh
sudo docker exec -it mysql mysql -ustreamuser -pstream streamdb -e "SELECT * from delay_etl_image order by date desc limit 20;"
```


# Uruchomienie lokalne

Alternatywna metoda uruchomienia, której używałem podczas pisania kodu, z wykorzystaniem WSL.

Running Kafka locally, via WSL -- following: [Install and Run Kafka 3.2.0 On WSL (kontext.tech)](https://kontext.tech/article/1047/install-and-run-kafka-320-on-wsl#:~:text=Install%20and%20Run%20Kafka%203.2.0%20On%20WSL%201,the%20topic%20...%208%20Shutdown%20Kafka%20services%20)

```sh
sudo apt update
sudo apt install default-jre openjdk-11-jre-headless openjdk-8-jre-headless -y
java -version
echo "export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64" >> ~/.bashrc
source ~/.bashrc
wget https://archive.apache.org/dist/kafka/3.1.2/kafka_2.13-3.1.2.tgz
tar -xvzf kafka_2.13-3.1.2.tgz
echo "export KAFKA_HOME=~/kafka_2.13-3.1.2/" >> ~/.bashrc
source ~/.bashrc
## Start the zookeeper environment
$KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties
## Start kafka server
$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties
```

Reset kafka topic and run producer
```sh
$KAFKA_HOME/bin/kafka-topics.sh --delete --topic flight-records --bootstrap-server localhost:9092
$KAFKA_HOME/bin/kafka-topics.sh --create --topic flight-records --bootstrap-server localhost:9092

$KAFKA_HOME/bin/kafka-topics.sh --delete --topic flight-records --bootstrap-server localhost:9092
$KAFKA_HOME/bin/kafka-topics.sh --create --topic flight-records --bootstrap-server localhost:9092
java -cp USFlights.jar com.example.bigdata.TestProducer
```

```sh
## requires running Docker Desktop with WSL integration enabled
sudo docker run --name mysql -v /tmp/datadir:/var/lib/mysql -p 6033:3306 -e MYSQL_ROOT_PASSWORD=admin -d mysql:debian

sudo docker exec -it mysql mysql -uroot -padmin -e \
"CREATE USER 'streamuser'@'%' IDENTIFIED BY 'stream'; \
CREATE DATABASE IF NOT EXISTS streamdb CHARACTER SET utf8; \
GRANT ALL ON streamdb.* TO 'streamuser'@'%';"

sudo docker exec -it mysql mysql -ustreamuser -pstream streamdb -e \
"CREATE TABLE delay_etl_image ( \
    state VARCHAR(255) \
  , arrival_count INT \
  , departure_count INT \
  , arrival_delay INT \
  , departure_delay INT \
  , date VARCHAR(64) \
);"
```

Run producer
```sh
$KAFKA_HOME/bin/kafka-topics.sh --delete --topic flight-records --bootstrap-server localhost:9092
$KAFKA_HOME/bin/kafka-topics.sh --create --topic flight-records --bootstrap-server localhost:9092
java -cp USFlights.jar com.example.bigdata.TestProducer
```


Run consumer (requires docker mysql container or disabling mysql sink in flink.properties)
```sh
java -cp USFlights.jar com.example.bigdata.USFlightsApp
```

Displaying real time etl image
```sh
sudo docker exec -it mysql mysql -ustreamuser -pstream streamdb -e "SELECT * from delay_etl_image order by date desc limit 20;"
```

Clearing results, Removing mysql container
```sh
sudo docker exec -it mysql mysql -ustreamuser -pstream streamdb -e "delete from delay_etl_image;"
sudo docker stop mysql
sudo docker rm mysql
```


## Przykładowe z uzyskanych wyników

Komentarz: Chyba obrałem błędną definicję obliczania czasu opóźnień, skoro dla odlotów wyszły mi same zera. Faktycznie jedynie sumowałem wartości przypisane przyczyn opóźnień (airSystemDelay, securityDelay, airlineDelay, lateAircraftDelay, weatherDelay). Ale też uważam to za mało istotny szczegół, który w zasadzie nie wpływa na kształt przetwarzania.

`SELECT * from delay_etl_image where state = 'CA' order by date desc limit 50;` (w trakcie przetwarzania)
```
+-------+---------------+-----------------+---------------+-----------------+------------+
| state | arrival_count | departure_count | arrival_delay | departure_delay | date       |
+-------+---------------+-----------------+---------------+-----------------+------------+
| CA    |          2024 |            2031 |         12685 |               0 | 2015-09-02 |
| CA    |          2031 |            2013 |          7623 |               0 | 2015-09-01 |
| CA    |          2099 |            2080 |         12491 |               0 | 2015-08-31 |
| CA    |          1888 |            1950 |         11160 |               0 | 2015-08-30 |
| CA    |          1713 |            1672 |         13798 |               0 | 2015-08-29 |
| CA    |          2109 |            2090 |          8159 |               0 | 2015-08-28 |
| CA    |          2076 |            2088 |         13269 |               0 | 2015-08-27 |
| CA    |          2048 |            2054 |         13242 |               0 | 2015-08-26 |
| CA    |          2014 |            2010 |         17327 |               0 | 2015-08-25 |
| CA    |          2065 |            2056 |         15091 |               0 | 2015-08-24 |
```

`SELECT * from delay_etl_image order by date desc limit 50;` (po zakończeniu)
```
+-------+---------------+-----------------+---------------+-----------------+------------+
| state | arrival_count | departure_count | arrival_delay | departure_delay | date       |
+-------+---------------+-----------------+---------------+-----------------+------------+
| AK    |            75 |              78 |           633 |               0 | 2015-12-30 |
| MT    |            49 |              49 |          1030 |               0 | 2015-12-30 |
| ME    |            16 |              16 |           499 |               0 | 2015-12-30 |
| MI    |           368 |             372 |          9465 |               0 | 2015-12-30 |
| KY    |            98 |              97 |          3514 |               0 | 2015-12-30 |
| NC    |           424 |             428 |         17939 |               0 | 2015-12-30 |
| NE    |            62 |              62 |          2445 |               0 | 2015-12-30 |
| GA    |          1009 |            1030 |         61235 |               0 | 2015-12-30 |
| IL    |          1131 |            1102 |         50866 |               0 | 2015-12-30 |
| SC    |            76 |              77 |          2639 |               0 | 2015-12-30 |
| RI    |            35 |              35 |          1278 |               0 | 2015-12-30 |
| ID    |            60 |              60 |           591 |               0 | 2015-12-30 |
| ND    |            50 |              50 |           918 |               0 | 2015-12-30 |
```