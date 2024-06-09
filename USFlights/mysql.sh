docker run --name mysql -v /tmp/datadir:/var/lib/mysql -p 6033:3306 -e MYSQL_ROOT_PASSWORD=admin -d mysql:debian

docker exec -it mysql mysql -uroot -padmin -e \
"CREATE USER 'streamuser'@'%' IDENTIFIED BY 'stream'; \
CREATE DATABASE IF NOT EXISTS streamdb CHARACTER SET utf8; \
GRANT ALL ON streamdb.* TO 'streamuser'@'%';"

docker exec -it mysql mysql -ustreamuser -pstream streamdb -e \
"CREATE TABLE delay_etl_image ( \
    state VARCHAR(255) \
  , arrival_count INT \
  , departure_count INT \
  , arrival_delay INT \
  , departure_delay INT \
);"