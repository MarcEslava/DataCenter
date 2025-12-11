docker compose up -d --build

docker compose run --rm apiserver airflow db migrate

docker compose up -d

docker compose ps

mkdir -p secrets

docker compose run -rm broker kafka-configs --bootstrap-server broker:29092 \
    --alter --add-config 'SCRAM-SHA-512=[password=CHANGE_ME]' \
    --entity-type users --entity-name app1

docker run --rm -v "${PWD}:/work" -w /work eclipse-temurin:17-jdk-jammy \
    keytool -genkeypair -alias kafka-broker \
    -keystore server.keystore.jks -storepass CHANGE_ME -keypass CHANGE_ME \
    -keyalg RSA -keysize 4096 -validity 3650 \
    -dname "CN=kafka.example.com, OU=Dev, O=Org, L=City, ST=Region, C=US" \
    -ext "SAN=dns:kafka.example.com"

docker run --rm -v "${PWD}:/work" -w /work eclipse-temurin:17-jdk-jammy \
    keytool -exportcert -alias kafka-broker \
    -keystore server.keystore.jks -storepass CHANGE_ME \
    -rfc -file server.crt

docker run --rm -v "${PWD}:/work" -w /work eclipse-temurin:17-jdk-jammy \
    keytool -importcert -alias kafka-broker \
    -file server.crt -keystore server.truststore.jks \
    -storepass CHANGE_ME -noprompt

Set-Content -NoNewline -Path keystore_creds   -Value 'CHANGE_ME'
Set-Content -NoNewline -Path key_creds        -Value 'CHANGE_ME'
Set-Content -NoNewline -Path truststore_creds -Value 'CHANGE_ME'

printf 'CHANGE_ME' > keystore_creds
printf 'CHANGE_ME' > key_creds
printf 'CHANGE_ME' > truststore_creds

docker compose logs apiserver