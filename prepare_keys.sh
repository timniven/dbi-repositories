#!/bin/bash
# script to prepare keys for testing postgres tls

# copied from here:
# https://www.vertica.com/docs/9.2.x/HTML/Content/Authoring/Security/SSL/GeneratingCertificationsAndKeys.htm
openssl genrsa -out servercakey.pem
openssl req -new -x509 -key servercakey.pem -out serverca.crt
openssl genrsa -out server.key
openssl req -new -key server.key -out server_reqout.txt
openssl x509 -req -in server_reqout.txt -days 3650 -sha256 -CAcreateserial -CA serverca.crt -CAkey servercakey.pem -out server.crt
openssl genrsa -out client.key
openssl req -new -key client.key -out client_reqout.txt
openssl x509 -req -in client_reqout.txt -days 3650 -sha256 -CAcreateserial -CA serverca.crt -CAkey servercakey.pem -out client.crt
rm client.crt
rm client.key
rm client_reqout.txt
rm server_reqout.txt
rm serverca.crt
rm serverca.srl
rm servercakey.pem
mv server.key docker/provisions/postgres/certs/server.key
mv server.crt docker/provisions/postgres/certs/server.crt
sudo chown 999:999 docker/provisions/postgres/certs/server.key
sudo chmod 600 docker/provisions/postgres/certs/server.key
