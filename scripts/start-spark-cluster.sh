# this script create cluster with 2 worker node and a history server, increase slave=2 to 3 or any number
# to add more workers
docker-compose build
docker-compose up -d --scale slave=2