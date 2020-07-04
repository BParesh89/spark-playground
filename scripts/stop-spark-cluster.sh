# stop master, worker nodes and history server
docker-compose stop master slave history-server
docker rm -f $(docker ps -a -q)
docker volume rm -f $(docker volume ls -q)