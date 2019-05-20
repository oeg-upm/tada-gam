#docker-compose build
docker-compose down
docker-compose up -d --build  --scale score=3 --scale combine=4
docker-compose ps
