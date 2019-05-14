docker image build -t tadacombine:latest  .
docker container run --interactive --tty --rm -p 5000:5000 --name tadacombine tadacombine:latest
