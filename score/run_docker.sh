docker image build -t tadascore:latest  .
docker container run --interactive --tty --rm -p 5000:5000 --name tadascore tadascore:latest
