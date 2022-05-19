FROM python:3.10-buster

RUN apt-get update
RUN apt-get -y install apt-transport-https ca-certificates curl gnupg2 software-properties-common

EXPOSE 8000

COPY . /app
RUN pip install -r /app/requirements.txt
RUN pip install -i https://test.pypi.org/simple/ fuse-cdm==1.5.0

WORKDIR /app

CMD ./startup.sh
