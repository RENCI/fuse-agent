FROM python:3.10-buster

COPY ./requirements.txt /app/requirements.txt

RUN pip install -r /app/requirements.txt

RUN apt-get update

RUN apt-get -y install apt-transport-https ca-certificates curl gnupg2 software-properties-common

COPY . /app
COPY ./fuse /fuse

CMD uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 2
