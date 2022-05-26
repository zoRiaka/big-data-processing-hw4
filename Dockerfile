FROM python:3.9-slim

RUN apt-get update

RUN pip install --upgrade pip

RUN pip install cassandra-driver

COPY ./requirements.txt /opt/app/requirements.txt
RUN pip install -r /opt/app/requirements.txt
COPY ./read_from_cassandra.py /opt/app/
COPY ./app.py /opt/app/

ENTRYPOINT ["python", "/opt/app/app.py"]
