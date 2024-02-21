FROM python:3.11-slim as base

COPY streaming/data_producer/requirements.txt ./requirements.txt

RUN apt-get update &&\
    apt-get -y upgrade &&\
    apt-get -y install --no-install-recommends git &&\
    apt-get -y install --no-install-recommends gcc &&\
    pip install --upgrade pip

RUN pip install -r requirements.txt

COPY streaming/data_producer/main.py ./main.py

CMD ["python3", "main.py"]