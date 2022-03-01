FROM python:3.8-slim-buster AS py3
FROM openjdk:8-slim-buster

COPY --from=py3 / /

RUN pip --no-cache-dir install pyspark==3.2.1
RUN pip3 install cython==0.29.21 numpy==1.18.5 pytest==7.0.1 pandas==1.0.5
ENV PYSPARK_PYTHON=python3

WORKDIR /app

COPY test/ ./test/