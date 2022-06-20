FROM python:3.8-slim-buster AS py3
FROM openjdk:8-slim-buster

COPY --from=py3 / /

RUN pip --no-cache-dir install pyspark==3.2.1
RUN pip --no-cache-dir install cython==0.29.28 numpy==1.22.2 pytest==7.0.1 pandas==1.4.1
ENV PYSPARK_PYTHON=python3

WORKDIR /app

COPY test/ ./test/
# this image is 503mb (compressed) in ECR