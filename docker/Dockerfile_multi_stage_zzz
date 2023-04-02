FROM python:3.9-slim as base

ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONFAULTHANDLER 1

FROM base AS python-deps

RUN apt-get update && apt-get -y upgrade && apt-get -y install --no-install-recommends gcc
RUN pip install --upgrade pip && pip install pipenv

COPY Pipfile /
COPY Pipfile.lock /
RUN PIPENV_VENV_IN_PROJECT=1 pipenv install --dev

FROM openjdk:17-slim-buster as runtime
# FROM base AS runtime

COPY --from=base / /
COPY --from=python-deps /.venv /.venv
ENV PATH="/.venv/bin:$PATH"
ENV PYSPARK_PYTHON=python3

WORKDIR /app

COPY test/ ./test/
# this image is 503mb (compressed) in ECR