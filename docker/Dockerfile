FROM python:3.9-slim-buster AS py3
FROM openjdk:17-slim-buster

ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONFAULTHANDLER 1

COPY --from=py3 / /
COPY Pipfile /
COPY Pipfile.lock /

RUN pip install pipenv
RUN PIPENV_VENV_IN_PROJECT=1 pipenv install --dev

ENV PATH="/.venv/bin:$PATH"
ENV PYSPARK_PYTHON=python3

WORKDIR /app

COPY test/ ./test/

# multi stage build wasn't workin & pissing me tf off so this is good enough for now