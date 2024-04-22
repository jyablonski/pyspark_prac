FROM python:3.11-slim AS base

ENV POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_NO_INTERACTION=1

FROM base AS python-deps

COPY pyproject.toml poetry.lock ./

RUN pip install poetry
RUN poetry install

FROM base AS runtime

COPY --from=python-deps ./.venv /.venv
ENV PATH="/.venv/bin:$PATH"

# this can prolly be multi staged but fuqqit and fuck java too while we at it
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
            openjdk-17-jre-headless \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

RUN useradd --create-home appuser
WORKDIR /home/appuser
USER appuser

COPY ./src ./src
COPY ./tests ./tests

# CMD ["python3", "-m", "pytest", "-v"]