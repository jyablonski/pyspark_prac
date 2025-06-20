FROM python:3.13-slim AS base

ENV DEBIAN_FRONTEND=noninteractive \
    PATH="/.venv/bin:/root/.cargo/bin:$PATH" \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never \
    UV_PYTHON=3.13

# install OS-level deps once in base (so we don't repeat in each stage)
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*
FROM base AS python-deps

COPY pyproject.toml uv.lock ./

ARG INSTALL_DEV_DEPENDENCIES=true

RUN pip install uv==0.7.13 && \
    if [ "$INSTALL_DEV_DEPENDENCIES" = "true" ]; then \
        uv sync --frozen; \
    else \
        uv sync --frozen --no-dev; \
    fi


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