FROM python:3.11-slim as builder

RUN apt-get update && apt-get -y upgrade && \
    apt-get -y install --no-install-recommends git gcc

COPY streaming/data_producer/requirements.txt /app/requirements.txt
WORKDIR /app
RUN python3 -m venv .venv && \
    . .venv/bin/activate && \
    pip install --upgrade pip && \
    pip install -r requirements.txt

FROM python:3.11-slim as final

COPY --from=builder /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"

WORKDIR /app
COPY streaming/data_producer/main.py ./main.py

CMD ["python3", "main.py"]