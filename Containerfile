# protocol's images (Kamino item 70) - one recipe, three images:
#
#   podman build -t protocol:latest .                                           # the API, maintenance worker
#   podman build --build-arg BASE=localhost/marker-service:latest -t protocol-marker:latest .
#   podman build --build-arg BASE=localhost/markitdown-service:latest -t protocol-markitdown:latest .
#
# A converter's worker has to run where its engine imports (converters.py):
# protocol-droid's images carry marker (torch, llama-server, 6.8 GB) and
# markitdown, each with the module protocol calls in /app. So protocol goes on
# top of them rather than the engines into an image of protocol's. The API and
# the maintenance worker convert nothing and start from plain Python.
#
# The command is python -m protocol.deploy (api | worker <name> | check);
# every setting is an environment variable (README, Configuration).
ARG BASE=docker.io/library/python:3.12-slim
FROM ${BASE}

# protocol-droid's images end as uid 1000 (app); installing needs root.
USER root

# The same pins as protocol-droid's for what both use (rq, redis, fastapi), so
# on its images this adds the backends' clients and changes nothing else.
COPY requirements.txt /opt/protocol/requirements.txt
RUN pip install --no-cache-dir -r /opt/protocol/requirements.txt

COPY protocol /opt/protocol/protocol

# /app first: the engine modules (tasks, markitdown_tasks) are there in
# protocol-droid's images, and absent - harmlessly - from plain Python.
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app:/opt/protocol \
    HOME=/home/app

# protocol-droid's images have app already; plain Python does not.
RUN id app >/dev/null 2>&1 \
    || useradd --system --uid 1000 --create-home --shell /usr/sbin/nologin app
USER app

ENTRYPOINT ["python", "-m", "protocol.deploy"]
CMD ["check"]
