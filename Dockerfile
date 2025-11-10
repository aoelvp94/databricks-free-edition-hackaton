FROM python:3.11-slim

ARG UV_VERSION=0.4.20

ENV PATH="/root/.local/bin:/root/.databricks/bin:${PATH}"

RUN apt-get update \
    && apt-get install --no-install-recommends -y curl ca-certificates build-essential git \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fsSL https://astral.sh/uv/${UV_VERSION}/install.sh | sh \
    && curl -fsSL https://raw.githubusercontent.com/databricks/cli/main/install.sh | bash \
    && ln -s /root/.databricks/bin/databricks /usr/local/bin/databricks

WORKDIR /workspace

COPY requirements-dev.txt ./
RUN if [ -f requirements-dev.txt ]; then uv pip install --system -r requirements-dev.txt; fi

COPY . .

CMD ["bash"]
