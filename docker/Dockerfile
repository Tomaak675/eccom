# Agent image. Built natively on each target host by the SSH deployment
# (source is synced there — no repo clone needed) and locally by
# docker-compose. Layer order matters: the dependency layer is cached until
# pyproject.toml changes, so code-only changes rebuild in seconds.
FROM python:3.12-slim

WORKDIR /app

# iperf3 for the link-capacity probe (instrumentation.link_capacity); the
# orchestrator runs it from this image with --network host.
RUN apt-get update \
 && apt-get install -y --no-install-recommends iperf3 \
 && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
RUN python -c "import tomllib, pathlib; deps = tomllib.load(open('pyproject.toml','rb'))['project']['dependencies']; pathlib.Path('requirements.txt').write_text('\n'.join(deps))" \
 && pip install --no-cache-dir -r requirements.txt

COPY eccom ./eccom
RUN pip install --no-cache-dir --no-deps .

# Deployments pass provenance through the environment:
#   ECCOM_GIT_COMMIT, ECCOM_IMAGE_DIGEST
ENTRYPOINT ["eccom"]
