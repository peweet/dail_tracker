FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy PYTHONUNBUFFERED=1

COPY requirements.txt ./requirements.txt
RUN uv pip install --system --no-cache -r requirements.txt

COPY app.py ./app.py
COPY snapshot.json ./snapshot.json

EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s \
  CMD python -c "import urllib.request,sys; r=urllib.request.urlopen('http://127.0.0.1:8080/v1/health', timeout=5); sys.exit(0 if r.status==200 else 1)"

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
