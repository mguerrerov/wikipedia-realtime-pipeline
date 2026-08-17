# Imagen del productor. Misma serie de Python que en local (ver docs/versiones.md).
FROM python:3.10.21-slim-bookworm

WORKDIR /app

# Las dependencias primero: cambian mucho menos que el codigo, asi la capa
# se reaprovecha entre reconstrucciones.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

# Sin buffer, para que los logs aparezcan en `docker compose logs` al momento.
ENV PYTHONUNBUFFERED=1

# Usuario sin privilegios: el productor no necesita ser root.
RUN useradd --create-home --uid 10001 productor
USER productor

CMD ["python", "-m", "src.productor"]
