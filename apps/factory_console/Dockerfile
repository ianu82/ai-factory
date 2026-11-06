FROM python@sha256:4f26056fb6ace322265b99c8441e7adae065a56720432e4e722ca921217a68e3
RUN useradd --uid 10001 --create-home console && mkdir /data && chown console:console /data
WORKDIR /app
COPY . /app/apps/factory_console
ENV PYTHONUNBUFFERED=1 PORT=8080 FACTORY_CONSOLE_DB=/data/console.db
EXPOSE 8080
CMD ["python", "-m", "apps.factory_console.boot"]
