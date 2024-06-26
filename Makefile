DC = docker compose
APP = -f ./docker-compose/app.yaml
MAIN_CONTAINER = airflow

.PHONY: up
up:
	${DC} ${APP} up -d --build
	make logs

.PHONY: logs
logs:
	${DC} ${APP} logs ${MAIN_CONTAINER} -f

.PHONY: down
down:
	${DC} ${APP} down

.PHONY: restart
restart:
	make down
	make up
