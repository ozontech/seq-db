download-dataset:
	cd ./dataset/logs && bash download.sh

prepare-dataset: download-dataset
	cd ./dataset && rm -rdf docs-*.log && python3 distribute.py ./logs -n 32

docker-run-seqdb:
	docker compose -f docker-compose.yml -f docker-compose-seqdb.yml up

docker-run-elastic:
	docker compose -f docker-compose.yml -f docker-compose-elastic.yml up

docker-run-vlogs:
	docker compose -f docker-compose.yml -f docker-compose-vlogs.yml up

docker-run-seqdb-d:
	docker compose -f docker-compose.yml -f docker-compose-seqdb.yml up -d

docker-run-elastic-d:
	docker compose -f docker-compose.yml -f docker-compose-elastic.yml up -d

docker-run-vlogs-d:
	docker compose -f docker-compose.yml -f docker-compose-vlogs.yml up -d

docker-cleanup:
	docker compose \
		-f docker-compose-seqdb.yml \
		-f docker-compose-elastic.yml \
		-f docker-compose-vlogs.yml \
		down -v --remove-orphans

docker-cleanup-all:
	docker compose -f docker-compose.yml \
		-f docker-compose-seqdb.yml \
		-f docker-compose-elastic.yml \
		-f docker-compose-vlogs.yml \
		down -v --remove-orphans
