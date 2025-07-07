download-dataset:
	cd ./dataset/logs && bash download.sh

prepare-dataset: download-dataset
	cd ./dataset && rm -rdf docs-*.json && python3 distribute.py ./logs -n 10

docker-run-seq-db:
	docker compose -f docker-compose.yml -f docker-compose-seq-db.yml up -d

docker-run-elastic:
	docker compose -f docker-compose.yml -f docker-compose-elastic.yml up -d

docker-run-seq-db-vs-elastic:
	docker compose -f docker-compose.yml -f docker-compose-elastic.yml -f docker-compose-seq-db.yml up -d

docker-run-seq-db-vs-vlogs:
	docker compose -f docker-compose.yml -f docker-compose-vlogs.yml -f docker-compose-seq-db.yml up -d

docker-cleanup:
	docker compose \
		-f docker-compose-seq-db.yml \
		-f docker-compose-elastic.yml \
		down -v --remove-orphans

docker-cleanup-all:
	docker compose -f docker-compose.yml \
		-f docker-compose-seqdb.yml \
		-f docker-compose-elastic.yml \
		down -v --remove-orphans
