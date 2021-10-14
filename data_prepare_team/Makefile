setup:
	pipenv sync

build:
	docker build -f cluster-base.Dockerfile -t cluster-base . && \
	docker build -f spark-base.Dockerfile -t spark-base . && \
	pipenv lock --requirements > spark_master_requirements && \
	docker build -f spark-master.Dockerfile -t spark-master . && \
	docker build -f spark-worker.Dockerfile -t spark-worker .

cluster-up:
	docker-compose up

run:
	rm -R workspace/invalid workspace/valid workspace/product_catalog.csv ; \
  curl https://backmarket-data-jobs.s3-eu-west-1.amazonaws.com/data/product_catalog.csv -o workspace/product_catalog.csv ; \
	docker exec -it spark-master bin/spark-submit /opt/transformater/transform.py \
		--source_path=/opt/workspace/product_catalog.csv \
		--target_path=/opt/workspace

run-from-aws:
	rm -R workspace/invalid workspace/valid workspace/product_catalog.csv ; \
	docker exec -it spark-master bin/spark-submit /opt/transformater/transform.py \
		--source_path=s3a://backmarket-data-jobs/data/product_catalog.csv \
		--target_path=/opt/workspace \
		--aws_aki=AKIXXXXXXXXXXX \
		--aws_sak=secretkeysecretkeysecretkeysecretke

test:
	pipenv run pytest

pyspark:
	docker exec -it  spark-master bin/pyspark --name backmarket-test

spark-master-bash:
	docker exec -it  spark-master /bin/bash
