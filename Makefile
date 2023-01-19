unit:
	pip install -e .['local, test'] && python tests/entrypoint.py tests/unit --cov=covid_prophet_dbx

integration:
	dbx execute covid-prophet-dbx-sample-tests --cluster-id 0812-165614-tibia842

ingest:
	dbx execute covid-prophet-dbx-ingest --task ingest --cluster-id 0812-165614-tibia842

prophet:
	dbx execute covid-prophet-dbx-prophet --task prophet --cluster-id 0812-165614-tibia842

