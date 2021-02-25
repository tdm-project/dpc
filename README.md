# dpc
This repo contains the code for ingesting data from Protezione Civile (temperature and radar map) to TDM.

##Docker Build
Run:
```
docker build -t tdmproject/dpc_ingestor -f docker/Dockerfile .
```

## Example
Let's ingest radar data from Protezione Civile. First of all, start up the services:

```
  docker-compose -f docker/docker-compose.yml up -d
```

Then create the bucket:
  
```
  docker run --rm --network docker_default --entrypoint s3cmd d3fk/s3cmd  --no-ssl --host=minio:9000 --host-bucket= --access_key=tdm-user --secret_key=tdm-user-s3 mb s3://tdm-public
```

then retrieve the auth token:
```
  AUTH_TOKEN=$(docker-compose -f docker/docker-compose.yml logs web | awk 'match($0, /token is (\w+)/,_match) {a=_match[1]} END {print a}')
```

Finally, run the ingestor:
```
  docker run --rm --network docker_default tdmproject/dpc_ingestor -u http://web:8000/api/v0.0 --auth-token ${AUTH_TOKEN} temperature
```

