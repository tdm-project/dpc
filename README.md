# dpc

This repo contains the code for ingesting precipitation and temperature data
from [Protezione Civile meteorological data
service](http://www.protezionecivile.gov.it/attivita-rischi/meteo-idro/attivita/previsione-prevenzione/centro-funzionale-centrale-rischio-meteo-idrogeologico/monitoraggio-sorveglianza/mappa-radar)
to the TDM polystore.

## Docker Build

Pre-built images are [provided on Docker
Hub](https://hub.docker.com/repository/docker/tdmproject/dpc_ingestor/).

To build your own image run:
```
docker build -t tdmproject/dpc_ingestor -f docker/Dockerfile .
```

## Example

You can run the polystore locally and ingest some data. First of all, start up
the services:

```
wget -O docker/docker-compose.base.yml https://raw.githubusercontent.com/tdm-project/tdm-polystore/develop/docker/docker-compose.base.yml
wget -O docker/settings.conf https://raw.githubusercontent.com/tdm-project/tdm-polystore/develop/docker/settings.conf
docker-compose -f docker/docker-compose.base.yml up -d
```

Now run the ingestor. Here we'll grab temperature data from the DPC for last 12
hours.

```
docker run --rm --network docker_tdmq tdmproject/dpc_ingestor \
    temperature \
    "http://web:8000/api/v0.0/" "$(sed -n -e '/TDMQ_AUTH_TOKEN/s/.*=//p' docker/settings.conf)"  ingest --strictly-after $(date -d '12 hour ago' --iso-8601=minutes --utc)
```

