#!/usr/bin/env python3

import asyncio
import io
import logging
import re
import sys

from datetime import datetime, timedelta, timezone
from functools import wraps
from typing import Any, Coroutine, Dict, Iterable, List, Tuple, Union

import aiohttp
import click
import numpy as np
import tifffile as tiff
import tenacity as ten

from tdmq.client import Client, Source
from tdmq.utils import timeit

logger = logging.getLogger("DPC")

TempSource = {
    "id": "dpc/meteo-mosaic/v0/temperature",
    "public": True,
    "alias": "Mosaic of temperature field acquisitions",
    "entity_category": "Radar",
    "entity_type": "MeteoRadarMosaic",
    "default_footprint": {
        "coordinates": [[
            [6.0, 47.50026321411133],
            [6.0, 35.990049599775915],
            [18.60927915042647, 35.990049599775915],
            [18.60927915042647, 47.50026321411133],
            [6.0, 47.50026321411133]]],
        "type": "Polygon"},
    "stationary": True,
    "controlledProperties": ["TEMP"],
    "shape": [576, 631],
    "geomapping": {
        "SRID": "EPSG:4326",
        "grid": {"xsize": 631, "ysize": 576},
        "ModelTransformation": [
            [0.019983009747110096, 0.0, 0.0, 6.0],
            [0.0, -0.019983009747110096, 0.0, 47.50026321411133],
            [0.0, 0.0, 0.0, 0.0],
            [0.0, 0.0, 0.0, 1.0]]
    },
    "description": {
        "brand_name": "DPC",
        "model_name": "dpc-temperature-mosaic",
        "operated_by": "Dipartimento Protezione Civile",
        "reference": "http://www.protezionecivile.gov.it/attivita-rischi/meteo-idro/attivita/previsione-prevenzione/centro-funzionale-centrale-rischio-meteo-idrogeologico/monitoraggio-sorveglianza/mappa-radar",
        "comments": None
        }
    }


PrecipitationSource = {
    "id": "dpc/meteo-mosaic/v0/precipitation",
    "public": True,
    "alias": "Mosaic of dpc meteo radars",
    "entity_category": "Radar",
    "entity_type": "MeteoRadarMosaic",
    "default_footprint": {
        "coordinates": [[
            [4.537000517753033, 47.856095810774605],
            [4.537000517753033, 35.07686201381699],
            [20.436762466677894, 35.07686201381699],
            [20.436762466677894, 47.856095810774605],
            [4.537000517753033, 47.856095810774605]]],
        "type": "Polygon"},
    "stationary": True,
    "controlledProperties": ["VMI", "SRI"],
    "shape": [1400, 1200],
    "geomapping": {
        "SRID": "EPSG:4326",
        "grid": {"xsize": 1200, "ysize": 1400},
        "ModelTransformation": [
            [0.013249801624104052, 0.0, 0.0, 4.537000517753033],
            [0.0, -0.009128024140684008, 0.0, 47.856095810774605],
            [0.0, 0.0, 0.0, 0.0],
            [0.0, 0.0, 0.0, 1.0]]
        },
    "description": {
        "brand_name": "DPC",
        "model_name": "dpc-radar-mosaic",
        "operated_by": "Dipartimento Protezione Civile",
        "reference": "http://www.protezionecivile.gov.it/attivita-rischi/meteo-idro/attivita/previsione-prevenzione/centro-funzionale-centrale-rischio-meteo-idrogeologico/monitoraggio-sorveglianza/mappa-radar",
        "comments": None
        }
    }


# The sources with which the ingested time series will be assiciated.  The keys
# of this dictionary define the run modes of this script.
Sources = {
    'temperature': {
        'def': TempSource,
        # Array configuration properties to be passed to tdmq.client.Client.register_source
        'array_properties': { },
    },
    'precipitation': {
        'def': PrecipitationSource,
        'array_properties': { },
    }
}


class RunConfig:
    def __init__(self, mode_name: str, source_id_basename_override: str=None):
        self._mode = mode_name
        self._source_cfg = Sources[self.mode]

        if source_id_basename_override:
            _, _, extension = self.source_def['id'].rpartition('/')
            self.source_def['id'] = source_id_basename_override + '/' + extension
            logger.info("Overriding default base source id. New source id %s", self.source_def['id'])

    @property
    def mode(self) -> str:
        return self._mode

    @property
    def source_def(self) -> Dict[str, Any]:
        return self._source_cfg['def']

    @property
    def source_id(self) -> str:
        return self.source_def['id']

    @property
    def source_array_properties(self) -> Dict[str, Any]:
        return self._source_cfg['array_properties']

    @property
    def products(self) -> List[str]:
        return self.source_def['controlledProperties']


def dpc_retry(fn: Coroutine) -> Coroutine:
    @wraps(fn)
    @ten.retry(retry=ten.retry_if_exception_type((aiohttp.ClientError,
                                                  aiohttp.http_exceptions.HttpProcessingError)),
               reraise=True,
               wait=ten.wait_random(1,3),
               stop=ten.stop_after_attempt(3),
               before_sleep=ten.before_sleep_log(logger, logging.DEBUG))
    async def wrapped_fn(*args, **kwargs):
        return await fn(*args, **kwargs)

    return wrapped_fn


class DPCclient:
    """
    API docs: https://dpc-radar.readthedocs.io/it/latest/api.html

    /findAvailableProducts   GET  Restituisce la lista dei prodotti disponibili (types)
    /findLastProductByType GET  Restituisce il timestamp dell’ultima acquisizione per il prodotto specificato
    /existsProduct         GET  Verifica se esiste una acquisizione del prodotto specificato al timestamp specificato
    /downloadProduct       POST Richiede il download dell’acquisizione del prodotto specificato al timestamp specificato
    /watchProductDir       GET  Utilizzato dal back-end per monitorare la presenza di una nuova acquisizione del prodotto specificato
    """

    BASE_URL = \
        'https://radar-api.protezionecivile.it/wide/product/'

    def __init__(self, conn_limit: int=4):
        self._logger = logging.getLogger("DPCclient")
        self._conn = aiohttp.TCPConnector(limit=conn_limit)
        self._session = aiohttp.ClientSession(connector=self._conn,
                                              raise_for_status=True)
        self._stats = dict.fromkeys((
            'downloaded_bytes',
            'downloaded_products',
            'missing_products',
            'total_requests'), 0)

    def _count_product(self, data: bytes) -> None:
        self._stats['downloaded_bytes'] += len(data)
        self._stats['downloaded_products'] += 1
        self._stats['total_requests'] += 1

    def _count_missing_product(self) -> None:
        self._stats['missing_products'] += 1
        self._stats['total_requests'] += 1

    def _count_generic_request(self, resp_size: int) -> None:
        self._stats['downloaded_bytes'] += resp_size
        self._stats['total_requests'] += 1


    @property
    def stats(self) -> Dict[str, int]:
        return self._stats


    async def close(self) -> None:
        await self._session.close()
        await self._conn.close()

    @dpc_retry
    async def available_products(self) -> List[str]:
        # findAvailableProducts
        # {
        #  "total" : 13,
        #  "types" : [ "AMV", "LTG", "SRT6",
        #              "IR108", "VMI", "HRD",
        #              "TEMP", "SRT1", "SRT3",
        #              "RADAR_STATUS", "SRT24",
        #              "SRI", "SRT12" ]
        # }
        url = self.BASE_URL + "findAvailableProducts"
        async with self._session.get(url) as response:
            data = await response.json()
            self._count_generic_request(len(data))
            return data['types']


    @dpc_retry
    async def latest_product(self, product_type: str) -> Dict[str, Union[str, int]]:
        # findLastproductByType
        # {
        #  "total" : 1,
        #  "lastProducts" : [ {
        #  "productType" : "AMV",
        #  "time" : 1593764400000,
        #  "period" : "PT20M"
        #  } ]
        # }
        url = self.BASE_URL + "findLastProductByType"
        async with self._session.get(url, params={'type': product_type}) as response:
            data = await response.json()
            self._count_generic_request(len(data))
            if data['total'] > 1:
                self._logger.error("Unexpected:  DPC returned %s \"last products\" for %s",
                                   data['total'], product_type)
                self._logger.error("Data: %s", data)
            if data['total'] == 0:
                return None

            latest = data['lastProducts'][0]
            latest['time'] = datetime.fromtimestamp(latest['time'] / 1000.0, tz=timezone.utc)
            latest['period'] = self._period_to_timedelta(latest['period'])
            return latest


    @dpc_retry
    async def product_exists(self, product_type: str, when: datetime) -> bool:
        # existsProduct
        # API returns a string 'true' or 'false'
        url = self.BASE_URL + "existsProduct"
        params = {'type': product_type, 'time': self._dt_to_timestamp(when)}
        self._logger.debug("Sending existsProduct with params %s", params)
        async with self._session.get(url, params=params) as response:
            data = await response.text()
            self._count_generic_request(len(data))
            return data.lower().strip() == 'true'


    @dpc_retry
    async def download_product(self, product_type: str, when: datetime) -> bytes:
        # downloadProduct
        url = self.BASE_URL + "downloadProduct"
        request_data = {'productType': product_type, 'productDate': self._dt_to_timestamp(when)}
        headers = {'Content-Type': 'application/json'}
        self._logger.debug("Sending download request with data %s", request_data)
        try:
            async with self._session.post(url, json=request_data, headers=headers, raise_for_status=False) as response:
                data = await response.read()
                if response.status >= 400:
                    # Error
                    content = await response.json()
                    if response.status < 500:
                        # The DPC service returns status code 500 when it doesn't find what you're looking for,
                        # so we handle this code specifically.
                        # the message when the product is missing looks like:
                        #   'The File for Product Type VMI is null or not exist.'
                        if 'is null' in content['gpRestExceptionMessage']['message'] or \
                           'not exist' in content['gpRestExceptionMessage']['message']:
                            self._logger.info("Product %s for timestamp %s not present", request_data['productType'], when.isoformat())
                            self._count_missing_product()
                            return None
                    # else
                    self._logger.error("Download attempt error. Status: %s; response: %s", response.status, content)
                    response.raise_for_status() # kicks the exception to the handler below
                else:
                    self._logger.debug("Download complete: %s; %s bytes", request_data, sizeof_fmt(len(data)))
                    self._count_product(data)
                    return data
        except aiohttp.client_exceptions.ClientResponseError as e:
            self._logger.error("Failed to download product: %s", request_data)
            self._logger.exception(e)
            raise


    @staticmethod
    def _dt_to_timestamp(dt: datetime) -> int:
        if dt.tzinfo is None:
            # assume utc
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)


    @staticmethod
    def _period_to_timedelta(period: str) -> timedelta:
        """
        Parse the period string reported by the DPC service and return it as a timedelta.
        """
        # These are the different periods I've seen from the service: 'PT1H' 'PT5M'
        # We deduce the format is 'PT' + quantity + units, where unit can be
        # M for minutes, H for hours (perhaps D for days)
        m = re.fullmatch(r'PT(\d+)(M|H|D)', period)
        if m is None:
            raise ValueError(f"DPC product period {period} is in an unexpected format")
        quantity, unit = m.group(1, 2)

        if unit == 'M':
            return timedelta(minutes=int(quantity))
        if unit == 'H':
            return timedelta(hours=int(quantity))
        if unit == 'D':
            return timedelta(days=int(quantity))

        raise ValueError(f"period {period} specifies an unknown unit {unit}")


def sizeof_fmt(num, suffix='B'):
    # Thanks to Sridhar Ratnakumar
    # https://stackoverflow.com/questions/1094841/get-human-readable-version-of-file-size
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


def extract_data_from_tiff(tif):
    page = tif.pages[0]
    data = page.asarray()
    return data


async def download_products_at_ts(dpc_client: DPCclient,
                                  products: Union[List, tuple], ts: datetime) -> Dict[str, Any]:
    """
    Download all products at the single specified timestamp.
    """
    retval = dict(timestamp=ts)
    downloads = await asyncio.gather(*(dpc_client.download_product(p, ts) for p in products))
    for p, data in zip(products, downloads):
        tiff_data = None
        # If the download failed in a non-exceptional way, dpc_client.download_product
        # returns None; we propagate this None up the call chain.  If the download error
        # was unexpected the function will raise an error that that will be handled
        # at a higher level.
        if data is not None:
            with io.BytesIO(data) as f:
                tiff_data = extract_data_from_tiff(tiff.TiffFile(f))
        retval[p] = tiff_data
    return retval


async def gen_product_timestamps(dpc_client: DPCclient, products: Union[List, tuple], strictly_after: datetime=None):
    """
    Generator for timestamps of downloads available from the DPC service for the
    specified products.
    If `strictly_after` is not specified, all available products are selected.
    If `strictly_after` is specified, only the ones newer than the specified
    timestamp are selected.

    Yields timestamps (i.e., datetime objects)
    """
    if not products:
        return
    dpc_product_types = await dpc_client.available_products()
    logger.debug("Products available from DPC: %s", dpc_product_types)

    unsupported_products = list(p for p in products if p not in dpc_product_types)
    if unsupported_products:
        logger.error("unsupported products: %s", unsupported_products)
        raise RuntimeError(f"The products {' and '.join(unsupported_products)} "
                           "required by source are not available from DPC")

    logger.debug("Required products: %s", products)

    latest = await asyncio.gather(*(dpc_client.latest_product(p) for p in products))
    # ensure all products have the same time and period
    all_periods = set(item['period'] for item in latest)
    if len(all_periods) > 1:
        raise ValueError(f"The selected products {' and '.join(products)} have "
                         "different periods: {', '.join(all_periods)}")
    all_timestamps = set(item['time'] for item in latest)
    if len(all_timestamps) > 1:
        raise ValueError(f"The selected products {' and '.join(products)} have "
                         "different starting timestamps: {', '.join(all_timestamps)}")
    latest_timestamp = all_timestamps.pop()
    period = all_periods.pop()
    logger.info("Requested products have latest timestamp %s and period %s",
                latest_timestamp.isoformat(), period)

    # Now compute the starting timestamp.  The DPC service makes products
    # available for one week.
    # Rather than trying to fetch the very oldest, we shift forward by one period
    # to avoid trying to download something that gets deleted between the call
    # to latest_product and the download.
    earliest_timestamp = latest_timestamp - timedelta(days=7) + period
    if strictly_after is None or strictly_after < earliest_timestamp:
        first_timestamp = earliest_timestamp
        logger.info("Selecting for download all timestamps.  First one is %s",
                    first_timestamp.isoformat())
    else:
        # we need to compute the oldest product time that is later than
        # `after` time.
        num_products = (latest_timestamp - strictly_after) // period
        first_timestamp = latest_timestamp - num_products * period
        if first_timestamp <= strictly_after:
            # strictly_after is on a product timestamp.  Advance to the next one
            first_timestamp += period
        logger.info("Computed timestamps to download starting after latest recorded activity")
        logger.info("Time bound: %s; first download timestamp: %s",
                    strictly_after.isoformat(), first_timestamp.isoformat())

    # Finally, create the download jobs for each product and timestamp
    ts = first_timestamp
    while ts <= latest_timestamp:
        yield ts
        ts += period


def restructure_download_results(destination: Source, downloads: Iterable[Dict[str, Any]]) -> Tuple[List, List]:
    """
    Each invocation of download_products_at_ts in the batch will return a
    dict with a key 'timestamp' plus one key for each product.
    If a product was not available for download, its value will be None.
    E.g.,

        { 'timestamp': datetime(xxxx),
           'TEMP': np.array,
           'VMI': None
        }

    This function restructures this data for writing to the TDM-polystore client:

        times = [ n timestamps ]
        products = {
            'TEMP': [ n arrays ],
            'VMI': [ n arrays ]
        }

    The times and products arrays will always have the same length.
    If a timestamp dictionary contained no data, it will be skipped, so the output
    arrays may be smaller than the input `downloads` array.

    If only one of a number of products for a timestamp is None, then it will be
    replaced by a NaN-filled numpy array.
    """
    times = []
    products = dict((p, []) for p in destination.controlled_properties)
    for d in downloads:
        # If all products are None, we skip this timestamp altogether.
        # Otherwise:
        if any(d[p] is not None for p in destination.controlled_properties):
            times.append(d['timestamp'])
            for p in destination.controlled_properties:
                if d[p] is not None:
                    value = d[p]
                else:
                    value = np.empty(destination.shape)
                    value.fill(np.nan)
                products[p].append(value)
    return times, products


async def write_to_destination(destination: Source, downloads_ary: Iterable[Dict[str, Any]], task_name: str) -> None:
    times, products = restructure_download_results(destination, downloads_ary)

    logger.debug("%s: Executing Source.insert_many in executor.  "
                 "Inserting %s timestamps and %s products",
                 task_name, len(times), len(products))
    # Source.ingest_many is not a coroutine.  We execute it through
    # `run_in_executor` to avoid blocking the program.
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, destination.ingest_many, times, products)

    # Report some info to log
    logger.info("Ingested batch with %s items", len(times))
    if logger.isEnabledFor(logging.INFO):
        logger.info("Ingested %s for timestamps:\n\t%s",
                    ' and '.join(products.keys()),
                    '\n\t'.join(t.isoformat() for t in times))


async def ingest_products(destination: Source, strictly_after: datetime, batch_size: int=20, max_batches: int=3) -> None:
    outstanding_batch_semaphore = asyncio.Semaphore(max_batches)
    outstanding_batches = 0

    async def finalize_batch(batch: Iterable[asyncio.Task], previous_finalizer: asyncio.Task) -> None:
        nonlocal outstanding_batches
        task_name = asyncio.current_task().get_name()
        logger.debug("%s: waiting on semaphore. ", task_name)
        async with outstanding_batch_semaphore:
            outstanding_batches += 1
            try:
                logger.debug("%s: Semaphore acquired.  Outstanding batches %s",
                             task_name, outstanding_batches)
                logger.debug("awaiting on %s downloads", len(batch))
                # Each invocation of download_products_at_ts in the batch will return a
                # dict with a key 'timestamp' plus one key for each product.  If a product
                # was not available for download, its value will be None.
                try:
                    downloads_ary = await asyncio.gather(*batch)
                except Exception as e:
                    logging.error("Exception from download tasks")
                    logging.exception(e)
                    cancel_all(batch)
                    # Await cancelled tasks or we'll get warnings about coroutines never having been awaited
                    await asyncio.gather(*batch, return_exceptions=True)
                    raise
                logger.debug("%s: downloads completed", task_name)
                logger.info("Downloaded batch of %s images complete", len(batch))

                if previous_finalizer:
                    # We await on the previous finalizer to ensure our writes happen in order
                    logger.debug("%s awaiting on finalizing of previous batch %s",
                                 task_name,
                                 previous_finalizer.get_name())
                    await previous_finalizer
                    logger.debug("%s: Finished awaiting.  Ingesting batch", task_name)

                await write_to_destination(destination, downloads_ary, task_name)
            finally:
                outstanding_batches -= 1
        logger.debug("%s: Semaphore released.  Outstanding batches %s",
                     task_name, outstanding_batches)

    products = destination.controlled_properties
    dpc_client = DPCclient()
    with timeit(logger.info, "Total ingestion time: %0.5f seconds"):
        try:
            logger.debug("Created DPCclient")
            batch = list()
            batch_counter = 1
            tasks = list()
            async for timestamp in gen_product_timestamps(dpc_client, products, strictly_after):
                logger.debug("appending timestamp %s to batch", timestamp.isoformat())
                batch.append(
                    asyncio.create_task(download_products_at_ts(dpc_client, products, timestamp)))
                if len(batch) >= batch_size:
                    logger.debug("Created batch of size %s. Passing to finalize", len(batch))
                    # We chain the calls to finalize_batch, making each call await on the previous
                    # one.  This enforces a global time order in the products are ingested.
                    t = asyncio.create_task(finalize_batch(batch, tasks[-1] if tasks else None),
                                            name=f"finalize_batch_{batch_counter}")
                    tasks.append(t)
                    batch = list()
                    batch_counter += 1
            if len(batch) > 0:
                logger.debug("Finalizing last batch; size %s", len(batch))
                t = asyncio.create_task(finalize_batch(batch, tasks[-1] if tasks else None),
                                        name=f"finalize_batch_{batch_counter}")
                tasks.append(t)

            if tasks:
                logger.info("Waiting for download and ingestion tasks to finish")
                logger.debug("%s tasks are not yet done", sum(1 for t in asyncio.all_tasks() if not t.done()))
                try:
                    await asyncio.gather(*tasks)
                except Exception as e:
                    logger.error("Caught exception at the top level!")
                    logger.exception(e)
                    logger.error("Cancelling outstanding ingestion tasks")
                    cancel_all(tasks)
                    cancel_all(batch)
                    # await cancelled tasks
                    await asyncio.gather(*tasks, return_exceptions=True)
                    await asyncio.gather(*batch, return_exceptions=True)
                    raise
        finally:
            await dpc_client.close()
    print_dpc_client_stats(dpc_client.stats)


def cancel_all(tasks: Iterable[asyncio.Task]) -> None:
    for t in tasks:
        logger.debug("Cancelling task %s", t)
        t.cancel()


def print_dpc_client_stats(stats: dict) -> None:
    logger.info("Closed DPCclient.  Download stats:")
    logger.info("\tdownloaded volume: %s", sizeof_fmt(stats['downloaded_bytes']))
    logger.info("\tdownloaded products: %s", stats['downloaded_products'])
    logger.info("\tmissing products: %s", stats['missing_products'])
    logger.info("\ttotal_requests: %s", stats['total_requests'])


def fetch_tdmq_source(client: Client, run_conf: RunConfig) -> Source:
    sources = client.find_sources(args={'id': run_conf.source_id})
    if len(sources) > 1:
        raise RuntimeError(f"Bug?  Got {len(sources)} sources from tdmq query "
                           "for source id {run_conf.source_id}. Aborting")

    if len(sources) == 1:
        source = sources[0]
        logger.info("Found existing source with tdmq_id %s", source.tdmq_id)
    else:
        source = None

    return source


def register_tdmq_source(client: Client, run_conf: RunConfig) -> Source:
    source = client.register_source(run_conf.source_def, properties=run_conf.source_array_properties)
    logger.info("New source registered with tdmq_id %s", source.tdmq_id)

    return source


def configure_logging(log_level: str) -> None:
    level = getattr(logging, log_level)
    log_format = '[%(asctime)s] %(name)s %(levelname)s:  %(message)s'
    logging.basicConfig(level=level, format=log_format)


@click.group()
@click.argument('mode', envvar='DPC_MODE', type=click.Choice(Sources.keys()))
@click.argument('tdmq-endpoint', envvar='TDMQ_URL')
@click.argument('tdmq-token', envvar='TDMQ_AUTH_TOKEN')
@click.option("--log-level", envvar="DPC_LOG_LEVEL",
              type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']),
              default='INFO')
@click.option('--override-source-id', envvar='DPC_SOURCE_OVERRIDE', default=None,
              help="Override the default source id basename to which data will be written")
@click.pass_context
def dpc(ctx, mode: str, tdmq_endpoint: str, tdmq_token: str, override_source_id: str, log_level) -> None:
    configure_logging(log_level)
    logger.debug("main(%s, %s, %s, %s, %s)",
                 mode, tdmq_endpoint, tdmq_token, override_source_id, log_level)

    if not tdmq_token:
        raise ValueError("TDMQ token not provided!")

    run_conf = RunConfig(mode, override_source_id)
    logger.info("DPC ingestor running in %s mode.  Source id: %s",
                run_conf.mode, run_conf.source_id)

    logger.info("Pointing to tdmq service %s.  Auth token provided", tdmq_endpoint)
    tdmq_client = Client(tdmq_endpoint, auth_token=tdmq_token)

    # make the client available to subcommands
    ctx.ensure_object(dict)
    ctx.obj['run_conf'] = run_conf
    ctx.obj['tdmq_client'] = tdmq_client
    logger.debug("dpc finished.  Continuing in subcommand (if any)")


############################ High-level overview ###############################
# `ingest` is the entry point of the download and ingest process.
# This functions sets up the the work, then calls the ingest_products
# coroutine to actually do the work.
#
# ingest_products:
#   * Uses the gen_product_timestamps() generator to create the timestamps of the
#     products to be downloaded, in order.
#   * Iterates over these timestamps and, for each one, creates a download task
#     implemented by download_products_at_ts.
#   * These download tasks get put in batches.
#   * Each batch is passed to a call to finalize_batch, which awaits them and then
#     ingests their output.
#   * To enforce write order, each batch awaits on the previous one.
#
# Task dependency summary
#
# ingest_products -> last finalize_batch -> second-last finalize_batch -> ... -> first finalize_batch
#
# each finalize_batch -> download_products_at_ts -> dpc_client.download_product
#                                                -> dpc_client.download_product
#                     -> download_products_at_ts -> dpc_client.download_product
#                                                -> dpc_client.download_product
# and so on.
################################################################################
@dpc.command()
@click.option('--batch-size', default=20, envvar='DPC_BATCH_SIZE', type=int, show_default=True,
              help="Size of batch of products to be concurrently downloaded and then written.")
@click.option('--max-batches', default=3, envvar='DPC_MAX_BATCHES', type=int, show_default=True,
              help="Max number of downloaded batches to queue up in memory for writing to the array")
@click.option('--strictly-after', help="Force the start timestamp for the downloaded products to be downloaded. ISO format.")
@click.option('--consolidate/--no-consolidate', envvar='DPC_CONSOLIDATE', default=True, show_default=True)
@click.pass_context
def ingest(click_ctx, batch_size: int, max_batches: int, strictly_after: str=None, consolidate: bool=True) -> None:
    """
    Ingest data from the Radar DPC meteorological radar mosaic service into the
    TDM polystore.
    """
    run_conf = click_ctx.obj['run_conf']
    tdmq_client = click_ctx.obj['tdmq_client']

    if strictly_after:
        # parse user input.  Set `last_time` variable
        strictly_after = datetime.fromisoformat(strictly_after)
        if strictly_after.tzinfo is None:
            strictly_after = strictly_after.replace(tzinfo=timezone.utc)
            logger.info("--strictly-after: Time zone not specified.  Assuming UTC")
        logger.warning("Clipping data download to start after timestamp %s", strictly_after.isoformat())

    logger.info("Ingesting %s products.", ' and '.join(run_conf.products))

    # Fetch or register the TDMq Source.  The source will be different depending
    # on whether we're running in `temperature` or `precipitation` mode.
    source = fetch_tdmq_source(tdmq_client, run_conf)
    if not source:
        source = register_tdmq_source(tdmq_client, run_conf)
    elif strictly_after:
        click_ctx.fail("Can't specify --strictly-after on an existing source!")

    ts = source.get_latest_activity()
    if len(ts) > 0:
        last_time = ts.time[0]
        logger.info("Last previous activity reported by source: %s.", last_time.isoformat())
    else:
        last_time = None
        logger.info("No previous activity reported by source.")

    if strictly_after and (not last_time or strictly_after > last_time):
        # There is previous activity that is previous to strictly_after.  We override
        # The ingestion start timestamp with the one provided by strictly_after
        last_time = strictly_after
        logger.info("Bounding ingestion to %s as specified by --strictly-after option", last_time.isoformat())

    logger.info("Ingesting data starting from %s", last_time.isoformat() if last_time else "infinity")

    asyncio.run(ingest_products(destination=source, strictly_after=last_time,
                                batch_size=batch_size, max_batches=max_batches))

    logger.info("Finished ingesting.")
    if consolidate:
        logger.info("Consolidating array...")
        source.consolidate()

    logger.info("Operation complete")


@dpc.command()
@click.pass_obj
def register_source(click_obj) -> None:
    """
    Register TDMq Source, if it doesn't exist.
    """
    run_conf = click_obj['run_conf']
    tdmq_client = click_obj['tdmq_client']

    # Fetch or register the TDMq Source.  The source will be different depending
    # on whether we're running in `temperature` or `precipitation` mode.
    source = fetch_tdmq_source(tdmq_client, run_conf)
    if not source:
        register_tdmq_source(tdmq_client, run_conf)


@dpc.command()
@click.pass_obj
def delete_source(click_obj) -> None:
    """
    Deregister the specified source
    """
    click.confirm("Are you sure you want to delete the source and all associated data?",
                  abort=True)
    run_conf = click_obj['run_conf']
    tdmq_client = click_obj['tdmq_client']

    sources = tdmq_client.find_sources(args={'id': run_conf.source_id})
    if len(sources) > 1:
        raise RuntimeError(f"Bug?  Got {len(sources)} sources from tdmq query "
                           "for source id {run_conf.source_id}. Aborting")

    if not sources:
        logger.error("Selected source %s not found in tdm polystore.  Nothing to delete", run_conf.source_id)
        sys.exit(2)
    else:
        source = sources[0]
        tdmq_client.deregister_source(source)

if __name__ == "__main__":
    dpc()
