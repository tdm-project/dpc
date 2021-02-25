import json
import logging
import math
import os
import tempfile
from datetime import datetime, timedelta

import dateparser
import numpy as np
import requests
import tifffile as tiff
from clize import parameters, run
from tdmq.client import Client

logger = None
dpc_url = \
    'http://www.protezionecivile.gov.it/wide-api/wide/product/downloadProduct'

mask_value = -9999.0


def extract_data(tif):
    page = tif.pages[0]
    data = page.asarray()
    # zeroout nan, force mask value to -9999.0
    data[np.isnan(data)] = mask_value
    return page.geotiff_tags, data


def gather_data(t, field):
    # FIXME either datetime or np.datetime64[*]
    t = t if isinstance(t, datetime) else t.tolist()
    logger.debug('time %s', t)
    product_date = math.floor(t.timestamp() * 1000)
    payload = {'productType': field, 'productDate': product_date}
    logger.debug('dpc_url %s', dpc_url)
    logger.debug('payload %s', payload)
    r = requests.post(dpc_url, json=payload)
    if r.status_code != 200:
        raise ValueError(f"Bad return code: {r.status_code}")
    # FIXME find a cleaner way to handle the returned tif file.
    handle, fpath = tempfile.mkstemp()
    with open(fpath, 'wb') as f:
        f.write(r.content)
    tif = tiff.TiffFile(fpath)
    os.unlink(fpath)
    return extract_data(tif)


def fetch_dpc_data(source, t, field):
    try:
        g, d = gather_data(t, field)
    except ValueError as e:
        print(f'adding masked data for {t} {field} because of {e}.')
        d = np.zeros(source.shape)
        d.fill(mask_value)
    return d


def setup_logging(level_name):
    global logger
    level = getattr(logging, level_name)

    logging.basicConfig(level=level)
    logger = logging.getLogger('setup_source')
    logger.info('Logging is active (level %s).', level_name)


def load_desc(opt):
    source_dir = 'sources'
    if opt == 'radar':
        fname = os.path.join(source_dir, 'dpc-meteoradar.json')
    elif opt == 'temperature':
        fname = os.path.join(source_dir, 'dpc-temperature.json')
    else:
        assert False
    full_path = os.path.join(os.path.dirname(__file__), fname)
    logging.debug('loading source description from %s.', full_path)
    with open(full_path) as f:
        return json.load(f)


def create_timebase(when, dt_back):
    # We align on the hourly edge
    return (datetime(when.year, when.month, when.day, when.hour) - dt_back)


def main(source: parameters.one_of('radar', 'temperature'),
         *,
         tdmq_url: ('u'),
         start='6 days ago',
         end='now',
         log_level: ('v',
                     parameters.one_of("DEBUG", "INFO", "WARN", "ERROR",
                                       "CRITICAL")) = 'INFO',
         auth_token: str = None):
    setup_logging(log_level)
    logger.info(
        'ingesting with params source: %s, tdmq_url: %s, start: %s, end: %s',
        source, tdmq_url, start, end)
    desc = load_desc(source)

    dt = timedelta(seconds=desc['description']['acquisition_period'])

    c = Client(tdmq_url, auth_token)
    srcs = c.find_sources({'id': desc['id']})
    if len(srcs) > 0:
        assert len(srcs) == 1
        s = srcs[0]
        logger.info(f"Using source {s.tdmq_id} for {s.id}.")
    else:
        s = c.register_source(desc)
        logger.info(f"Created source {s.tdmq_id} for {s.id}.")

    start = dateparser.parse(start)
    end = dateparser.parse(end)
    try:
        ts = s.timeseries(after=start, before=end)
    except Exception as ex:  # FIXME too general
        ts = []
    # The DPC source keeps data available for only one week
    time_base = ts.time[0] if len(ts) > 0 else create_timebase(
        end, end - start)
    times = np.arange(time_base, end, dt)
    filled = times[np.searchsorted(times, ts.time)]\
        if len(ts) > 0 else []
    to_be_filled = set(times) - set(filled)

    with s.array_context('w'):
        for t in to_be_filled:
            t = t if isinstance(t, datetime) else t.tolist()
            t = t.replace(minute=0, second=0)
            data = {}
            for f in s.controlled_properties:
                data[f] = fetch_dpc_data(s, t, f)
            slot = int((t - time_base).total_seconds() // dt.total_seconds())
            logger.info(f"Ingesting data at time {t}, slot {slot}.")
            try:
                s.ingest(t, data, slot)
            except Exception as e:
                logger.error(
                    'an error occurred when ingesting time %s at slot %s.'
                    'Exception: %s', t, slot, e)
    logger.info(f"Done ingesting.")


if __name__ == "__main__":
    run(main)
