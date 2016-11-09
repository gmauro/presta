from __future__ import absolute_import

from . import app
from alta.bims import Bims
from presta.utils import get_conf
from celery import chain

import os
import json

from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


@app.task(name='presta.app.lims.sync_samples')
def sync_samples(samples, **kwargs):
    bika_conf = kwargs.get('conf')
    result = kwargs.get('result', '1')
    if samples and len(samples) > 0:
        pipeline = chain(
            submit_analyses.si(samples, bika_conf, result),
            verify_analyses.si(samples, bika_conf),
            publish_analyses.si(samples, bika_conf),
            publish_analysis_requests.si(samples, bika_conf),
        )
        pipeline.delay()

    else:
        logger.info('No samples to sync')

    return True


@app.task(name='presta.app.lims.sync_batches')
def sync_batches(batches, **kwargs):
    bika_conf = kwargs.get('conf')

    if batches and len(batches) > 0:
        pipeline = chain(
            close_batches.si(batches, bika_conf)
        )
        pipeline.delay()

    else:
        logger.info('No batches to sync')

    return True


@app.task(name='presta.app.lims.sync_worksheets')
def sync_worksheets(worksheets, **kwargs):
    bika_conf = kwargs.get('conf')

    if worksheets and len(worksheets) > 0:
        pipeline = chain(
            close_worksheets.si(worksheets, bika_conf)
        )
        pipeline.delay()

    else:
        logger.info('No worksheets to sync')

    return True


@app.task(name='presta.app.lims.sync_analysis_requests')
def sync_analysis_requests(samples, bika_conf):
    if samples and len(samples) > 0:
        pass

    return True


@app.task(name='presta.app.lims.submit_analyses')
def submit_analyses(samples, bika_conf, result):
    if samples and len(samples) > 0:
        bika = __init_bika(bika_conf)
        analyses = bika.get_analyses_ready_to_be_synchronized(samples=samples, action='submit')

        if isinstance(analyses, list) and len(analyses) > 0:
            logger.info('Submit {} analyses'.format(len(analyses)))
            res = bika.submit_analyses(analyses, result)
            logger.info('Submit Result {}'.format(res))
            return res.get('success')

    logger.info('Nothing to submit')

    return True


@app.task(name='presta.app.lims.verify_analyses')
def verify_analyses(samples, bika_conf):
    if samples and len(samples) > 0:
        bika = __init_bika(bika_conf)
        analyses = bika.get_analyses_ready_to_be_synchronized(samples=samples, action='verify')

        if isinstance(analyses, list) and len(analyses) > 0:
            logger.info('Verify {} analyses'.format(len(analyses)))
            res = bika.verify_analyses(analyses)
            logger.info('Verify Result: {}'.format(res))
            return res.get('success')

    logger.info('Nothing to verify')

    return True


@app.task(name='presta.app.lims.publish_analyses')
def publish_analyses(samples, bika_conf):
    if samples and len(samples) > 0:
        bika = __init_bika(bika_conf)
        analyses = bika.get_analyses_ready_to_be_synchronized(samples=samples, action='publish')

        if isinstance(analyses, list) and len(analyses) > 0:
            logger.info('Publish {} analyses'.format(len(analyses)))
            res = bika.publish_analyses(analyses)
            logger.info('Publish Result: {}'.format(res))
            return res.get('success')

    logger.info('Nothing to publish')

    return True


@app.task(name='presta.app.lims.publish_analysis_requests')
def publish_analysis_requests(samples, bika_conf):
    if samples and len(samples) > 0:
        bika = __init_bika(bika_conf)
        analysis_requests = bika.get_analysis_requests_ready_to_be_published(samples=samples)

        if isinstance(analysis_requests, list) and len(analysis_requests) > 0:
            logger.info('Publish {} analysis requests'.format(len(analysis_requests)))
            res = bika.publish_analysis_requests(analysis_requests)
            logger.info('Publish Result: {}'.format(res))
            return res.get('success')

    logger.info('Nothing to publish')

    return True


@app.task(name='presta.app.lims.close_batches')
def close_batches(batches, bika_conf):
    bika = __init_bika(bika_conf)
    batches = bika.get_batches_ready_to_be_closed(batches=batches)

    if isinstance(batches, list) and len(batches) > 0:
        logger.info('Close {} batches'.format(len(batches)))
        res = bika.close_batches(batches)
        logger.info('Close Result: {}'.format(res))
        return res.get('success')

    logger.info('Nothing to close')

    return True


@app.task(name='presta.app.lims.close_worksheets')
def close_worksheets(worksheets, bika_conf):
    bika = __init_bika(bika_conf)
    worksheets = bika.get_worksheets_ready_to_be_closed(worksheets=worksheets)

    if isinstance(worksheets,list) and len(worksheets) > 0:
        logger.info('Close {} worksheets'.format(len(worksheets)))
        res = bika.close_worksheets(worksheets)
        logger.info('Close Result: {}'.format(res))
        return res.get('success')

    logger.info('Nothing to close')

    return True


@app.task(name='presta.app.lims.search_batches_to_sync')
def search_batches_to_sync(**kwargs):
    emit_events = kwargs.get('emit_events', False)
    conf = get_conf(logger, None)
    bika_conf = conf.get_section('bika')
    bika = __init_bika(bika_conf)

    batches, samples = bika.get_batches_ready_to_be_closed(also_samples=True)

    if emit_events:
        pipeline = chain(
            sync_samples.si(samples, conf=bika_conf),
            sync_batches.si(batches, conf=bika_conf),
        )
        pipeline.delay()

    return True


@app.task(name='presta.app.lims.search_worksheets_to_sync')
def search_worksheets_to_sync(**kwargs):
    emit_events = kwargs.get('emit_events', False)
    conf = get_conf(logger, None)
    bika_conf = conf.get_section('bika')
    bika = __init_bika(bika_conf)

    worksheets, samples = bika.get_worksheets_ready_to_be_closed(also_samples=True)

    if emit_events:
        pipeline = chain(
            sync_samples.si(samples, conf=bika_conf),
            sync_worksheets.si(worksheets, conf=bika_conf),
        )
        pipeline.delay()

    return True


@app.task(name='presta.app.lims.search_samples_to_sync')
def search_samples_to_sync(**kwargs):
    conf = get_conf(logger)
    bika_conf = conf.get_section('bika')
    return True


def __init_bika(bika_conf, role='admin'):
    bika_roles = bika_conf.get('roles')
    if bika_conf and bika_roles and role in bika_roles:
        bika_role = bika_roles.get(role)
        url = bika_conf.get('url')
        user = bika_role.get('user')
        password = bika_role.get('password')
        bika = Bims(url, user, password, 'bikalims').bims
        return bika