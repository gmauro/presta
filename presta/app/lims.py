from __future__ import absolute_import

from . import app
from alta.bims import Bims
from presta.utils import get_conf
from presta.app.router import dispatch_event
from celery import chain

import os

from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)

DENIED_ANALYSIS = ['full-analysis']


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

    return True


@app.task(name='presta.app.lims.sync_analysis_requests')
def sync_analysis_requests(samples, bika_conf):
    if samples and len(samples) > 0:
        pass

    return True


@app.task(name='presta.app.lims.submit_analyses')
def submit_analyses(samples, bika_conf, result):
    if samples and len(samples) > 0:
        paths = __get_analysis_paths(samples=samples, review_state='sample_received', bika_conf=bika_conf)

        if len(paths) > 0:
            logger.info('Submit {} analyses'.format(len(paths)))
            bika = __init_bika(bika_conf=bika_conf, role='analyst')
            res = bika.client.submit_analyses(paths, result)
            logger.info('Submit Result {}'.format(res))
            return res.get('success')

        logger.info('Nothing to submit')

    return True


@app.task(name='presta.app.lims.verify_analyses')
def verify_analyses(samples, bika_conf):
    if samples and len(samples) > 0:
        paths = __get_analysis_paths(samples=samples, review_state='to_be_verified', bika_conf=bika_conf)

        if len(paths) > 0:
            logger.info('Verify {} analyses'.format(len(paths)))
            bika = __init_bika(bika_conf=bika_conf)
            res = bika.client.verify_analyses(paths)
            logger.info('Verify Result: {}'.format(res))
            return res.get('success')

        logger.info('Nothing to verify')

    return True


@app.task(name='presta.app.lims.publish_analyses')
def publish_analyses(samples, bika_conf):
    if samples and len(samples) > 0:
        paths = __get_analysis_paths(samples=samples, review_state='verified', bika_conf=bika_conf)

        if len(paths) > 0:
            logger.info('Publish {} analyses'.format(len(paths)))
            bika = __init_bika(bika_conf=bika_conf)
            res = bika.client.publish_analyses(paths)
            logger.info('Publish Result: {}'.format(res))
            return res.get('success')

        logger.info('Nothing to publish')

    return True


@app.task(name='presta.app.lims.publish_analysis_requests')
def publish_analysis_requests(samples, bika_conf):
    if samples and len(samples) > 0:
        paths = __get_ar_to_publish_paths(samples=samples, bika_conf=bika_conf)

        if len(paths) > 0:
            logger.info('Publish {} analysis requests'.format(len(paths)))
            bika = __init_bika(bika_conf=bika_conf)
            res = bika.client.publish_analysis_requests(paths)
            logger.info('Publish Result: {}'.format(res))
            return res.get('success')

        logger.info('Nothing to publish')

    return True


@app.task(name='presta.app.lims.search_batches_to_sync')
def search_batches_to_sync(**kwargs):
    conf = get_conf(logger)
    bika_conf = conf.get_section('bika')
    bika = __init_bika(bika_conf)

    # get open batches
    params = dict(Subject='open')
    batches = bika.client.get_batches(params)
    bids = [b.get('id') for b in batches.get('objects')]

    # search for
    for batch_id in bids:
        params = dict(title=batch_id)
        ars = bika.client.get_analysis_requests(params)

        # for ar in ars['objects']:
        #     ready = True
        #     if ar.get('sample_type')
        #     for a in ar['Analyses']:

    return True


@app.task(name='presta.app.lims.search_worksheets_to_sync')
def search_worksheets_to_sync(**kwargs):
    conf = get_conf(logger)
    bika_conf = conf.get_section('bika')

    return True

@app.task(name='presta.app.lims.search_samples_to_sync')
def search_samples_to_sync(**kwargs):
    conf = get_conf(logger)
    bika_conf = conf.get_section('bika')
    return True


def __get_analysis_paths(samples, review_state, bika_conf):
    bika = __init_bika(bika_conf)
    ids = [s.get('sample_id') for s in samples]
    params = dict(ids='|'.join(ids))

    ars = bika.client.get_analysis_requests(params)
    paths = list()

    for ar in ars['objects']:
        for a in ar['Analyses']:
            if str(a['id']) not in DENIED_ANALYSIS and str(a['review_state']) in [review_state]:
                paths.append(os.path.join(ar['path'], a['id']))

    return paths


def __get_ar_to_publish_paths(samples, bika_conf):
    bika = __init_bika(bika_conf)
    ids = [s.get('sample_id') for s in samples]
    params = dict(ids='|'.join(ids))

    ars = bika.client.get_analysis_requests(params)
    paths = list()

    for ar in ars['objects']:
        ready_to_publish = True
        for a in ar['Analyses']:
            if str(a['review_state']) not in ['verified']:
                ready_to_publish = False
                break

        if ready_to_publish:
            paths.append(ar['path'])

    return paths


def __init_bika(bika_conf, role='admin'):
    bika_roles = bika_conf.get('roles')
    if bika_conf and bika_roles and role in bika_roles:
        bika_role = bika_roles.get(role)
        url = bika_conf.get('url')
        user = bika_role.get('user')
        password = bika_role.get('password')
        bika = Bims(url, user, password, 'bikalims').bims
        return bika