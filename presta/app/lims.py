from __future__ import absolute_import

from . import app
from alta.bims import Bims
from presta.app.router import dispatch_event
from celery import chain

import os

from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)

DENIED_ANALYSIS = ['full-analysis']

QUERY_REVIEW_STATE = dict(
    submit='sample_received',
    verify='to_be_verified',
    publish='verified',
)

@app.task(name='presta.app.lims.sync_samples')
def sync_samples(samples, **kwargs):
    bika_conf = kwargs.get('conf')
    result = kwargs.get('result', '1')

    if samples and len(samples) > 0:
        pipeline = chain(
            sync_analyses.si(samples, bika_conf, result),
            sync_analysis_requests.si(samples, bika_conf),
        )
        pipeline.delay()

    return True


@app.task(name='presta.app.lims.sync_analyses')
def sync_analyses(samples, bika_conf, result='1'):

    if samples and len(samples) > 0:
        pipeline = chain(
            submit.si(samples, bika_conf, result),
            verify.si(samples, bika_conf),
            publish.si(samples, bika_conf),
        )
        pipeline.delay()

    return True


@app.task(name='presta.app.lims.sync_analysis_requests')
def sync_analysis_requests(samples, bika_conf):
    if samples and len(samples) > 0:
        pass

    return True


@app.task(name='presta.app.lims.submit')
def submit(samples, bika_conf, result='1'):
    if samples and len(samples) > 0:
        try:
            logger.info('Submitting...')
            paths = __get_analysis_paths(samples=samples, review_state='sample_received', bika_conf=bika_conf)
            bika = __init_bika(bika_conf, role='analyst')

            res = bika.client.submit_analyses(paths=paths, result=1)
            logger.info(res)
        except:
            return False

    return True


@app.task(name='presta.app.lims.verify')
def verify(samples, bika_conf):
    if samples and len(samples) > 0:
        paths = __get_analysis_paths(samples=samples, review_state='to_be_verified', bika_conf=bika_conf)

        # pipeline = chain()

    return True


@app.task(name='presta.app.lims.publish')
def publish(samples, bika_conf):
    if samples and len(samples) > 0:
        paths = __get_analysis_paths(samples=samples, review_state='verified', bika_conf=bika_conf)
        #logger.info(paths)
        # pipeline = chain()

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


def __init_bika(bika_conf, role='admin'):
    bika_roles = bika_conf.get('roles')
    if bika_conf and bika_roles and role in bika_roles:
        bika_role = bika_roles.get(role)
        url = bika_conf.get('url')
        user = bika_role.get('user')
        password = bika_role.get('password')
        bika = Bims(url, user, password, 'bikalims').bims
        return bika