from __future__ import absolute_import

from . import app
from alta.bims import Bims
from presta.utils import get_conf
from presta.utils import runJob
from celery import chain

from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


@app.task(name='presta.app.lims.sync_samples')
def sync_samples(samples, **kwargs):
    bika_conf = kwargs.get('conf')
    result = kwargs.get('result', '1')
    sync_all_analyses = kwargs.get('sync_all_analyses', False)

    if samples and len(samples) > 0:
        pipeline = chain(
            submit_analyses.si(samples, bika_conf, sync_all_analyses, result),
            verify_analyses.si(samples, bika_conf, sync_all_analyses),
            publish_analyses.si(samples, bika_conf, sync_all_analyses),
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


@app.task(name='presta.app.lims.process_deliveries')
def process_deliveries(deliveries):

    if isinstance(deliveries,list) and len(deliveries) > 0:
        logger.info('{} deliveries ready to process'.format(len(deliveries)))
        for delivery in deliveries:
            pipeline = chain(
                run_presta_delivery.si(delivery_id=delivery.get('id'))
            )
            pipeline.delay()

    else:
        logger.info('No deliveries to sync')


    return True


@app.task(name='presta.app.lims.set_delivery_started')
def set_delivery_started(**kwargs):
    delivery_id = kwargs.get('delivery_id')

    conf = get_conf(logger, None)
    bika_conf = conf.get_section('bika')
    bika = __init_bika(bika_conf)

    if delivery_id:
        logger.info('Set delivery {} as started'.format(delivery_id))
        res = bika.set_delivery_started(delivery_id)
        logger.info('Result {}'.format(res))
        return res.get('success')

    return True


@app.task(name='presta.app.lims.set_delivery_completed')
def set_delivery_completed(**kwargs):
    delivery_id = kwargs.get('delivery_id')

    conf = get_conf(logger, None)
    bika_conf = conf.get_section('bika')
    bika = __init_bika(bika_conf)

    if delivery_id:
        logger.info('Set delivery {} as completed'.format(delivery_id))
        res = bika.set_delivery_completed(delivery_id)
        logger.info('Result {}'.format(res))
        return res.get('success')

    return True


@app.task(name='presta.app.lims.update_delivery_details')
def update_delivery_details(**kwargs):
    delivery_id = kwargs.get('delivery_id')
    user = kwargs.get('user')
    password = kwargs.get('password')
    path = kwargs.get('path')

    conf = get_conf(logger, None)
    bika_conf = conf.get_section('bika')
    bika = __init_bika(bika_conf)

    if delivery_id:
        logger.info('Update details of delivery {}'.format(delivery_id))
        res = bika.update_delivery_details(delivery_id, user=user, password=password, path=path)
        logger.info('Result {}'.format(res))
        return res.get('success')

    return True


@app.task(name='presta.app.lims.sync_analysis_requests')
def sync_analysis_requests(samples, bika_conf):
    if samples and len(samples) > 0:
        pass

    return True


@app.task(name='presta.app.lims.submit_analyses')
def submit_analyses(samples, bika_conf, sync_all_analyses=False, result='1'):
    if samples and len(samples) > 0:
        bika = __init_bika(bika_conf)
        analyses = bika.get_analyses_ready_to_be_synchronized(samples=samples, action='submit',
                                                              sync_all_analyses=sync_all_analyses)

        if isinstance(analyses, list) and len(analyses) > 0:
            logger.info('Submit {} analyses'.format(len(analyses)))
            res = bika.submit_analyses(analyses, result)
            logger.info('Submit Result {}'.format(res))
            return res.get('success')

    logger.info('Nothing to submit')

    return True


@app.task(name='presta.app.lims.verify_analyses')
def verify_analyses(samples, bika_conf, sync_all_analyses=False):
    if samples and len(samples) > 0:
        bika = __init_bika(bika_conf)
        analyses = bika.get_analyses_ready_to_be_synchronized(samples=samples, action='verify',
                                                              sync_all_analyses=sync_all_analyses)

        if isinstance(analyses, list) and len(analyses) > 0:
            logger.info('Verify {} analyses'.format(len(analyses)))
            res = bika.verify_analyses(analyses)
            logger.info('Verify Result: {}'.format(res))
            return res.get('success')

    logger.info('Nothing to verify')

    return True


@app.task(name='presta.app.lims.publish_analyses')
def publish_analyses(samples, bika_conf, sync_all_analyses=False):
    if samples and len(samples) > 0:
        bika = __init_bika(bika_conf)
        analyses = bika.get_analyses_ready_to_be_synchronized(samples=samples, action='publish',
                                                              sync_all_analyses=sync_all_analyses)

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


@app.task(name='presta.app.lims.search_deliveries_to_sync')
def search_deliveries_to_sync(**kwargs):
    emit_events = kwargs.get('emit_events', False)
    conf = get_conf(logger, None)
    bika_conf = conf.get_section('bika')
    bika = __init_bika(bika_conf)

    deliveries = bika.get_deliveries_ready_to_process()

    if emit_events:
        pipeline = chain(
            process_deliveries.si(deliveries),
        )
        pipeline.delay()

    return True


@app.task(name='presta.app.lims.search_samples_to_sync')
def search_samples_to_sync(**kwargs):
    conf = get_conf(logger)
    bika_conf = conf.get_section('bika')
    return True


@app.task(name='presta.app.lims.run_presta_delivery')
def run_presta_delivery(**kwargs):
    emit_events = kwargs.get('emit_events', False)
    delivery_id = kwargs.get('delivery_id')

    cmd_line = ['presta', 'delivery']

    if delivery_id:
        cmd_line.extend(['--delivery_id', delivery_id])

        if emit_events:
            cmd_line.append('--emit_events')

        result = runJob(cmd_line, logger)
        return True if result else False

    return False


def __init_bika(bika_conf, role='admin'):
    bika_roles = bika_conf.get('roles')
    if bika_conf and bika_roles and role in bika_roles:
        bika_role = bika_roles.get(role)
        url = bika_conf.get('url')
        user = bika_role.get('user')
        password = bika_role.get('password')
        bika = Bims(url, user, password, 'bikalims').bims
        return bika