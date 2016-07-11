from alta.workflows import Wms
from alta.bims import Bims
from alta.objectstore import build_object_store


class Client:
    def __init__(self, conf, logger):
        self.conf = conf
        self.logger = logger
        self.gi = None
        self.bk = None
        self.ir = None

        logger.debug('conf: '.format(conf))

    # Initializing connection to iRODS server
    def __init_irods(self):
        irods_conf = self.conf.get_section('irods')
        if irods_conf:
            host = irods_conf.get('host')
            port = irods_conf.get('port')
            user = irods_conf.get('user')
            password = irods_conf.get('password')
            zone = irods_conf.get('zone')

            self.ir = build_object_store(store='irods', host=host, user=user,
                                         port=port, password=password,
                                         zone=zone)

    # Initializing connection to Bika server
    def init_bika(self):
        bika_conf = self.conf.get_section('bika')
        if bika_conf:
            url = bika_conf.get('url')
            user = bika_conf.get('user')
            password = bika_conf.get('password')
            self.bk = Bims(url, user, password, 'bikalims').bims

    # Initializing connection to Galaxy server
    def __init_galaxy(self):
        galaxy_conf = self.conf.get_section('galaxy')
        if galaxy_conf:
            galaxy_host = galaxy_conf.get('url', None)
            api_key = galaxy_conf.get('user_api_key', None)

            self.gi = Wms(galaxy_host, api_key, 'galaxy').wms.client
