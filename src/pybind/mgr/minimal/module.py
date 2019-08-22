
"""
A minimal module
"""

from mgr_module import MgrModule
from threading import Event

def dict_contains_path(dct, keys):
    """
    Tests whether the keys exist recursively in `dictionary`.

    :type dct: dict
    :type keys: list
    :rtype: bool
    """
    if keys:
        if not isinstance(dct, dict):
            return False
        key = keys.pop(0)
        if key in dct:
            dct = dct[key]
            return dict_contains_path(dct, keys)
        return False
    return True

class Minimal(MgrModule):
    # these are CLI commands we implement
    COMMANDS = []

    # these are module options we understand.  These can be set with
    # 'ceph config set global mgr/hello/<name> <value>'.  e.g.,
    # 'ceph config set global mgr/hello/place Earth'
    MODULE_OPTIONS = []

    def __init__(self, *args, **kwargs):
        super(Minimal, self).__init__(*args, **kwargs)

        # set up some members to enable the serve() method and shutdown
        self.run = True
        self.event = Event()

    def serve(self):
        """
        This method is called by the mgr when the module starts and can be
        used for any background activity.
        """
        self.log.info("Starting")

        # get the rgw frontend config info from the service_map
        service_map = self.get('service_map')
        daemon = None
        daemons = service_map['services']['rgw']['daemons']
        for key in daemons.keys():
            if dict_contains_path(daemons[key], ['metadata', 'frontend_config#0']):
                daemon = daemons[key]
                break
        rgw_frontend_config_service_map = daemon['metadata']['frontend_config#0']
        self.log.info('service map rgw frontend config is: {rgw_frontend_config}'.format(rgw_frontend_config=rgw_frontend_config_service_map))

        # get the rgw frontend config info from config
        config = self.get('config')
        rgw_frontend_from_config = config['rgw_frontends']
        self.log.info('config rgw frontend config is: {rgw_frontend_config}'.format(rgw_frontend_config=rgw_frontend_from_config))

        while self.run:
            sleep_interval = 5
            self.log.debug('Sleeping for %d seconds', sleep_interval)
            ret = self.event.wait(sleep_interval)
            self.event.clear()

    def shutdown(self):
        """
        This method is called by the mgr when the module needs to shut
        down (i.e., when the serve() function needs to exit.
        """
        self.log.info('Stopping')
        self.run = False
        self.event.set()
