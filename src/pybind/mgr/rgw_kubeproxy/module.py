
"""
A hello world module

See doc/mgr/hello.rst for more info.
"""

from mgr_module import MgrModule, HandleCommandResult
from threading import Event
from kubernetes import client, config
from .utils import (
    dict_contains_path,
    _parse_addr,
    _parse_frontend_config,
    )
class RGWKubeProxy(MgrModule):
    # these are CLI commands we implement
    COMMANDS = [
        {
            "cmd": "hello "
                   "name=person_name,type=CephString,req=false",
            "desc": "Say hello",
            "perm": "r"
        },
        {
            "cmd": "count "
                   "name=num,type=CephInt",
            "desc": "Do some counting",
            "perm": "r"
        },
    ]

    # These are module options we understand.  These can be set with
    #
    #   ceph config set global mgr/hello/<name> <value>
    #
    # e.g.,
    #
    #   ceph config set global mgr/hello/place Earth
    #
    MODULE_OPTIONS = [
        {
            'name': 'place',
            'default': 'world',
            'desc': 'a place in the world',
            'runtime': True,   # can be updated at runtime (no mgr restart)
        },
        {
            'name': 'emphatic',
            'type': 'bool',
            'desc': 'whether to say it loudly',
            'default': True,
            'runtime': True,
        },
        {
            'name': 'foo',
            'type': 'enum',
            'enum_allowed': [ 'a', 'b', 'c' ],
            'default': 'a',
            'runtime': True,
        },
    ]


    def __init__(self, *args, **kwargs):
        super(RGWKubeProxy, self).__init__(*args, **kwargs)

        # set up some members to enable the serve() method and shutdown
        self.run = True
        self.event = Event()

    def handle_command(self, inbuf, cmd):
        ret = 0
        out = ''
        err = ''
        if cmd['prefix'] == 'hello':
            if 'person_name' in cmd:
                out = "Hello, " + cmd['person_name']
            else:
                out = "Hello " + self.get_module_option('place')
            if self.get_module_option('emphatic'):
                out += '!'
        elif cmd['prefix'] == 'count':
            num = cmd.get('num', 0)
            if num < 1:
                err = 'That\'s too small a number'
                ret = -errno.EINVAL
            elif num > 10:
                err = 'That\'s too big a number'
                ret = -errno.EINVAL
            else:
                out = 'Hello, I am the count!\n'
                out += ', '.join([str(x) for x in range(1, num + 1)]) + '!'
        return HandleCommandResult(
            retval=ret,   # exit code
            stdout=out,   # stdout
            stderr=err)

                                   stderr=message + "\n" + output_string)
    
    def setup_loadbalancer(rgws):
        rgw_dns_name = s3.ceph.svc.cluster.local

        k8s_service_name = 's3'
        k8s_api =  'https://api.cluster.example.com:6443'
        k8s_user = 'ceph'
        k8s_pass = '123456'
        k8s_project = 'ceph'
        k8s_proxy_mode = 'iptables'

        client, config

    def serve(self):
        """
        This method is called by the mgr when the module starts and can be
        used for any background activity.
        """
        self.log.info("Starting")
        service_map = self.get('service_map')
        if not dict_contains_path(service_map, ['services', 'rgw', 'daemons']):
            raise LookupError('No RGW found')
        daemon = None
        daemons = service_map['services']['rgw']['daemons']
        for key in daemons.keys():
            if dict_contains_path(daemons[key], ['metadata', 'frontend_config#0']):
                daemon = daemons[key]
                break
        if daemon is None:
            raise LookupError('No RGW daemon found')

        rgw_addr = _parse_addr(daemon['addr'])
        self.log.info('rgw addr is: {addr}'.format(addr=rgw_addr))
        rgw_port, rgw_ssl = _parse_frontend_config(daemon['metadata']['frontend_config#0'])
        self.log.info('rgw port is: {port}'.format(port=rgw_port))
        config = self.get('config')
        rgw_frontends = config['rgw_frontends']
        self.log.info('config rgw_frontends is: {rgw_frontends}'.format(rgw_frontends=rgw_frontends))
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
