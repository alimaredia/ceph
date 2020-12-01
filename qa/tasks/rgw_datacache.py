"""
rgw datacache testing
"""
from io import BytesIO
from configobj import ConfigObj
import logging

from teuthology.exceptions import ConfigError
from teuthology.task import Task
from teuthology import misc as teuthology

log = logging.getLogger(__name__)

class RGWDataCache(Task):
    """
    Runs a test against a rgw configuration with the data cache enabled. 

    - rgw-datacache:
        client0:
    """
    def __init__(self, ctx, config):
        super(RGWDataCache, self).__init__(ctx, config)
        clients = ['client.{id}'.format(id=id_)
                   for id_ in teuthology.all_roles_of_type(self.ctx.cluster, 'client')]
        self.all_clients = []
        for client in clients:
            if client in self.config:
                self.all_clients.extend([client])
        if self.all_clients is None:
            raise ConfigError('no clients have been declared in the rgw-datacache task config')

    def setup(self):
        super(RGWDataCache, self).setup()

        if not self.ctx.rgw.datacache:
            raise ConfigError('rgw datacache task must run if datacache is set int the rgw task')

        log.info('setting up rgw datacache tests')

        for client in self.all_clients:
            # unsure if install task will allow overrides for extra_packages, manually installing this package for now
            #self.ctx.cluster.only(client).run(
                #args=[
                #    'sudo',
                #    'yum',
                #    '-y',
                #    'install',
                #    's3cmd',
                #],
                #stdout=BytesIO()
            #)

            (remote,) = self.ctx.cluster.only(client).remotes.keys()
            testdir = teuthology.get_testdir(self.ctx)

            file_path = '{tdir}/archive/8M.dat'.format(tdir=testdir)
            self.ctx.cluster.only(client).run(
                args=[
                    'fallocate',
                    '-l',
                    '8M',
                    '{path}'.format(path=file_path),
                ],
                stdout=BytesIO()
            )

            # create rgw user for s3cmd operations
            self.create_user()
            
            # create minimal config for s3cmd
            self.create_s3cmd_config(client)

        log.info('set up rgw datacache tests')


    def begin(self):
        log.info('running rgw datacache task')

        for client in self.all_clients:
            (remote,) = self.ctx.cluster.only(client).remotes.keys()
            testdir = teuthology.get_testdir(self.ctx)
            archive_path = '{tdir}/archive'.format(tdir=testdir)
            s3cfg_path = '{archive_dir}/s3cfg'.format(archive_dir=archive_path)
            file_path = '{archive_dir}/8M.dat'.format(archive_dir=archive_path)

            endpoint = self.ctx.rgw.role_endpoints.get(client)

            self.ctx.cluster.only(client).run(
                args=[
                    's3cmd',
                    '--access_key=akey',
                    '--secret_key=skey',
                    '--config={path}'.format(path=s3cfg_path),
                    '--host={ip}:{port}'.format(ip=endpoint.hostname, port=endpoint.port),
                    'mb',
                    's3://bkt',
                ],
                stdout=BytesIO()
            )

            self.ctx.cluster.only(client).run(
                args=[
                    's3cmd',
                    '--access_key=akey',
                    '--secret_key=skey',
                    '--config={path}'.format(path=s3cfg_path),
                    '--host={ip}:{port}'.format(ip=endpoint.hostname, port=endpoint.port),
                    'put',
                    '{file_path}'.format(file_path=file_path),
                    's3://bkt',
                ],
                stdout=BytesIO()
            )

            self.ctx.cluster.only(client).run(
                args=[
                    's3cmd',
                    '--access_key=akey',
                    '--secret_key=skey',
                    '--config={path}'.format(path=s3cfg_path),
                    '--host={ip}:{port}'.format(ip=endpoint.hostname, port=endpoint.port),
                    'get',
                    's3://bkt/8M.dat',
                    '{archive_dir}/8M-get.dat'.format(archive_dir=archive_path),
                    '--force',
                ],
                stdout=BytesIO()
            )

            self.ctx.cluster.only(client).run(
                args=[
                    'ls',
                    '-asl',
                    '{path}'.format(path=self.ctx.rgw.datacache_path),
                ],
                stdout=BytesIO()
            )

        log.info('finished rgw datacache task')

        # Failure on purpose for debugging
        #log.info("data cache path is %s", self.ctx.rgw.dtacache_path)

    def create_user(self):
        """
        Create a user for data cache tests
        """
        log.info("RGW Datacache Tests: Creating S3 user...")
        testdir = teuthology.get_testdir(self.ctx)
        for client in self.all_clients:
                cluster_name, daemon_type, client_id = teuthology.split_role(client)
                client_with_id = daemon_type + '.' + client_id
                display_name = 'data cache user'
                access_key = 'akey'
                secret_key = 'skey'
                email = 'data_cache_user@email.com'
                args = [
                    'adjust-ulimits',
                    'ceph-coverage',
                    '{tdir}/archive/coverage'.format(tdir=testdir),
                    'radosgw-admin',
                    '-n', client_with_id,
                    'user', 'create',
                    '--uid', 'datacache_user',
                    '--display-name', display_name,
                    '--access-key', access_key,
                    '--secret', secret_key,
                    '--email', email,
                    '--cluster', cluster_name,
                ]
                log.info('{args}'.format(args=args))
                self.ctx.cluster.only(client).run(
                    args=args,
                    stdout=BytesIO()
                )
        log.info("Created S3 user for RGW datacache tests")

    def create_s3cmd_config(self, client):
        """
        Create a minimal config file for s3cmd
        """
        log.info("Creating s3cmd config...")
        (remote,) = self.ctx.cluster.only(client).remotes.keys()
        testdir = teuthology.get_testdir(self.ctx)
        path = '{tdir}/archive/s3cfg'.format(tdir=testdir)

        s3cmd_config = ConfigObj(
            indent_type='',
            infile={
                'default':
                    {
                    'host_bucket': 'no.way.in.hell',
                    'use_https': 'False',
                    },
                }
            )

        conf_fp = BytesIO()
        s3cmd_config.write(conf_fp)
        remote.write_file(path, data=conf_fp.getvalue())
        log.info("Created s3cmd config at %s", path)

task = RGWDataCache
