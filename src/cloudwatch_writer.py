"""
CollectdCloudWatchPlugin plugin
"""
try:
    import collectd # this will be in python path when running from collectd
except:
    import cloudwatch.modules.collectd as collectd
import traceback

# import boto3
# test = boto3.client("cloudwatch")

from cloudwatch.modules.configuration.confighelper import ConfigHelper
from cloudwatch.modules.flusher import Flusher
from cloudwatch.modules.logger.logger import get_logger

_LOGGER = get_logger(__name__)


def aws_init():
    """
    Collectd callback entry used to initialize plugin
    """
    try:
        config = ConfigHelper()
        if config.proxy_server_name:     # TODO: ugly monkeypatching to overcome the limitations of boto3 with proxies
            import botocore.endpoint
            botocore.endpoint.EndpointCreator._get_proxies = {
                config.proxy_protocol: "%s:%s/" % (config.proxy_server_name, config.proxy_server_port)
            }
        import boto3
        session = boto3.session.Session()

        # check the credentials to use
        if config.credentials.access_key:                   # using standard acces and secret keys
            cw_client = session.client("cloudwatch",
                                     aws_access_key_id=config.credentials.access_key,
                                     aws_secret_access_key=config.credentials.secret_key
                                     )
        else:                                               # using IAM_Profile
            cw_client = session.client("cloudwatch")

        flusher = Flusher(config, cw_client)
        collectd.register_write(aws_write, data = flusher)
        _LOGGER.info('Initialization finished successfully.')
    except Exception as e:
        _LOGGER.error("Cannot initialize plugin. Cause: " + str(e) + "\n" + traceback.format_exc())


def aws_write(vl, flusher):
    """ 
    Collectd callback entry used to write metric data
    """
    flusher.add_metric(vl)
    
collectd.register_init(aws_init)
