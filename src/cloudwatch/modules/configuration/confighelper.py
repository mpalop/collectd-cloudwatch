import os
import re
from ..logger.logger import get_logger
from configreader import ConfigReader
from metadatareader import MetadataReader
from credentialsreader import CredentialsReader
from whitelist import Whitelist, WhitelistConfigReader

class ConfigHelper(object):
    """
    The configuration helper is responsible for obtaining configuration data from number 
    of sources based on predefined configuration precendence. 
    
    The configuration precedence from highest to lowest:
    1. Plugin Config File 
    2. Environment Variables
    3. Metadata 
    4. Collectd config file
    
    Keyword arguments:
    config_path -- The path to the plugin configuration file (Default '/opt/AmazonCloudWatchAgent/.aws/config')
    metadata_server -- The address of the metadata server (Default 'http://169.254.169.254/')
    """
    
    _LOGGER = get_logger(__name__)
    _DEFAULT_AGENT_ROOT_FOLDER = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, './config/') # '/opt/AmazonCloudWatchAgent/'
    _DEFAULT_CONFIG_PATH = _DEFAULT_AGENT_ROOT_FOLDER + 'plugin.conf'
    _DEFAULT_CREDENTIALS_PATH = _DEFAULT_AGENT_ROOT_FOLDER + ".aws/credentials"
    _METADATA_SERVICE_ADDRESS = 'http://169.254.169.254/' 
    WHITELIST_CONFIG_PATH = _DEFAULT_AGENT_ROOT_FOLDER + 'whitelist.conf'
    BLOCKED_METRIC_PATH = _DEFAULT_AGENT_ROOT_FOLDER + 'blocked_metrics'

    def __init__(self, config_path=_DEFAULT_CONFIG_PATH, metadata_server=_METADATA_SERVICE_ADDRESS):
        self._config_path = config_path
        self._metadata_server = metadata_server
        self._use_iam_role_credentials = False
        self.region = ''
        self.endpoint = ''
        self.host = ''
        self.proxy_protocol = ''
        self.proxy_server_name = ''
        self.proxy_server_port = ''
        self.debug = False
        self.pass_through = False
        self.enable_high_definition_metrics = False
        self.flush_interval_in_seconds = "60"
        self._load_configuration()
        self.whitelist = Whitelist(WhitelistConfigReader(self.WHITELIST_CONFIG_PATH, self.pass_through).get_regex_list(), self.BLOCKED_METRIC_PATH)

    @property
    def credentials(self):
        """ 
        Returns credentials. If IAM role is used, credentials will be empty
        Otherwise old credentials are returned.
        """
        return self._credentials

    @credentials.setter
    def credentials(self, credentials):
        self._credentials = credentials
        
    def _load_configuration(self):
        """ Try and load configuration based on the predefined precendence """
        self.config_reader = ConfigReader(self._config_path)
        self.credentials_reader = CredentialsReader(self._get_credentials_path())
        self.metadata_reader = MetadataReader(self._metadata_server)
        self._load_credentials()
        self._load_region()
        self._load_hostname()
        self._load_proxy_server_name()
        self._load_proxy_server_port()
        # TODO: implement Auto Scaling Group
        self._set_endpoint()
        self.debug = self.config_reader.debug
        self.pass_through = self.config_reader.pass_through
        self._check_configuration_integrity()
    
    def _get_credentials_path(self):
        credentials_path = self.config_reader.credentials_path
        if not self.config_reader.credentials_path:
            credentials_path = self._DEFAULT_CREDENTIALS_PATH
        return credentials_path
            
    def _load_credentials(self):
        """ 
        Tries to load credentials from plugin configuration file. If such file does not exist
        or does not contain credentials, then IAM role is used. 
        """
        self.credentials = self.credentials_reader.credentials
        
    def _load_region(self):
        """
        Loads region from plugin configuration file, if such file does not exist or does not
        contain region information, then metadata service is used.
        """
        if self.config_reader.region:
            self.region = self.config_reader.region
        else:
            try:
                self.region = self.metadata_reader.get_region()
            except Exception as e:
                ConfigHelper._LOGGER.warning("Cannot retrieve region from the local metadata server. Cause: " + str(e))
    
    def _load_hostname(self):
        """ 
        Load host from the configuration file, if configuration file does not contain host entry 
        then try to retrieve Instance ID from local metadata service. 
        """
        if self.config_reader.host:
            self.host = self.config_reader.host
        else:
            try:
                self.host = self.metadata_reader.get_instance_id()
            except Exception as e:
                ConfigHelper._LOGGER.warning("Cannot retrieve Instance ID from the local metadata server. Cause: " +
                                             str(e) + " Using host information provided by Collectd.")

    def _load_proxy_server_name(self):
        """
        Load proxy server name from the configuration file, if configuration file does not contain proxy entry
        then set proxy to None.
        """
        the_proxy = self.config_reader.proxy_server_name
        if the_proxy:
            proto = re.compile("^(https?)://").findall(the_proxy)
            if proto and proto[0]:
                self.proxy_protocol = proto[0]
                self.proxy_server_name = the_proxy
            else:
                raise ValueError("proxy_server_name must start with http: or https:")
        else:
            self.proxy_server_name = None

    def _load_proxy_server_port(self):
        """
        Load proxy server port from the configuration file, if configuration file does not contain proxy port entry
        then set proxy to None.
        """
        the_port = self.config_reader.proxy_server_port
        if the_port:
            if the_port.isdigit():
                self.proxy_server_port = self.config_reader.proxy_server_port
            else:
                raise ValueError("proxy_server_port must be an integer value")
        else:
            self.proxy_server_port = None

    def _set_endpoint(self):
        """ Creates endpoint from region information """
        if self.region is "localhost":
            self.endpoint = "http://" + self.region + "/"
        elif self.region.startswith("cn-"):
            self.endpoint = "https://monitoring." + self.region + ".amazonaws.com.cn/"
        else:
            self.endpoint = "https://monitoring." + self.region + ".amazonaws.com/"
            
    def _check_configuration_integrity(self):
        """ Check the state of this configuration helper object to ensure that all required values are loaded """
        if not self._credentials:
            raise ValueError("AWS _credentials are missing.")
        if not self.region:
            raise ValueError("Region is missing")
