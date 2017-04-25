from ..logger.logger import get_logger

class PutClient(object):
    """
    This is a simple HTTPClient wrapper which supports putMetricData operation on CloudWatch endpoints. 
    
    Keyword arguments:
    region -- the region used for request signing.
    endpoint -- the endpoint used for publishing metric data
    credentials -- the AWSCredentials object containing access_key, secret_key or 
                IAM Role token used for request signing
    connection_timeout -- the amount of time in seconds to wait for extablishing server connection
    response_timeout -- the amount of time in seconds to wait for the server response 
    """
    
    _LOGGER = get_logger(__name__)
    _DEFAULT_CONNECTION_TIMEOUT = 1
    _DEFAULT_RESPONSE_TIMEOUT = 3
    _TOTAL_RETRIES = 1
    _LOG_FILE_MAX_SIZE = 10*1024*1024

    def __init__(self, config_helper, cw_client):
        self.config = config_helper
        self.client = cw_client

    def put_metric_data(self, namespace, metric_list):
        """
        Publishes metric data to the endpoint with single namespace defined. 
        It is consumers responsibility to ensure that all metrics in the metric list 
        belong to the same namespace.
        """
        
        if not self._is_namespace_consistent(namespace, metric_list):
            raise ValueError("Metric list contains metrics with namespace different than the one passed as argument.")

        # preparing the data in the format necessary to send the stats to cloudwatch
        data = []
        for metric in metric_list:
            curr = {"MetricName":  metric.metric_name,
                    "Timestamp":   metric.timestamp,
                    # "Unit":        metric.unit,           FIXME: currently it is empty
                    "Dimensions":  [
                        {"Name": "Host",            "Value": metric.dimensions['Host']},
                        {"Name": "PluginInstance",  "Value": metric.dimensions['PluginInstance']}
                    ],
                    "StatisticValues": {
                         "SampleCount": metric.statistics.sample_count,
                         "Sum":         metric.statistics.sum,
                         "Minimum":     metric.statistics.min,
                         "Maximum":     metric.statistics.max
                    }
            }
            data.append(curr)

        try:
            response = self.client.put_metric_data(
                Namespace=metric.namespace,
                MetricData=data
            )
        except Exception as e:
            self._LOGGER.warning("Could not put metric data using the following data: '" + str(data) +
                                 "'. [Exception: " + str(e) + "]")

    def _is_namespace_consistent(self, namespace, metric_list):
        """
        Checks if namespaces declared in MetricData objects in the metric list are consistent
        with the defined namespace.
        """
        for metric in metric_list:
            if metric.namespace is not namespace:
                return False
        return True

    class InvalidEndpointException(Exception):
        pass
