import requests

class Yarn():
    """
    Docs: http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html
    """
    headers = {"Content-Type": "application/json"}
    def __init__(self, yarn_url, auth):
        self.yarn_url = yarn_url
        self.auth = auth
    
    def __request_yarn__(self, uri, params=None):
        request_url = f"{self.yarn_url}{uri}"
        session = requests.Session()
        session.trust_env = False
        resp = session.get(request_url, headers=self.headers, auth=self.auth)
        return resp.json()
    
    def cluster_information(self):
        uri = "/ws/v1/cluster/info"
        return self.__request_yarn__(uri)
    
    def cluster_metrics(self):
        uri = "/ws/v1/cluster/metrics"
        return self.__request_yarn__(uri)

    def cluster_scheduler(self):
        uri = "/ws/v1/cluster/scheduler"
        return self.__request_yarn__(uri)

    def cluster_applications(self, params=None):
        # Query Parameters Supported
        uri = "/ws/v1/cluster/apps"
        return self.__request_yarn__(uri, params)

    def cluster_appstatistics(self, params=None):
        # Query Parameters Supported
        uri = "/ws/v1/cluster/appstatistics"
        return self.__request_yarn__(uri, params)

    def cluster_application(self, app_id):
        uri = "/ws/v1/cluster/apps/" + app_id
        return self.__request_yarn__(uri)

    def cluster_application_attempts(self, app_id):
        uri = "/ws/v1/cluster/apps/" + app_id + "/appattempts"
        return self.__request_yarn__(uri)

    def cluster_nodes(self, params=None):
        uri = "/ws/v1/cluster/nodes"
        return self.__request_yarn__(uri, params)

    def cluster_node(self, node_id):
        uri = "/ws/v1/cluster/nodes/" + node_id
        return self.__request_yarn__(uri)