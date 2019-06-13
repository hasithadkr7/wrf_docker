from google.cloud import container_v1

client = container_v1.ClusterManagerClient()

project_id = 'uwcc-160712'
zone = 'us-central1-a'

response = client.list_clusters(project_id, zone)

cluster = client.get_cluster(project_id, zone, 'temp-cluster-1')

from googleapiclient.discovery import build

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook


class GoogleKubernetesEngineHook(GoogleCloudBaseHook):
    def get_conn(self):
        http_authorized = self._authorize()
        return build('container', 'v1', http=http_authorized)
#
#
# hook = GoogleKubernetesEngineHook(conn_id='curw-gcp')
#
# import json
#
# j = hook.get_conn().projects().zones().clusters().get(projectId='uwcc-160712',  zone='us-central1-a', clusterId='temp-cluster-1').execute()
#
# print(json.dumps(j))
pass