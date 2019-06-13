from kubernetes import client, config

config.load_kube_config()

v1 = client.CoreV1Api()
print("Listing pods with their IPs:")
ret = v1.list_pod_for_all_namespaces(watch=False)
for i in v1.list_node().items:
    print(i.metadata.name)

for i in ret.items:
    print("%s\t%s\t%s" % (i.status.pod_ip, i.metadata.namespace, i.metadata.name))

pod = client.V1Pod()
pod.metadata = client.V1ObjectMeta(name="busybox")

container = client.V1Container(name='test')
container.image = "busybox"
container.args = ["sleep", "3600"]
# container.name = "busybox"

spec = client.V1PodSpec(containers=[container])

pod.spec = spec

v1.create_namespaced_pod(namespace="default", body=pod)

pass
