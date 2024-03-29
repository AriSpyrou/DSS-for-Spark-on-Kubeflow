The repository for my master's thesis. 

Involves setting up a kubernetes cluster with kubeflow on a private cloud and using Hadoop (hdfs) as a data lake. Additionally, I developed a noval method for a kubeflow pipeline that is able to use an Apache Spark computing cluster to process data and apply ML models. What is more, I conducted experiments using the popular TPC-DS benchmark to test my setup and finally, I developed a decision support system for Spark configuration on the cloud using regression techniques.

I did all this in a cluster of 5 VMs that each consisted of: 4 vCPU, 8GB Ram, 60GB HDD.

Below are the cluster build and tear down instructions for anybody who's interested.

|||||||||||||||||||| Build: ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||

sudo snap install microk8s --classic --channel=1.21/stable

sudo snap install juju --classic

microk8s add-node

microk8s enable dashboard dns helm3 ingress metallb:10.64.140.43-10.64.140.49 storage

juju unregister my-c

juju bootstrap microk8s my-c

juju add-model kubeflow

juju deploy kubeflow-lite --trust

kubectl patch role -n kubeflow istio-ingressgateway-operator -p '{"apiVersion":"rbac.authorization.k8s.io/v1","kind":"Role","metadata":{"name":"istio-ingressgateway-operator"},"rules":[{"apiGroups":["*"],"resources":["*"],"verbs":["*"]}]}'

juju config dex-auth public-url=http://10.64.140.43.nip.io

juju config oidc-gatekeeper public-url=http://10.64.140.43.nip.io

juju config dex-auth static-username=admin

juju config dex-auth static-password=admin

helm install spark spark-operator/spark-operator --namespace spark-operator \
      --create-namespace \
      --set webhook.enable=true \
      --set ingress-url-format="10.64.140.43/\{\{\$appName\}\}"

kubectl patch configmap/coredns -n kube-system --patch-file coredns-patch.json / kubectl patch configmap/coredns -n kube-system -p '{"apiVersion": "v1","data": {"Corefile": ".:53 {\n    errors\n    health {\n      lameduck 5s\n    }\n    ready\n    hosts {\n    10.0.1.41 aspyrou-1\n    10.0.1.42 aspyrou-2\n    10.0.1.11 aspyrou-3\n    10.0.1.5 aspyrou-4\n    10.0.1.32 aspyrou-5\n    fallthrough\n}\nlog . {\n      class error\n    }\n    kubernetes cluster.local in-addr.arpa ip6.arpa {\n      pods insecure\n      fallthrough in-addr.arpa ip6.arpa\n    }\n    prometheus :9153\n    forward . 8.8.8.8 8.8.4.4 \n    cache 30\n    loop\n    reload\n    loadbalance\n}\n"},"kind": "ConfigMap"}'



|||||||||||||||||||| Tear Down: ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||

microk8s leave

sudo snap remove microk8s --purge

sudo snap remove juju --purge
