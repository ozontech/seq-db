# Helm Chart

Install [Helm](https://helm.sh/docs/intro/install/)

## Examples for Minikube Minikube

Install [Minikube](https://minikube.sigs.k8s.io/docs/start/)

### Install

```shell
helm upgrade --install seq-db . -f values.minikube.yaml

# add ingress hosts
echo "$( minikube ip )    seq-proxy.local" >> /etc/hosts
echo "$( minikube ip )    sequi-server.local" >> /etc/hosts
echo "$( minikube ip )    sequi.local" >> /etc/hosts

# send test data
curl --request POST \
  --url http://seq-proxy.local/_bulk \
  --header 'Content-Type: application/json' \
  --data '{"index" : {"unused-key":""}}
{"k8s_pod": "app-backend-123", "k8s_namespace": "production", "k8s_container": "app-backend", "request": "POST", "request_uri": "/api/v1/orders", "message": "New order created successfully"}
{"index" : {"unused-key":""}}
{"k8s_pod": "app-frontend-456", "k8s_namespace": "production", "k8s_container": "app-frontend", "request": "GET", "request_uri": "/api/v1/products", "message": "Product list retrieved"}
{"index" : {"unused-key":""}}
{"k8s_pod": "payment-service-789", "k8s_namespace": "production", "k8s_container": "payment-service", "request": "POST", "request_uri": "/api/v1/payments", "message": "failed"}
'
```

Open http://sequi.local/?from=3600
