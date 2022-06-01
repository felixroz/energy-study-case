```sh
helm repo remove minio
helm repo add minio https://operator.min.io/
helm install --namespace minio-operator --create-namespace --generate-name minio/minio-operator
kubectl apply -f https://github.com/minio/operator/blob/master/examples/tenant.yaml
```

# Get the JWT for logging in to the console:
kubectl get secret $(kubectl get serviceaccount console-sa --namespace minio-operator -o jsonpath="{.secrets[0].name}") --namespace minio-operator -o jsonpath="{.data.token}" | base64 --decode 

# Get the Operator Console URL by running these commands:
kubectl --namespace minio-operator port-forward svc/console 9090:9090
echo "Visit the Operator Console at http://127.0.0.1:9090"

# MinIO Kubernetes
kubectl krew update
kubectl krew install minio

# Persistent Volume
kubectl apply -f https://k8s.io/examples/pods/storage/pv-volume.yaml

