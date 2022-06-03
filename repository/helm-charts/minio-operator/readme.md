# Installing MinIO using Helm
```sh
helm repo remove minio
helm repo add minio https://operator.min.io/
helm install --namespace deepstorage --create-namespace --generate-name minio/minio-operator
kubectl apply -f https://github.com/minio/operator/blob/master/examples/tenant.yaml
```

# Installing MiniO using ArgoCD
```sh
kubectl apply -f /repository/app-manifests/deepstorage/minio-operator.yaml
```

# Get the Operator Console URL by running these commands:
```sh
kubectl --namespace deepstorage port-forward svc/console 9090:9090
```
echo "Visit the Operator Console at http://127.0.0.1:9090"

# Get the JWT for logging in to the console:
kubectl get secret $(kubectl get serviceaccount console-sa --namespace deepstorage -o jsonpath="{.secrets[0].name}") --namespace deepstorage -o jsonpath="{.data.token}" | base64 --decode 

- Log in to the console using your JWT

# Create Your Tenant
```sh
kubectl apply -f ./repository/yamls/minio/tenant-dev.yaml
```
- Remember to take note of your Console and Endpoint IPs

