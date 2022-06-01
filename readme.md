# Requirements
First you have setted up your environment and your cluster, make sure you have installed:
* kubernetes cli
* kubens & kubectx
* an available cluster to interact with (In our case AKS) (you can deploy your own using the instructions passed inside /iac/aks/akd_dev/readme.md)
* helm
* [ArgoCD CLI](https://argo-cd.readthedocs.io/en/stable/cli_installation/#windows)
* GitBash (if you are using windows)

# Cluster Specifications (Recommended)
* microsoft azure aks cost
* 7 = virtual machines (ds3v2)
* 4 vcpus & 14 gb of ram
* 1,170.19
* ~ R$ 10.000 ~ $2000/month

# Creating Namespaces on your cluster
By doing this you can divide your resources logically
If you already know how Azure works, it will bring the same facilities that a resource group brings to you

```shell
kubectl create namespace orchestrator
kubectl create namespace database
kubectl create namespace ingestion
kubectl create namespace processing
kubectl create namespace datastore
kubectl create namespace deepstorage
kubectl create namespace tracing
kubectl create namespace logging
kubectl create namespace monitoring
kubectl create namespace viz
kubectl create namespace cicd
kubectl create namespace app
kubectl create namespace cost
kubectl create namespace misc
kubectl create namespace dataops
kubectl create namespace gateway
```

# Adding HELM repos
Here we are going to add the helm repos that we are going to use in this solution

```sh
helm repo add apache-airflow https://airflow.apache.org/
helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator
helm repo add argo https://argoproj.github.io/argo-helm
helm repo add minio https://operator.min.io/ 
helm repo update 
```

# Argo CD
We are going to use ArgoCD as our CD tool, by doing this we can:
* Garentee the desired state on our cluster accordingly with the properties passed on the file inside our git respository
* Work with GitOps

# Argo CD - Installation
```sh
helm install argocd argo/argo-cd --namespace cicd --version 3.26.8
```
If you want to expose this service through a load balancer

# Argo CD - Login - Port-Forward

- 1. Run
```sh
kubectl port-forward service/argocd-server -n cicd 8080:443
```
open the browser on http://localhost:8080 and accept the certificate
- 2. After reaching the UI the first time you can login with username: admin and the random password generated during the installation. You can find the password by running:
```sh
kubectl -n cicd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | base64 -d
```

# Argo CD - Login - Load Balancer
```shell
kubectl patch svc argocd-server -n cicd -p '{"spec": {"type": "LoadBalancer"}}'
```
Once you retrieve your load balancer ip
```shell
kubens cicd && kubectl get services -l app.kubernetes.io/name=argocd-server,app.kubernetes.io/instance=argocd -o jsonpath="{.items[0].status.loadBalancer.ingress[0].ip}"
```
Now you need to get the password to login into argocd UI
```shell
ARGOCD_LB=<your-loadbalancer-ip>
kubens cicd && kubectl get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | base64 -d | xargs -t -I {} argocd login $ARGOCD_LB --username admin --password {} --insecure
```

# Hints ArgoCD - Windows user
If you are using windows probably the instructions above will not work for you. So instead of using these commands try to:

* Apply the yaml "argo-load-balancer.yaml" and then get the ip using get services
```shell
kubectl apply -f argo-load-balancer.yaml
kubectl get services -n cicd
```

Once you have access you the ArgoCD UI, you will need to get the password for the admin account. So you can try to:
* Open your GitBash
```shell
kubectl -n cicd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | base64 -d
```
If you want to access your ArgoCD without the LoadBalancer you can use port-foward too, like this:
```shell
kubectl port-forward service/argocd-server -n cicd 8080:443
```

# Setting Up ArgoCD
Login into your ArgoCD
```shell
kubectl get services
argocd login <your-loadbalancer-ip>
```
Now create cluster role binding for admin user [sa]
```shell
kubectl create clusterrolebinding cluster-admin-binding --clusterrole=cluster-admin --user=system:serviceaccount:cicd:argocd-application-controller -n cicd
```

Register your cluster in ArgoCD
```shell
CLUSTER=<your-cluster-name>
argocd cluster add $CLUSTER --in-cluster
```

Register your repository in ArgoCD

```shell
argocd repo add https://github.com/ntc-Felix/bigdata-on-k8s --username <username> --password <password>
```


# Installing AIRFLOW
helm upgrade --install airflow apache-airflow/airflow --namespace orchestrator

s