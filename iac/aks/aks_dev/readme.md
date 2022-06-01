# Terraform & AKS [Azure Kubernetes Service]
https://docs.microsoft.com/en-us/azure/developer/terraform/create-k8s-cluster-with-tf-and-aks 

### configure terraform
```sh
# install terraform 
brew install terraform

# verify version
# v1.0.11
terraform
terraform version

# azure default subscription
az login
az account show
az account set --subscription "<your-subscription>"

# create service principal
az ad sp create-for-rbac --name iac_terraform_identity

{
  "appId": "<your-app>",
  "displayName": "iac_terraform_identity",
  "name": "http://iac_terraform_identity",
  "password": "<your-pass>",
  "tenant": "<your-tenant-id>"
}
```

### Configuring 'variables.tf'
You will need to create a file called 'variables.tf' in the same directory that you are provisioning your AKS cluster (this directory). To do this, follow these steps:
- crete a copy of var_template.tf (located in path/to/directory/iac/aks/aks_dev/template/) to path/to/directory/iac/aks/aks_dev (one directory above)
- rename it as 'variable.tf'
- fill the variable.tf with the values that you got from the previous step

Now you are ready to run the next steps.

### build aks cluster using azure provider
```sh
# access iac terraform script
path/to/your/directory/iac/aks/aks_dev

# k8s.tf = build cluster and resources
# variables.tf = reusable variables

# init terraform script process
# prepare working directory
terraform init

# build plan to build 
# changes required
terraform plan

# apply creation iac code
# create resources
terraform apply -auto-approve

# access cluster
# kubernetes aks engine
az account set --subscription <your-subscription>
az aks get-credentials --resource-group k8s-aks-dev --name aks-dev

# change [variables.tf]
terraform plan
terraform apply

# remove resources [rg]
# destroy resources
terraform destroy -auto-approve
```