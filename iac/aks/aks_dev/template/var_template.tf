variable "subscription_id" {
    default = "your-subscription-id"
}

variable "tenant_id" {
    default = "your-tenant-id"
}

variable "client_id" {
    default = "your-app-id"
}

variable "client_secret" {
    default = "your-app-password"
}

variable "agent_count" {
    default = 4
}

variable "dns_prefix" {
    default = "dns"
}

variable cluster_name {
    default = "aks-dev"
}

variable resource_group_name {
    default = "k8s-aks-dev"
}

variable location {
    default = "East US 2"
}
