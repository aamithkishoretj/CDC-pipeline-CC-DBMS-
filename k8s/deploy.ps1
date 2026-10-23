# Deploys the whole pipeline to a local Kubernetes cluster
# (Docker Desktop Kubernetes or Minikube). Run from the project root:
#     powershell -ExecutionPolicy Bypass -File k8s\deploy.ps1
# For Minikube, first point Docker at Minikube's daemon so the images are
# built inside the cluster:
#     & minikube -p minikube docker-env --shell powershell | Invoke-Expression

$ErrorActionPreference = "Stop"

Write-Host "Building images ..."
docker build -t cdc-consumer:latest  ./consumer
docker build -t cdc-write-api:latest ./services/write-api
docker build -t cdc-read-api:latest  ./services/read-api

Write-Host "Creating namespace, secret and config maps ..."
kubectl apply -f k8s/00-namespace-secrets.yaml
kubectl -n cdc create configmap postgres-init   --from-file=database/01_schema.sql --from-file=database/02_seed.sql --from-file=database/03_roles.sql --dry-run=client -o yaml | kubectl apply -f -
kubectl -n cdc create configmap debezium-config --from-file=debezium/      --dry-run=client -o yaml | kubectl apply -f -
kubectl -n cdc create configmap frontend --from-file=frontend/index.html --from-file=nginx.conf=k8s/nginx-k8s.conf --dry-run=client -o yaml | kubectl apply -f -

Write-Host "Deploying infrastructure and applications ..."
kubectl apply -f k8s/10-infrastructure.yaml
kubectl apply -f k8s/20-applications.yaml
kubectl apply -f k8s/30-network-policies.yaml

Write-Host ""
Write-Host "Watch pods start:   kubectl -n cdc get pods -w"
Write-Host "Dashboard:          http://localhost:30080   (Minikube: minikube -n cdc service frontend --url)"
