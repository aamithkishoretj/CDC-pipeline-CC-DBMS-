# Runs the whole pipeline on a real Kubernetes cluster (k3s) inside Docker,
# with no Minikube or Docker Desktop Kubernetes needed. Run from the project root:
#     powershell -ExecutionPolicy Bypass -File k8s\k3s-local.ps1
# Remove the cluster afterwards with:
#     powershell -ExecutionPolicy Bypass -File k8s\k3s-local.ps1 -Delete
#
# The Docker Compose stack also uses a lot of memory, so it is stopped first
# (start it again later with: docker compose up -d).

param([switch]$Delete)
# "Continue", not "Stop": Docker writes normal progress to stderr, which Windows
# PowerShell 5.1 would otherwise treat as a fatal error. Run() checks exit codes.
$ErrorActionPreference = "Continue"
$Cluster = "cdc-k3s"
$env:KUBECONFIG = Join-Path $PWD "k8s\kubeconfig.yaml"

function Run($exe) {
    # Run a native command and stop the script if it fails.
    & $exe @args
    if ($LASTEXITCODE -ne 0) { throw "$exe $($args -join ' ') failed with exit code $LASTEXITCODE" }
}

if ($Delete) {
    docker rm -f $Cluster | Out-Null
    Remove-Item -ErrorAction SilentlyContinue $env:KUBECONFIG
    Write-Host "Cluster removed."
    exit 0
}

Write-Host "Stopping the Docker Compose stack to free memory ..."
docker compose stop | Out-Null

Write-Host "Building application images ..."
Run docker build -q -t cdc-consumer:latest  ./consumer
Run docker build -q -t cdc-write-api:latest ./services/write-api
Run docker build -q -t cdc-read-api:latest  ./services/read-api

Write-Host "Starting k3s (Kubernetes) in Docker ..."
docker rm -f $Cluster 2>$null | Out-Null
Run docker run -d --name $Cluster --privileged --cgroupns=host -p 6443:6443 -p 30080:30080 `
    --tmpfs /run --tmpfs /var/run rancher/k3s:v1.30.4-k3s1 server --disable=traefik --write-kubeconfig-mode=644

Write-Host "Waiting for the node to be Ready ..."
for ($i = 0; $i -lt 60; $i++) {
    docker cp "${Cluster}:/etc/rancher/k3s/k3s.yaml" $env:KUBECONFIG 2>$null | Out-Null
    if ((Test-Path $env:KUBECONFIG) -and ((kubectl get nodes 2>$null) -match " Ready")) { break }
    Start-Sleep -Seconds 3
}
Run kubectl get nodes

Write-Host "Copying images into the cluster (so nothing is downloaded twice) ..."
$images = "cdc-consumer:latest", "cdc-write-api:latest", "cdc-read-api:latest", "postgres:15",
          "confluentinc/cp-zookeeper:7.6.0", "confluentinc/cp-kafka:7.6.0", "debezium/connect:2.6",
          "redis:7-alpine", "nginx:1.27-alpine"
$tar = Join-Path $env:TEMP "cdc-image.tar"
foreach ($img in $images) {
    Write-Host "  $img"
    docker image inspect $img 2>$null | Out-Null
    if ($LASTEXITCODE -ne 0) { Run docker pull -q $img | Out-Null }
    Run docker save --platform linux/amd64 -o $tar $img
    Run docker cp $tar "${Cluster}:/tmp/image.tar"
    Run docker exec $Cluster ctr -n k8s.io images import --platform linux/amd64 /tmp/image.tar | Out-Null
}
Remove-Item -ErrorAction SilentlyContinue $tar

Write-Host "Deploying manifests ..."
Run kubectl apply -f k8s/00-namespace-secrets.yaml
kubectl -n cdc create configmap postgres-init   --from-file=postgres-init/ --dry-run=client -o yaml | kubectl apply -f -
kubectl -n cdc create configmap debezium-config --from-file=debezium/      --dry-run=client -o yaml | kubectl apply -f -
kubectl -n cdc create configmap frontend --from-file=frontend/index.html --from-file=nginx.conf=k8s/nginx-k8s.conf --dry-run=client -o yaml | kubectl apply -f -
Run kubectl apply -f k8s/10-infrastructure.yaml
Run kubectl apply -f k8s/20-applications.yaml
Run kubectl apply -f k8s/30-network-policies.yaml

Write-Host "Waiting for all pods ..."
Run kubectl -n cdc wait --for=condition=complete job/register-connector --timeout=300s
Run kubectl -n cdc wait --for=condition=ready pod --all --field-selector=status.phase=Running --timeout=300s
Run kubectl -n cdc get pods

Write-Host ""
Write-Host "Website on Kubernetes:  http://localhost:30080"
Write-Host "Use kubectl in this window with:  `$env:KUBECONFIG = '$env:KUBECONFIG'"
Write-Host "Run the tests against it:          `$env:BASE_URL = 'http://localhost:30080'; python tests/test_e2e.py"
