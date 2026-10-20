#!/usr/bin/env bash
# =============================================================================
#  Deploys the CDC pipeline to Microsoft Azure:
#    1. Azure Container Registry (ACR) holds the project's Docker images
#    2. images are built locally with Docker and pushed to ACR
#    3. an Ubuntu VM with Docker pulls them and runs the stack with Compose
#
#  Prerequisites: Docker Desktop running, `az login` done.
#  Usage (Git Bash):  bash azure/deploy-azure.sh
#  Tear down:         az group delete -n "$RG" --yes
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")/.."

AZ="${AZ:-az}"
RG="${RG:-cdc-pipeline-rg}"
LOCATION="${LOCATION:-centralindia}"          # resource group + registry
VM_LOCATION="${VM_LOCATION:-koreacentral}"   # Azure for Students: centralindia had no VM capacity
ACR="${ACR:-cdcpipeline$(printf '%s' "$($AZ account show --query id -o tsv)" | md5sum | cut -c1-8)}"
VM="${VM:-cdc-pipeline-vm}"
VM_SIZE="${VM_SIZE:-Standard_B2as_v2}"       # 2 vCPU / 8 GiB: enough for 3 Kafka brokers + Debezium
IMAGES="cdc-postgres cdc-connect-init cdc-consumer cdc-write-api cdc-read-api cdc-frontend cdc-prometheus cdc-grafana"

echo "==> Resource group $RG ($LOCATION)"
$AZ group create -n "$RG" -l "$LOCATION" -o none

echo "==> Container registry $ACR"
$AZ acr create -g "$RG" -n "$ACR" --sku Basic --admin-enabled true -o none
LOGIN_SERVER="$($AZ acr show -n "$ACR" --query loginServer -o tsv)"
ACR_USER="$($AZ acr credential show -n "$ACR" --query username -o tsv)"
ACR_PASS="$($AZ acr credential show -n "$ACR" --query 'passwords[0].value' -o tsv)"

echo "==> Building images with Docker and pushing to $LOGIN_SERVER"
echo "$ACR_PASS" | docker login "$LOGIN_SERVER" -u "$ACR_USER" --password-stdin
docker compose build
for img in $IMAGES; do
  docker tag "$img:latest" "$LOGIN_SERVER/$img:latest"
  docker push "$LOGIN_SERVER/$img:latest"
done

echo "==> VM $VM ($VM_SIZE) with Docker via cloud-init"
$AZ vm create -g "$RG" -n "$VM" -l "$VM_LOCATION" --image Ubuntu2204 --size "$VM_SIZE" \
  --admin-username azureuser --generate-ssh-keys \
  --custom-data azure/cloud-init.yaml --public-ip-sku Standard \
  --os-disk-size-gb 64 -o none
$AZ vm open-port -g "$RG" -n "$VM" --port 8080 --priority 1010 -o none   # website
$AZ vm open-port -g "$RG" -n "$VM" --port 3000 --priority 1020 -o none   # Grafana
IP="$($AZ vm show -d -g "$RG" -n "$VM" --query publicIps -o tsv)"
SSH="ssh -o StrictHostKeyChecking=accept-new azureuser@$IP"

echo "==> Waiting for Docker on $IP"
until $SSH 'test -f /var/lib/cloud/instance/docker-ready' 2>/dev/null; do sleep 10; done

GRAFANA_PW="$(openssl rand -hex 8)"
printf 'ACR_LOGIN_SERVER=%s\nGRAFANA_ADMIN_PASSWORD=%s\n' "$LOGIN_SERVER" "$GRAFANA_PW" > azure/.env.vm
scp -o StrictHostKeyChecking=accept-new azure/docker-compose.azure.yml "azureuser@$IP:/opt/cdc/docker-compose.yml"
scp azure/.env.vm "azureuser@$IP:/opt/cdc/.env"
rm -f azure/.env.vm

echo "==> Pulling from ACR and starting the stack"
echo "$ACR_PASS" | $SSH "sudo docker login $LOGIN_SERVER -u $ACR_USER --password-stdin && cd /opt/cdc && sudo docker compose pull -q && sudo docker compose up -d"

echo
echo "Website : http://$IP:8080"
echo "Grafana : http://$IP:3000   (admin password: $GRAFANA_PW)"
