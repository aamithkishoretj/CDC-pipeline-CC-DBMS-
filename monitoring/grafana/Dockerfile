# Grafana with the datasource and CDC dashboard baked in (no bind mounts:
# Docker Desktop's Windows file sharing can't always read mounted folders).
FROM grafana/grafana:11.2.0
COPY provisioning /etc/grafana/provisioning
COPY dashboards /var/lib/grafana/dashboards
