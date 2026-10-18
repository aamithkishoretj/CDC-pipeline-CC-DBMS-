# nginx with the website and its routing config baked in (no bind mounts:
# Docker Desktop's Windows file sharing can fail to mount single files).
FROM nginx:1.27-alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY index.html /usr/share/nginx/html/index.html
