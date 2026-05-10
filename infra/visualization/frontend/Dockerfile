FROM nginx:1.27-alpine
COPY visualization/frontend/dist/               /usr/share/nginx/html/
COPY infra/visualization/frontend/nginx.conf    /etc/nginx/conf.d/default.conf
EXPOSE 80
