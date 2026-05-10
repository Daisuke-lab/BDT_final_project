FROM nginx:1.27-alpine
COPY visualization/frontend/target/scala-3.7.3/frontend-opt/ /usr/share/nginx/html/
COPY visualization/frontend/index.html                        /usr/share/nginx/html/index.html
COPY infra/visualization/frontend/nginx.conf                  /etc/nginx/conf.d/default.conf
EXPOSE 80
