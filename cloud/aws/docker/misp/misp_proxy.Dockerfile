FROM nginx

COPY configs/misp/nginx.conf.template /etc/nginx/templates/default.conf.template
