FROM nginx

COPY configs/misp/nginx.conf.template /etc/nginx/templates/default.conf.template
COPY configs/misp/502.html /var/www/html/502.html
