# Apache vhosts (host)

Este directorio contiene plantillas de vhost para publicar los servicios del stack Docker desde el Apache del host.

## Archivos

- `vhost/datahub.conf`: landing estática (`/var/www/html`).
- `vhost/datahub-airflow.conf`: proxy HTTPS a Airflow (`127.0.0.1:8080`) en raiz `/`.
- `vhost/datahub-spark.conf`: proxy HTTPS a Spark UI (`127.0.0.1:9090`) en raiz `/`.
- `vhost/datahub-kafka.conf`: proxy HTTPS a Control Center (`127.0.0.1:9021`) en raiz `/`.
- `vhost/domains.example.conf`: variables `Define` para no hardcodear dominios.

Importante: `datahub-kafka` aqui es la UI/API HTTP (Control Center), no el broker Kafka (9092).

## Variables de dominio

1. Copia la plantilla y ajusta dominios reales:

```bash
cp www/vhost/domains.example.conf /etc/apache2/conf-available/datahub-domains.conf
```

Contenido esperado:

```apache
Define DATAHUB_MAIN_DOMAIN datahub.ecoceutics.com
Define DATAHUB_AIRFLOW_DOMAIN datahub-airflow.ecoceutics.com
Define DATAHUB_SPARK_DOMAIN datahub-spark.ecoceutics.com
Define DATAHUB_KAFKA_DOMAIN datahub-kafka.ecoceutics.com
```

## Debian/Ubuntu (apache2)

1. Copia vhosts a `sites-available` (o crea symlinks).
2. Habilita modulos necesarios.
3. Habilita `datahub-domains` y los sites.
4. Valida y recarga Apache.

Ejemplo:

```bash
sudo cp www/vhost/datahub*.conf /etc/apache2/sites-available/
sudo cp www/vhost/domains.example.conf /etc/apache2/conf-available/datahub-domains.conf

sudo a2enmod ssl proxy proxy_http proxy_wstunnel headers rewrite
sudo a2enconf datahub-domains

sudo a2ensite datahub.conf
sudo a2ensite datahub-airflow.conf
sudo a2ensite datahub-spark.conf
sudo a2ensite datahub-kafka.conf

sudo apachectl configtest
sudo systemctl reload apache2
```

Alternativa con symlinks (recomendada durante cambios frecuentes):

```bash
sudo ln -sfn /ruta/al/repo/ecopipeline/www/vhost/datahub.conf /etc/apache2/sites-available/datahub.conf
sudo ln -sfn /ruta/al/repo/ecopipeline/www/vhost/datahub-airflow.conf /etc/apache2/sites-available/datahub-airflow.conf
sudo ln -sfn /ruta/al/repo/ecopipeline/www/vhost/datahub-spark.conf /etc/apache2/sites-available/datahub-spark.conf
sudo ln -sfn /ruta/al/repo/ecopipeline/www/vhost/datahub-kafka.conf /etc/apache2/sites-available/datahub-kafka.conf

sudo a2ensite datahub.conf datahub-airflow.conf datahub-spark.conf datahub-kafka.conf
sudo apachectl configtest
sudo systemctl reload apache2
```

Con symlinks, cualquier cambio en `www/vhost/datahub*.conf` dentro del repo se refleja al recargar Apache (sin volver a copiar archivos).

## RHEL/CentOS/Rocky (httpd)

1. Copia `domains` y vhosts en `conf.d` (o crea symlinks).
2. Verifica que SSL/proxy modules esten instalados/cargados.
3. Valida y recarga `httpd`.

Ejemplo:

```bash
sudo cp www/vhost/domains.example.conf /etc/httpd/conf.d/datahub-domains.conf
sudo cp www/vhost/datahub*.conf /etc/httpd/conf.d/

sudo apachectl configtest
sudo systemctl reload httpd
```

Alternativa con symlinks:

```bash
sudo ln -sfn /ruta/al/repo/ecopipeline/www/vhost/datahub.conf /etc/httpd/conf.d/datahub.conf
sudo ln -sfn /ruta/al/repo/ecopipeline/www/vhost/datahub-airflow.conf /etc/httpd/conf.d/datahub-airflow.conf
sudo ln -sfn /ruta/al/repo/ecopipeline/www/vhost/datahub-spark.conf /etc/httpd/conf.d/datahub-spark.conf
sudo ln -sfn /ruta/al/repo/ecopipeline/www/vhost/datahub-kafka.conf /etc/httpd/conf.d/datahub-kafka.conf

sudo apachectl configtest
sudo systemctl reload httpd
```

## Certificados TLS

Cada vhost espera certificados en:

- `/etc/letsencrypt/live/${DATAHUB_MAIN_DOMAIN}/fullchain.pem`
- `/etc/letsencrypt/live/${DATAHUB_MAIN_DOMAIN}/privkey.pem`

Y equivalentemente para `DATAHUB_AIRFLOW_DOMAIN`, `DATAHUB_SPARK_DOMAIN`, `DATAHUB_KAFKA_DOMAIN`.

## Variables del stack relacionadas

Para coherencia de enlaces en aplicaciones proxied:

- Airflow: definir en `.env` `AIRFLOW_BASE_URL=https://<tu-dominio-airflow>`.
- Spark: definir en `.env`:
  - `SPARK_PUBLIC_DNS=<tu-dominio-spark>`
  - `SPARK_UI_REVERSE_PROXY_URL=https://<tu-dominio-spark>`
