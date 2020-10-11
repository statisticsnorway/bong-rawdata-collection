# Docker Proxy

In Docker 17.06 and lower the proxy configuration does not work as expected (specifically, ver 1.13).

The Docker Proxy uses Docker 19.x and runs in _privileged mode_, thus it is able to connect to e.g. Google Cloud
 Container Registry (GCR) from within the docker container, running on an older engine.
 
The aim of the Docker Proxy is to:

* Authenticate with GCR using a service account file
* Pull an image
* Save image to a temp tar-file
* Load the pulled image onto the host Docker Engine

Proxy targets:

* `oauth2.googleapis.com:443`
* `www.googleapis.com:443`
* `eu.gcr.io:443`

Usage:

```
$ docker-proxy-start.sh

$ docker-proxy-login.sh SA_JSON_FILE

# Pull and import image to docker host
$ docker-proxy-merge.sh IMAGE IMAGE_VER

$ sudo docker run -it eu.gcr.io:443/CONTEXT/IMAGE/IMAGE_VER
```

Please refer to the `docker-proxy/docker-proxy-*.sh` scripts for further use.
