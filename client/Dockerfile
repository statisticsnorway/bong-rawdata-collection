#
# Build Image
#
FROM statisticsnorway/alpine-jdk15-buildtools:latest as build



#
# Container Image
#
FROM alpine:latest

RUN apk add --no-cache bash su-exec curl openssl

ENV APP_HOME=/opt/app

ENV JAVA_OPTS=""
ENV PROXY_HTTP_HOST=""
ENV PROXY_HTTP_PORT=""
ENV PROXY_HTTPS_HOST=""
ENV PROXY_HTTPS_PORT=""
ENV ENABLE_JMX_REMOTE_DEBUGGING=true

#
# Resources from build image
#
COPY --from=build /opt/jdk /opt/jdk/
COPY target/dependency $APP_HOME/lib/

RUN mkdir -p /ld_lib \
    && LMDB_NATIVE_JAR=$(find $APP_HOME/lib -type f -iname 'jffi-*-native.jar') \
    && unzip "$LMDB_NATIVE_JAR" "jni/x86_64-Linux/*" -d /ld_lib \
    && rm -f "$LMDB_NATIVE_JAR"

COPY target/rawdata-collection-client-*.jar $APP_HOME/lib

RUN mkdir -p /conf && \
    mkdir -p /source && \
    mkdir -p /database && \
    mkdir -p /avro/source && \
    mkdir -p /avro/target

ADD docker/start.sh /run.sh
RUN chmod +x /run.sh

ENV PATH=/opt/jdk/bin:$PATH
ENV JAVA_HOME=/opt/jdk
ENV LD_LIBRARY_PATH=/ld_lib/jni/x86_64-Linux/:$LD_LIBRARY_PATH

WORKDIR $APP_HOME

EXPOSE 9992

VOLUME ["/conf", "/source", "/database", "/avro", "/export"]

CMD ["/run.sh"]
