#
# https://docs.docker.com/engine/install/debian/
#
FROM debian:buster-slim

RUN apt-get update; \
    apt-get install -y \
        apt-transport-https \
        ca-certificates \
        wget \
        curl \
        gnupg-agent \
        software-properties-common \
        sysv-rc-conf \
        procps \
        iputils-ping \
        vim;
RUN curl -fsSL https://download.docker.com/linux/debian/gpg | apt-key add - && \
    apt-key fingerprint 0EBFCD88 && \
    add-apt-repository \
       "deb [arch=amd64] https://download.docker.com/linux/debian \
       $(lsb_release -cs) \
       stable" && \
    apt-get update && \
    apt-get install -y docker-ce=5:19.03.13~3-0~debian-buster docker-ce-cli=5:19.03.13~3-0~debian-buster containerd.io && \
    rm -rf /var/lib/apt/lists/*

# Update base image to fix vulnerabilities
RUN apt update && \
    apt-get -y clean all && \
    apt-get -y update && \
    apt-get -y upgrade && \
    apt-get -y dist-upgrade

ADD scripts/docker-start.bash /start.sh
RUN chmod +x /start.sh

ADD scripts/docker-login.bash /bin/docker-login.sh
RUN chmod +x /bin/docker-login.sh

ADD scripts/docker-pull.bash /bin/docker-pull.sh
RUN chmod +x /bin/docker-pull.sh

ADD scripts/docker-save.bash /bin/docker-save.sh
RUN chmod +x /bin/docker-save.sh

VOLUME ["/secret", "/export", "/var/lib/docker"]

CMD ["/start.sh"]
