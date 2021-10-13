ARG debian_buster_image_tag=8-jre-slim
FROM openjdk:${debian_buster_image_tag}

# -- Layer: OS + Python & Pip 3.9

ARG shared_workspace=/opt/workspace
ARG transformater=/opt/transformater

RUN mkdir -p ${shared_workspace} && \
    mkdir -p ${transformater} && \
    apt-get update -y && \
    apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

ENV SHARED_WORKSPACE=${shared_workspace}
ENV TRANSFORMATER=${shared_workspace}

# -- Runtime

VOLUME ${shared_workspace}
VOLUME ${transformater}
CMD ["bash"]
