FROM ubuntu:focal as base

ARG LOGIN_USER='autotest'
ARG WORKSPACE='/workspace'
ARG WORKERS='autotest1 autotest2 autotest3 autotest4'
ARG ADDITIONAL_REQUIREMENTS

ENV DEBIAN_FRONTEND=noninteractive
ENV WORKSPACE='/workspace'

RUN apt update -y && apt -y install sudo git python3 python3-pip $ADDITIONAL_REQUIREMENTS

COPY . /app

RUN python3 -m pip install wheel -r /app/requirements.txt

RUN useradd -ms /bin/bash $LOGIN_USER && \
    usermod -aG sudo $LOGIN_USER && \
    for worker in $WORKERS; do \
        adduser --disabled-login --no-create-home "${worker}" && \
        echo "$LOGIN_USER ALL=(${worker}) NOPASSWD:ALL" | EDITOR="tee -a" visudo && \
        usermod -aG "${worker}" $LOGIN_USER; \
    done

RUN mkdir -p ${WORKSPACE} && chown ${LOGIN_USER} ${WORKSPACE}

WORKDIR /home/${LOGIN_USER}

FROM base as dev

ENTRYPOINT ["/app/.dockerfiles/entrypoint-dev.sh"]

CMD ["python3", "/app/start_stop.py", "start", "--log=-", "--error_log=-",  "--nodaemon"]

RUN rm -rf /app/*

USER ${LOGIN_USER}

FROM base as prod

ENTRYPOINT ["/app/.dockerfiles/entrypoint-prod.sh"]

CMD ["python3", "/app/start_stop.py", "start", "--log=-", "--error_log=-",  "--nodaemon"]

USER ${LOGIN_USER}
