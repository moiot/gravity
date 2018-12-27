FROM frolvlad/alpine-glibc

COPY alpine.repositories /etc/apk/repositories

RUN apk update && apk upgrade && apk add bash && apk add tzdata

RUN cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime

RUN echo "Asia/Shanghai" > /etc/timezone

COPY bin/prometheus_sd-linux-amd64 /prometheus_sd

COPY wait-for-it.sh /wait-for-it.sh

WORKDIR /