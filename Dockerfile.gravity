FROM frolvlad/alpine-glibc

RUN apk update && apk upgrade && apk add bash && apk add tzdata

RUN cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && echo "Asia/Shanghai" > /etc/timezone

WORKDIR /

COPY bin/gravity-linux-amd64 /gravity