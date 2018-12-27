FROM frolvlad/alpine-glibc

COPY alpine.repositories /etc/apk/repositories

RUN apk update && apk upgrade && apk add bash && apk add tzdata

COPY mock-linux /mock

WORKDIR /