FROM jptmoore/alpine-baseimage-aarch64

# add the code
ADD src src
ADD test test
ADD utils utils
RUN sudo chown -R databox:nogroup src
RUN sudo chown -R databox:nogroup test
RUN sudo chown -R databox:nogroup utils

# add the build script
ADD build.sh .

# setup ocaml
RUN sudo chmod +x build.sh && sync \
&& ./build.sh \
&& rm -rf /home/databox/src \
&& rm -rf /home/databox/test \
&& rm -rf /home/databox/utils \
&& rm -rf /home/databox/.opam \
&& rm -rf /home/databox/opam-repository \
&& sudo apk del .build-deps 

FROM resin/aarch64-alpine:3.5

USER root
WORKDIR /home/databox/

COPY --from=0 /home/databox/ .
# runtime dependencies
RUN apk update && apk upgrade \
&& apk add libsodium gmp zlib libzmq

VOLUME /database

EXPOSE 5555
EXPOSE 5556

