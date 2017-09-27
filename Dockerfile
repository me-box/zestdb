FROM alpine:latest

RUN apk update && apk upgrade \
 && apk add sudo \
 && adduser -S zest \
 && echo 'zest ALL=(ALL:ALL) NOPASSWD:ALL' > /etc/sudoers.d/zest \
 && chmod 440 /etc/sudoers.d/zest \
 && chown root:root /etc/sudoers.d/zest \
 && sed -i.bak 's/^Defaults.*requiretty//g' /etc/sudoers

USER zest
WORKDIR /home/zest

# add the code
ADD src src
ADD test test
RUN sudo chown -R zest:nogroup src
RUN sudo chown -R zest:nogroup test

# add the build script
ADD build.sh .

# setup ocaml
RUN sudo apk add --no-cache --virtual .build-deps alpine-sdk bash ncurses-dev m4 perl gmp-dev zlib-dev libsodium-dev opam zeromq-dev \
&& opam init \ 
&& opam pin add -n sodium https://github.com/me-box/ocaml-sodium.git#with_auth_hmac256 \
&& opam install -y reason lwt tls sodium macaroons ezirmin bitstring ppx_bitstring uuidm lwt-zmq \
&& sudo chmod +x build.sh && sync \
&& ./build.sh \
&& rm -rf /home/zest/src \
&& rm -rf /home/zest/test \
&& rm -rf /home/zest/.opam \
&& sudo apk del .build-deps

FROM alpine:latest

USER root
WORKDIR /app/zest/

COPY --from=0 /home/zest/ .
# runtime dependencies
RUN apk update && apk upgrade \
&& apk add libsodium gmp zlib libzmq

VOLUME /database

EXPOSE 5555
EXPOSE 5556

