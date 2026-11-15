# Local broker statistics and MQTT5 session persistence corrections.
# Runtime assets stay on the exact pinned image; only the broker is replaced.
# No global application revision participates in this component build.
FROM eclipse-mosquitto@sha256:75d6662dee52c947dc9dd9c29f1d8d7ffef78ddaf38001caa09755681e8bbc98 AS build
USER root
COPY session-accounting.patch /tmp/session-accounting.patch
RUN set -eux; \
    echo '29cf5445c446ad41697bbb87813e5eda73a3506235bfe1ec992e54554776252d  /tmp/session-accounting.patch' | sha256sum -c -; \
    apk --no-cache add --virtual build-deps \
        argon2-dev build-base cjson-dev cmake gnupg libedit-dev \
        libmicrohttpd-dev linux-headers openssl-dev patch sqlite-dev util-linux-dev; \
    wget https://mosquitto.org/files/source/mosquitto-2.1.2.tar.gz -O /tmp/mosq.tar.gz; \
    echo 'fd905380691ac65ea5a93779e8214941829e3d6e038d5edff9eac5fd74cbed02  /tmp/mosq.tar.gz' | sha256sum -c -; \
    mkdir -p /build/mosq /out; \
    tar --strip=1 -xf /tmp/mosq.tar.gz -C /build/mosq; \
    echo 'c93817729a6fc58f65643950096ecc8595a859b02fd0fcf33fc653d04a97425b  /build/mosq/src/sys_tree.c' | sha256sum -c -; \
    echo 'c9f61fb563109c06fa864d7b1e73cb5864f418a8853709f693854e2513c7f1ce  /build/mosq/src/persist_write.c' | sha256sum -c -; \
    patch --batch --fuzz=0 -p1 -d /build/mosq < /tmp/session-accounting.patch; \
    echo '7968b3257f2f8725a89ef19923338bc214e404e45acd5885f3be588c6f3e2785  /build/mosq/src/sys_tree.c' | sha256sum -c -; \
    echo '4b59905def250305be79bd47224f073c57c8b39deb383dbb08d735c6c17f4bc8  /build/mosq/src/persist_write.c' | sha256sum -c -; \
    make -C /build/mosq -j2 \
        CFLAGS="-Wall -O2 -I/build -DHTTP_API_DIR=\\\"/usr/share/mosquitto/dashboard\\\"" \
        WITH_ADNS=no \
        WITH_DOCS=no \
        WITH_SHARED_LIBRARIES=yes \
        WITH_SRV=no \
        WITH_STRIP=yes \
        WITH_WEBSOCKETS=yes \
        prefix=/usr \
        binary; \
    install -s -m755 /build/mosq/src/mosquitto /out/mosquitto

FROM eclipse-mosquitto@sha256:75d6662dee52c947dc9dd9c29f1d8d7ffef78ddaf38001caa09755681e8bbc98
COPY --from=build /out/mosquitto /usr/sbin/mosquitto
