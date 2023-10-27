FROM debian:11-slim AS build
COPY . /app
WORKDIR /app
RUN apt update && \
    apt install --no-install-suggests --no-install-recommends -y \
        python3-pip git zip unzip curl docker.io && \
    pip3 install --no-cache-dir --upgrade pip --disable-pip-version-check -r requirements.txt && \
    curl -Lo /usr/bin/yq https://github.com/mikefarah/yq/releases/download/v4.13.4/yq_linux_amd64 && \
    chmod +x /usr/bin/yq && \
    apt clean && \
    rm -rf /var/lib/apt/lists/*

FROM gcr.io/distroless/python3-debian11:debug
COPY --from=build   /app                        /app
COPY --from=build   /usr/local/bin/flask        /usr/local/bin/flask
COPY --from=build   /usr/local/lib/python3.9    /usr/local/lib/python3.9
COPY --from=build   /usr/bin/git                /usr/bin/git
COPY --from=build   /usr/bin/zip                /usr/bin/zip
COPY --from=build   /usr/bin/unzip              /usr/bin/unzip
COPY --from=build   /usr/bin/curl               /usr/bin/curl
COPY --from=build   /usr/bin/docker             /usr/bin/docker
COPY --from=build   /usr/bin/yq                 /usr/bin/yq
COPY --from=build   /usr/lib/x86_64-linux-gnu   /usr/lib/x86_64-linux-gnu

RUN chmod +x /usr/bin/curl

WORKDIR /app
RUN adduser -D nutbodyslam053
USER nutbodyslam053
EXPOSE 5000
ENTRYPOINT ["flask", "run", "--host=0.0.0.0"]