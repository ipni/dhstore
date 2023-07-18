FROM golang:1.19-bullseye as build

ARG FDB_VERSION='7.3.7'
ARG FDB_CLIENTS_DEB_SHA256_SUM='1b620971319c3ad149f2fb09b2fed639fb558120a296538133c4f2358836e983'
RUN wget "https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/foundationdb-clients_${FDB_VERSION}-1_amd64.deb" && \
    echo "${FDB_CLIENTS_DEB_SHA256_SUM}  foundationdb-clients_${FDB_VERSION}-1_amd64.deb" | sha256sum --check && \
    dpkg --force-all -i foundationdb-clients_${FDB_VERSION}-1_amd64.deb

WORKDIR /go/src/dhstore
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o /go/bin/dhstore ./cmd/dhstore

FROM gcr.io/distroless/base-debian11

COPY --from=build /go/bin/dhstore /usr/bin/
COPY --from=build /usr/lib/libfdb_c.so \
                  /lib/x86_64-linux-gnu/liblzma.so.5 \
                  /lib/x86_64-linux-gnu/libz.so.1 \
                  /usr/lib/
COPY --from=build /usr/include/foundationdb /usr/include/foundationdb

ENTRYPOINT ["/usr/bin/dhstore"]
