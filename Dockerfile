FROM golang:1.19-bullseye as build

WORKDIR /go/src/dhstore

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /go/bin/dhstore ./cmd/dhstore

FROM gcr.io/distroless/static-debian11
COPY --from=build /go/bin/dhstore /usr/bin/

ENTRYPOINT ["/usr/bin/dhstore"]
