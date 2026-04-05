FROM golang:1.23-alpine AS builder

RUN apk add --no-cache git

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /bin/instgraph-server ./cmd/instgraph-server

FROM alpine:3.19
RUN apk add --no-cache ca-certificates
COPY --from=builder /bin/instgraph-server /usr/local/bin/instgraph-server

EXPOSE 4317 9090 9091
VOLUME /data

ENTRYPOINT ["instgraph-server"]
CMD ["--data", "/data"]
