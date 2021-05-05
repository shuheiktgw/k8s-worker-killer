FROM golang:1.16 AS builder

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./
COPY cloudprovider cloudprovider/
COPY config config/
COPY core core/
COPY utils utils/
COPY version version/

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w"

FROM gcr.io/distroless/static:latest-amd64

COPY --from=builder /build/k8s-worker-killer /k8s-worker-killer
ENTRYPOINT ["/k8s-worker-killer"]