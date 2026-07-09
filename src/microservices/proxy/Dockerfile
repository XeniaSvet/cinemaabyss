FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod ./
COPY main.go ./

RUN go build -o proxy-service .

FROM alpine:3.20

WORKDIR /app

COPY --from=builder /app/proxy-service /app/proxy-service

EXPOSE 8000

CMD ["/app/proxy-service"]
