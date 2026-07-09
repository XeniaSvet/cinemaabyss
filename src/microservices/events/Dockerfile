FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod ./
COPY main.go ./

RUN go mod tidy
RUN go build -o events-service .

FROM alpine:3.20

WORKDIR /app

COPY --from=builder /app/events-service /app/events-service

EXPOSE 8082

CMD ["/app/events-service"]