FROM golang:1.24.0-alpine AS builder

WORKDIR /app
COPY ./src .

RUN CGO_ENABLED=0 go build -o server

RUN apk add -U --no-cache ca-certificates

FROM scratch

COPY --from=builder /app/server /app/server

COPY --from=alpine /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

CMD ["/app/server"]