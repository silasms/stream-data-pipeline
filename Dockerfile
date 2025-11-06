FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod ./
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /bin/pipeline ./cmd/pipeline

FROM alpine:3.20
RUN addgroup -S appgroup && adduser -S appuser -G appgroup
USER appuser
COPY --from=builder /bin/pipeline /home/appuser/pipeline
EXPOSE 8080
ENTRYPOINT ["/home/appuser/pipeline"]
