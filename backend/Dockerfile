FROM golang:1.25-alpine AS builder

WORKDIR /app

# зависимости отдельно для кэша
COPY go.mod go.sum ./
RUN go mod download

# исходники
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o server ./cmd/service

FROM alpine:3.20
WORKDIR /app

COPY --from=builder /app/server /app/server

EXPOSE 8080
ENV PORT=8080

CMD ["/app/server"]