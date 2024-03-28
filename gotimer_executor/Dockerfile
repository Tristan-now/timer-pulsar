FROM golang:1.22 AS builder

# 构建可执行文件
#ENV CGO_ENABLED 1
ENV GOPROXY https://goproxy.cn,direct
#RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.aliyun.com/g' /etc/apk/repositories

WORKDIR /build
#ADD go.mod .
#ADD go.sum .
#ADD main.go .
ADD . .
#RUN go mod tidy
RUN go build -o main


FROM ubuntu:22.04
WORKDIR /app
COPY --from=builder /build/main /app
COPY --from=builder /build/conf.yml /app
CMD ["./main"]