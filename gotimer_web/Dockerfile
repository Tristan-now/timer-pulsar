FROM ubuntu:22.04

RUN mkdir "/app"
WORKDIR "/app"

COPY main /app/app
COPY conf.yml /app/conf.yml
EXPOSE 8092
ENTRYPOINT ["./app"]