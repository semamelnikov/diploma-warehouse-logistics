FROM openjdk:8

LABEL maintainer="sema.melnikov@gmail.com"

VOLUME /tmp

EXPOSE 8080

ARG JAR_FILE=target/delivery-service-1.0.jar

ADD ${JAR_FILE} delivery-service.jar

CMD ["bash", "--version"]
