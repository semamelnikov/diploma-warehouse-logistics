FROM openjdk:8

LABEL maintainer="sema.melnikov@gmail.com"

VOLUME /tmp

EXPOSE 8080

ARG JAR_FILE=target/transaction-service-1.0.jar

ADD ${JAR_FILE} transaction-service.jar

CMD ["bash", "--version"]
