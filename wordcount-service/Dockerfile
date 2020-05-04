FROM openjdk:8

LABEL maintainer="sema.melnikov@gmail.com"

VOLUME /tmp

ARG JAR_FILE=target/wordcount-service-1.0-jar-with-dependencies.jar

ADD ${JAR_FILE} wordcount-service.jar

CMD ["bash", "--version"]
