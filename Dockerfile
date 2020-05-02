FROM openjdk:8

LABEL maintainer="sema.melnikov@gmail.com"

VOLUME /tmp

ARG JAR_FILE=target/wordcount-1.0-jar-with-dependencies.jar

ADD ${JAR_FILE} wordcount-1.0-jar-with-dependencies.jar

CMD ["bash", "--version"]
