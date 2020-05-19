FROM rappdw/docker-java-python:zulu1.8.0_222-python3.6.9

LABEL maintainer="sema.melnikov@gmail.com"

VOLUME /tmp

EXPOSE 8080

ARG JAR_FILE=target/delivery-service-1.0.jar

ADD ${JAR_FILE} delivery-service.jar

RUN pip install --upgrade pip && \
    pip install --no-cache-dir psycopg2

CMD ["pip", "--version"]
