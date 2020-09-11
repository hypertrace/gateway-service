# Gateway Service
###### org.hypertrace.gateway.service

[![CircleCI](https://circleci.com/gh/hypertrace/gateway-service.svg?style=svg)](https://circleci.com/gh/hypertrace/gateway-service)

An entry service that acts as a single access point for querying data from other services like entity-service, query-service, Attribute service. 

## Description

| ![space-1.jpg](https://hypertrace-docs.s3.amazonaws.com/hypertrace-query-arch.png) | 
|:--:| 
| *Hypertrace Query Architecture* |


Gateway service routes queries to corresponding downstream service based on the source of attributes and then does appropriate type conversion of data returned by downstream services.

## Building locally
The Gateway service uses gradlew to compile/install/distribute. Gradle wrapper is already part of the source code. To build Gateway Service, run:

```
./gradlew dockerBuildImages
```

## Testing

### Running unit tests
Run `./gradlew test` to execute unit tests. 


### Testing image

You can test the image you built after modification by running docker-compose or helm setup. 

#### docker-compose

`Note:` 
- For docker-compose setup we use [hypertrace-service](https://github.com/hypertrace/hypertrace-service) which contains `gateway-service`. So to test changes with docker-compose you can checkout branch with your changes for `gateway-service` in [hypertrace-service](https://github.com/hypertrace/hypertrace-service) repo and build a new image to use with docker-compose setup. [This issue will be gone ones we switch to using macro-repo]

After that just Change the tag for `hypertrace-service` from `:main` to `:test` in [docker-compose file](https://github.com/hypertrace/hypertrace/blob/main/docker/docker-compose.yml) like this.

```yaml
  hypertrace-service:
    image: hypertrace/hypertrace-service:test
    container_name: hypertrace-service
    ...
```

and then run `docker-compose up` to test the setup.

## Docker Image Source:
- [DockerHub > Gateway service](https://hub.docker.com/r/hypertrace/gateway-service)
