# Gateway Service
###### org.hypertrace.gateway.service

[![CircleCI](https://circleci.com/gh/hypertrace/gateway-service.svg?style=svg)](https://circleci.com/gh/hypertrace/gateway-service)

An entry service that acts as a single access point for querying data from other services like entity-service, query-service, Attribute service. 

## How do we use Gateway service?

| ![space-1.jpg](https://hypertrace-docs.s3.amazonaws.com/HT-query-arch.png) | 
|:--:| 
| *Hypertrace Query Architecture* |


Gateway service routes queries to corresponding downstream service based on the source of attributes and then does appropriate type conversion of data returned by downstream services.

## Building locally
The Gateway service uses gradlew to compile/install/distribute. Gradle wrapper is already part of the source code. To build Gateway Service, run:

```
./gradlew clean build dockerBuildImages
```

## Docker Image Source:
- [DockerHub > Gateway service](https://hub.docker.com/r/hypertrace/gateway-service)
