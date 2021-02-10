# Gateway Service

An entry service that acts as a single access point for querying data from other services like entity-service, query-service, Attribute service. 

## Description

| ![space-1.jpg](https://hypertrace-docs.s3.amazonaws.com/arch/ht-query.png) | 
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

To test your image using the docker-compose setup follow the steps:

- Commit you changes to a branch say `gateway-service-test`.
- Go to [hypertrace-service](https://github.com/hypertrace/gateway-service) and checkout the above branch in the submodule.
```
cd gateway-service && git checkout gateway-service-test && cd ..
```
- Change tag for `hypertrace-service` from `:main` to `:test` in [docker-compose file](https://github.com/hypertrace/hypertrace/blob/main/docker/docker-compose.yml) like this.

```yaml
  hypertrace-service:
    image: hypertrace/hypertrace-service:test
    container_name: hypertrace-service
    ...
```
- and then run `docker-compose up` to test the setup.

### Helm setup
Add image repository and tag in values.yaml file [here](https://github.com/hypertrace/hypertrace/blob/main/kubernetes/platform-services/values.yaml) like below and then run `./hypertrace.sh install` again and you can test your image!

```yaml
gateway-service:
  image:
    repository: "hypertrace/gateway-service"
    tagOverride: "test"
 ```

## Docker Image Source:
- [DockerHub > Gateway service](https://hub.docker.com/r/hypertrace/gateway-service)
