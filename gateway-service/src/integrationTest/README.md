<h4>How do I run the integration tests locally?</h4>

- Bring up a mongodb container or port forward to the mongo container in e2e setup

    `docker run --name mongo-local -p 27017:27017 -d mongo`
    or
    `kubectl port-forward mongo-0 27017:27017 -n traceable`
    
- Bring up query-service from e2e setup and port-forward to them
    
    `kubectl port-forward svc/query-service 8090:8090 -n traceable`
    
- Run the integration tests from IDE