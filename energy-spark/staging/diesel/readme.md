# Building your spark image
```sh
docker build . -t load-to-staging-diesel:3.1.1

docker tag load-to-staging-diesel:3.1.1 <your-docker-hub>/load-to-staging-diesel:3.1.1

docker push <your-docker-hub>/load-to-staging-diesel:3.1.1
```

# JARS list
- aws-java-sdk-bundle-1.12.228.jar
- delta-core_2.12-1.0.1.jar
- hadoop-aws-3.2.0.jar
- postgresql-42.2.19.jar

# Apply the yaml file to your cluster



