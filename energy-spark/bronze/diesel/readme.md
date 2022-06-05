# Building your spark image
```sh
docker build . -t load-to-bronze-diesel:3.1.1

docker tag load-to-bronze-diesel:3.1.1 <your-docker-hub>/load-to-bronze-diesel:3.1.1

docker push <your-docker-hub>/load-to-bronze-diesel:3.1.1
```

# Apply the yaml file to your cluster

- Modify any argument needed

