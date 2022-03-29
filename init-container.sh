#!/bin/bash
image_name=data-cleaning-app:version0.1
containerName=data-cleaning-app-v0.1

echo Delete old container...
docker rm -f $containerName
docker image rm -f $image_name

echo Build new image
docker build -t $image_name -f Dockerfile  .

echo Run new container...
docker run -d -p 5000:5000 --name $containerName $image_name