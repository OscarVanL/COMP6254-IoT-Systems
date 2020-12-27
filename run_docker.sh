#!/bin/bash
docker run -d -v $PWD:/workspace/mqtt --restart=always --name iot-mqtt-server iot-mqtt-server
