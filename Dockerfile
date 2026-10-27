FROM ubuntu:22.04
RUN apt-get update && apt-get install -y cmake g++ make python3
WORKDIR /app
COPY . .
RUN rm -rf build && mkdir build && cd build && cmake .. && cmake --build .
WORKDIR /app/build