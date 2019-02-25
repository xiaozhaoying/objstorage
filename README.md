## Requirement
- ### go
 - go >= v1.8.5
 - arch amd64
- ### other
 - nsq >= v1.1.0
 - elasticsearch >= v5.6.10

## Install
```
git clone https://github.com/xiaozhaoying/objstorage.git
cd objstorage && go build -o apiserv api/*
cd objstorage && go build -o dataserv data/*
```

## Usage
```shell
start nsqd
start elasticsearch
./apiserv
./dataserv
```
