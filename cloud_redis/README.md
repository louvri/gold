# Usage  

## Installation
```
go get github.com/gomodule/redigo
go get github.com/louvri/gold
```

## Initiate Object
### Import Object
```
import (
    cloudRedis github.com/louvri/gold/cloud_redis
)
```
### Create Object
```
host := "localhost"
port := "6379"
ttl := 1800 // in seconds
redis := cloudRedis.New(host, port, ttl)
```  

## Distributed Locking
### Acquire Lock
```
name := "order-write-processing"
secret := fmt.Sprintf("%d", time.Now().UnixNano())
locked, _ := cloudRedis.Lock(name, secret)
if locked {
    // do work here
}
```  

### Release Lock
```
name := "order-write-processing"
secret := fmt.Sprintf("%d", time.Now().UnixNano())
unlocked, _ := cloudRedis.Lock(name, secret)
if unlocked {
    // do work here if needing something after unlocking
}
```

Please take a note that the name and secret used should be the same when locking and unlocking.

## Caching Data
### Set Cache Data
```
name := "master-data-area"
value := []byte(name)
err := cloudRedis.SetData(name, value)
```
you could also set customized TTL with:
```
ttl := 1800 // in seconds
err := cloudRedis.SetData(name, value, ttl)
```

### Get Cache Data
```
name := "master-data-area"
data, err := cloudRedis.GetData(name)
var parsedData map[string]interface{}
err := json.Unmarshal(data, &parsedData)
```
