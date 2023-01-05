# TimeCapsule

[![Go Report](https://goreportcard.com/badge/github.com/nekomeowww/timecapsule)](https://goreportcard.com/report/github.com/nekomeowww/timecapsule)
[![Unit Tests](https://github.com/nekomeowww/timecapsule/actions/workflows/unittest.yaml/badge.svg)](https://github.com/nekomeowww/timecapsule/actions/workflows/unittest.yaml)

TimeCapsule is a Redis based task scheduling module written in Golang. It is designed to be used as a library in your own application. It is not a standalone application.

As the name suggests, it is inspired by the concept of **time capsule** which is designed to be used in a similar way.
You can schedule tasks to be executed at a specific time in the future just like you buried a time capsule, and use this module to periodically look for tasks that are due to be executed from the dataloader. Once a task is due, it will be executed and removed from the dataloader.

Features:

- [x] Golang 1.18 Generic support
- [X] Customizable dataloader for performing task scheduling and execution

## Installation

```bash
go get github.com/nekomeowww/timecapsule
```

## Usage

```go
package main

import (
    "fmt"
    "time"

    "github.com/nekomeowww/timecapsule"
)

func main() {
    // initialize redis dataloader
    redisDataloder := NewRedisDataloader[string]("some/task/key", redis.NewClient(&redis.Options{
        Addr: net.JoinHostPort("localhost", "6379"),
        Password: "123456",
    }))

    // create a time capsule digger, pass 250*time.Millisecond into parameter as the interval
    digger := NewDigger[string](redisDataloder, 250*time.Millisecond)

    // set task handler function
    digger.SetHandler(func(capsule *TimeCapsule[string]) {
        fmt.Println("task", capsule.Payload, "is due") // print a message
    })

    // start digging in a seperated goroutine
    go digger.Start()
    // defer stop digging
    defer digger.Stop()

    // Bury a time capsule
    err = digger.BuryFor("this is a time capsule", time.Second)
    if err != nil {
        logger.Error(err)
    }

    err = digger.BuryUntil("this is a time capsule", time.Now().UTC().Add(time.Second).UnixMilli())
    if err != nil {
        logger.Error(err)
    }
}
```
