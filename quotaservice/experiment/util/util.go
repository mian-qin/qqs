package qqsutil

import (
	"fmt"
	"time"
)

func LogError(err error) {
	println("[Error]" + err.Error())
}

func Log(s string, a ...interface{}) {
	str := s
	if len(a) > 0 {
		str = fmt.Sprintf(s, a...)
	}

	fmt.Printf("[%v]%s\n", time.Now(), str)
}
