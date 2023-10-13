package utils

import (
	"fmt"
	"math/rand"
	"time"
)

func GetTableNameForRand(tableName string) string {
	rand.Seed(time.Now().UnixNano())
	return fmt.Sprintf("%s_%s_%d", tableName, time.Now().Format("20060102_150405"), rand.Intn(100))
}

func Contains(arr []string, elem string) bool {
	for _, e := range arr {
		if e == elem {
			return true
		}
	}
	return false
}
