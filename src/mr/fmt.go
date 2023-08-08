package mr

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

const (
	interFmt  = "mr-%d-%d"
	outputFmt = "mr-out-%d"
)

func toJsonString(object interface{}) string {
	data, _ := json.Marshal(object)
	return string(data)
}

func readFile(filename string) (string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return "", err
	}
	return string(content), nil
}
