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

func writeFile(filename string, content string) error {
	f, err := ioutil.TempFile("", "mr")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	_, err = f.Write([]byte(content))
	if err != nil {
		return err
	}
	err = f.Close()
	if err != nil {
		return err
	}
	err = os.Rename(f.Name(), filename)
	if err != nil {
		return err
	}
	return nil
}
