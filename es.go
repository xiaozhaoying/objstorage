package main

import (
	"fmt"

	"./tools"
)

var (
	ELASTIC_URL = "http://192.168.10.150:9200"
	INDEX       = "objstorage"
)

func main() {
	es := tools.NewES(ELASTIC_URL, INDEX)
	res := es.IsExists("file", "7cfc495f868bfe9ed036c16f176d68e8")
	fmt.Printf("%+v\n", res)
}
