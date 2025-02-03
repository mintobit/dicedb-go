package main

import (
	"fmt"

	"github.com/dicedb/dicedb-go"
	"github.com/dicedb/dicedb-go/wire"
)

func main() {
	client, err := dicedb.NewClient("localhost", 7379)
	if err != nil {
		fmt.Println(err)
	}

	resp := client.Fire(&wire.Command{Cmd: "PING"})
	fmt.Println(resp)
}
