package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)



func receiveMessages(conn net.Conn) {
	reader := bufio.NewReader(conn)

	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Disconnected from server")
			os.Exit(0)
		}

		fmt.Print(msg)
	}
}

func sendMessages(conn net.Conn) {
	reader := bufio.NewReader(os.Stdin)

	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println(err)
			return
		}

		_, err = conn.Write([]byte(msg))
		if err != nil {
			fmt.Println(err)
			return
		}
	}
}

func main() {
	var username string

	fmt.Print("Enter username: ")
	fmt.Scanln(&username)

	username = strings.TrimSpace(username)
	username = strings.ToLower(username)

	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// Send username first
	conn.Write([]byte(username + "\n"))

	fmt.Println("Connected to server")

	go receiveMessages(conn)

	sendMessages(conn)
}