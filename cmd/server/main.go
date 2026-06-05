package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
)

type Client struct {
	Username string
	Conn     net.Conn
}

var clients []Client
var mu sync.Mutex

func usernameExists(username string) bool {
	mu.Lock()
	defer mu.Unlock()

	for _, client := range clients {
		if client.Username == username {
			return true
		}
	}
	return false
}

func removeClient(username string){
	mu.Lock()
	defer mu.Unlock()

	for i,client := range clients{
		if client.Username == username{
			clients = append(clients[:i],clients[i+1:]...,)
			return
		}
	}
}

func broadcast(msg string){
	mu.Lock()
	clientCopy := make([]Client, len(clients))
	copy(clientCopy, clients)

	mu.Unlock()

	for _,c := range clientCopy {
		c.Conn.Write([]byte(msg))
	}
}

func handleClient(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)

	// Read username first
	username, err := reader.ReadString('\n')
	if err != nil {
		return
	}

	username = strings.TrimSpace(username)
	username = strings.ToLower(username)

	if usernameExists(username) {
		conn.Write([]byte("Username already taken\n"))
		return
	}

	client := Client{
		Username: username,
		Conn:     conn,
	}

	clients = append(clients, client)

	fmt.Printf("%s joined the chat\n", username)

	// Notify everyone
	broadcast("[SERVER] " + username + " joined the chat\n")

	// Chat loop
	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			removeClient(username)
		    leaveMsg := fmt.Sprintf("[SERVER] %s left the chat\n", username)

			broadcast(leaveMsg)

			fmt.Printf("%s disconnected\n", username)
			return
		}

		msg = strings.TrimSpace(msg)

		formattedMsg := fmt.Sprintf("%s: %s\n", username, msg)

		fmt.Print(formattedMsg)

		broadcast(formattedMsg)
	}
}

func main() {
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		panic(err)
	}
	defer listener.Close()

	fmt.Println("Server started on port 8080")

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}

		go handleClient(conn)
	}
}




//version 5 plan
//                  joinChan
// handleClient(A) ---------\
//                            \
// handleClient(B) -----------> clientManager
//                            /
// handleClient(C) ---------/

//                  leaveChan
//                  messageChan
// phele handleclient directly slice pr act kr re the isiliye humne mutex lgaya 
//but with chanels ye pblm hi ni ayegi.... 1 hi goroutine slice pr updates dalega


//Current version:

// Many goroutines
// touch same data

// Need Mutex

// V5:

// Many goroutines
// send messages

// One goroutine
// owns data