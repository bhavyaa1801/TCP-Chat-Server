package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
)

type Client struct {
	Username string
	Conn     net.Conn
}

type Message struct {
    Sender string
    Text string
}

type ServerMessage struct{
	Text string
}

//req-res model for channel 
type UsernameCheck struct {
	Username string
	Response chan bool
}



//channelss

var joinchan = make(chan Client)
var leaveChan = make(chan Client)
var messageChan = make(chan Message)
var servermsgchan = make(chan ServerMessage)
var usernamecheckchan = make(chan UsernameCheck)

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

    responseChan := make(chan bool)

	usernamecheckchan <- UsernameCheck{
		Username: username,
		Response: responseChan,
	}

	exists := <-responseChan

	if exists {
		conn.Write([]byte("Username already taken\n"))
		return
	}

	client := Client{
		Username: username,
		Conn:     conn,
	}

	// clients = append(clients, client)
	joinchan <- client

	fmt.Printf("%s joined the chat\n", username)

	// Notify everyone
	// broadcast("[SERVER] " + username + " joined the chat\n")
	servermsgchan <- ServerMessage{
		Text: "[SERVER]" + username + "joined the chat\n",
	}

	// Chat loop
	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			// removeClient(username)
			leaveChan <- client
		    leaveMsg := fmt.Sprintf("[SERVER] %s left the chat\n", username)

			servermsgchan <- ServerMessage{
				Text: leaveMsg,
			}

			fmt.Printf("%s disconnected\n", username)
			return
		}

		msg = strings.TrimSpace(msg)

		// formattedMsg := fmt.Sprintf("%s: %s\n", username, msg)
		// fmt.Print(formattedMsg)
		// broadcast(formattedMsg)


		fmt.Printf("%s: %s\n", username, msg)
		messageChan <- Message{
			Sender: username,
			Text: msg,
		}
	}
}

//client manager for chann

func clientManager(){
	
    var clients []Client
	for{
		select{
		case client := <-joinchan:

	    	
	    	clients = append(clients , client)
		    
		    fmt.Println("[MANAGER] Added:", client.Username)
		
		case client := <-leaveChan:
			for i,c := range clients{
				if c.Username == client.Username {
					clients = append(clients[:i],clients[i+1:]...)
					break
				}
			}
            fmt.Println("[MANAGER] Removed:", client.Username)

		case msg := <-messageChan:
            formattedmsg := fmt.Sprintf(
				"%s: %s\n",
                msg.Sender,
                msg.Text,
			)

			for _,c := range clients {
				c.Conn.Write([]byte(formattedmsg))
			}

		case msg := <-servermsgchan:
			for _,c := range clients {
				c.Conn.Write([]byte(msg.Text))
			}

		case req := <-usernamecheckchan:
			found := false

			for _,client := range clients{
				if client.Username == req.Username {
					found = true
					break
				}
			}
			req.Response <- found

		}
	}
}

func main() {

	go clientManager()

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






//                 +----------------+
//                 | clientManager  |
//                 +----------------+
//                         |
//                         |
//                  owns clients[]
//                         ^
//                         |
//     -----------------------------------------
//     |                  |                    |
//     |                  |                    |
//  joinChan          leaveChan          messageChan
//     ^                  ^                    ^
//     |                  |                    |
// handleClient()   handleClient()      handleClient()

