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
	Sender   string
	Text     string
	Receiver string
	IsPM     bool
}

type ServerMessage struct {
	Text string
}

// req-res model for channel
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
		Text: "[SERVER] " + username + " joined the chat\n",
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

		if strings.HasPrefix(msg, "/msg ") {
			parts := strings.SplitN(msg, " ", 3)

			if len(parts) < 3 {
				conn.Write([]byte("Usage: /msg <username> <message>\n"))
				continue
			}
			receiver := parts[1]
			text := parts[2]

			messageChan <- Message{
				Sender:   username,
				Receiver: receiver,
				Text:     text,
				IsPM:     true,
			}
			continue
		}
		messageChan <- Message{
			Sender:   username,
			Receiver: "",
			Text:     msg,
			IsPM:     false,
		}
	}
}

//client manager for chann

func clientManager() {

	var clients []Client
	for {
		select {
		case client := <-joinchan:

			clients = append(clients, client)

			fmt.Println("[MANAGER] Added:", client.Username)

		case client := <-leaveChan:
			for i, c := range clients {
				if c.Username == client.Username {
					clients = append(clients[:i], clients[i+1:]...)
					break
				}
			}
			fmt.Println("[MANAGER] Removed:", client.Username)

		case msg := <-messageChan:

			if msg.IsPM {
				//private msg
				found := false
				for _, c := range clients {
					if c.Username == msg.Receiver {

						found = true

						fmt.Fprintf(
							c.Conn,
							"[PM from %s] %s\n",
							msg.Sender,
							msg.Text,
						)
						break
					}
				}
				if found {
					for _, c := range clients {
						if c.Username == msg.Sender {
							fmt.Fprintf(
								c.Conn,
								"[PM to %s] %s\n",
								msg.Receiver,
								msg.Text,
							)
							break
						}
					}
				} else {
					for _, c := range clients {

						if c.Username == msg.Sender {

							fmt.Fprintf(
								c.Conn,
								"[SERVER] User %s not found\n",
								msg.Receiver,
							)

							break
						}
					}
				}

			} else {
				formattedmsg := fmt.Sprintf(
					"%s: %s\n",
					msg.Sender,
					msg.Text,
				)

				for _, c := range clients {
					c.Conn.Write([]byte(formattedmsg))
				}
			}

		case msg := <-servermsgchan:
			for _, c := range clients {
				c.Conn.Write([]byte(msg.Text))
			}

		case req := <-usernamecheckchan:
			found := false

			for _, client := range clients {
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
