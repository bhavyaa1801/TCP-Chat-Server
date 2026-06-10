package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"time"
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

type UserListRequest struct {
	Response chan []string
}

type RenameRequest struct {
	OldUsername string
	NewUsername string
	Response    chan bool
}

//channelss

var joinchan = make(chan Client)
var leaveChan = make(chan Client)
var messageChan = make(chan Message)
var servermsgchan = make(chan ServerMessage)
var usernamecheckchan = make(chan UsernameCheck)
var userListchan = make(chan UserListRequest)
var renameChan = make(chan RenameRequest)

func getUsers() []string {

	response := make(chan []string)

	userListchan <- UserListRequest{
		Response: response,
	}

	return <-response
}

func getTimestamp() string {
	return time.Now().Format("15:04:05")
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

	joinchan <- client

	fmt.Printf("%s joined the chat\n", username)

	servermsgchan <- ServerMessage{
		Text: username + " joined the chat\n",
	}

	// Chat loop
	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			// removeClient(username)
			leaveChan <- client
			leaveMsg := fmt.Sprintf("%s left the chat\n", username)

			servermsgchan <- ServerMessage{
				Text: leaveMsg,
			}

			fmt.Printf("%s disconnected\n", username)
			return
		}

		msg = strings.TrimSpace(msg)

		if msg == "/help" {

			const helpMessage = `
                Available Commands:
                /users              Show online users
                /msg <user> <msg>   Send private message
                /help               Show commadss
				/rename             to rename your old name
            `

			conn.Write([]byte(helpMessage))

			continue
		}

		if msg == "/users" {

			users := getUsers()

			conn.Write([]byte(
				fmt.Sprintf(
					"Online Users: %s\n",
					strings.Join(users, ", "),
				),
			))

			continue
		}

		if strings.HasPrefix(msg, "/rename ") {

			parts := strings.SplitN(msg, " ", 2)

			if len(parts) < 2 {
				conn.Write([]byte("Usage: /rename <new_username>\n"))
				continue
			}

			newUsername := strings.TrimSpace(parts[1])
			newUsername = strings.ToLower(newUsername)

			responseChan := make(chan bool)

			renameChan <- RenameRequest{
				OldUsername: username,
				NewUsername: newUsername,
				Response:    responseChan,
			}

			success := <-responseChan

			if !success {
				conn.Write([]byte("Username already taken\n"))
				continue
			}

			oldUsername := username
			username = newUsername

			servermsgchan <- ServerMessage{
				Text: fmt.Sprintf("%s is now known as %s",
					oldUsername,
					newUsername,
				),
			}

			continue
		}

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

				timestamp := getTimestamp()

				found := false
				for _, c := range clients {
					if c.Username == msg.Receiver {

						found = true

						fmt.Fprintf(
							c.Conn,
							"[%s] [PM from %s] %s\n",
							timestamp,
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
								"[%s] [PM to %s] %s\n",
								timestamp,
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
					"[%s] %s: %s\n",
					getTimestamp(),
					msg.Sender,
					msg.Text,
				)

				for _, c := range clients {
					c.Conn.Write([]byte(formattedmsg))
				}
			}

		case msg := <-servermsgchan:

			timestamp := getTimestamp()

			formatted := fmt.Sprintf(
				"[%s] [SERVER] %s\n",
				timestamp,
				msg.Text,
			)

			for _, c := range clients {
				c.Conn.Write([]byte(formatted))
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

		case req := <-userListchan:

			var users []string
			for _, c := range clients {
				users = append(users, c.Username)
			}
			req.Response <- users

		case req := <-renameChan:
			for _, c := range clients {
				if c.Username == req.NewUsername {
					req.Response <- false
					continue
				}
			}
			for i := range clients {
				if clients[i].Username == req.OldUsername {
					clients[i].Username = req.NewUsername
					break
				}
			}

			req.Response <- true

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
