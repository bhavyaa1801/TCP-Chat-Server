package main

import (
	"bufio"
	"fmt"
	"net"
	"sort"
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

type CreateRoomRequest struct {
	RoomName string
	Response chan bool
}

type JoinRoomResponse struct {
	Success bool
	Message string
}

type JoinRoomRequest struct {
	Username string
	RoomName string
	Response chan JoinRoomResponse
}

type LeaveRoomResponse struct {
	Success bool
	Message string
}

type LeaveRoomRequest struct {
	Username string
	RoomName string
	Response chan LeaveRoomResponse
}

type RoomListRequest struct {
	Response chan []string
}

type MyRoomsResponse struct {
	Rooms      []string
	ActiveRoom string
}

type MyRoomsRequest struct {
	Username string
	Response chan MyRoomsResponse
}

//channelss

var joinchan = make(chan Client)
var leaveChan = make(chan Client)
var messageChan = make(chan Message)
var servermsgchan = make(chan ServerMessage)
var usernamecheckchan = make(chan UsernameCheck)
var userListchan = make(chan UserListRequest)
var renameChan = make(chan RenameRequest)

// v7
var createRoomChan = make(chan CreateRoomRequest)
var joinRoomChan = make(chan JoinRoomRequest)
var leaveRoomChan = make(chan LeaveRoomRequest)
var roomListChan = make(chan RoomListRequest)
var myRoomsChan = make(chan MyRoomsRequest)

func getUsers() []string {

	response := make(chan []string)

	userListchan <- UserListRequest{
		Response: response,
	}

	return <-response
}

func leaveRoom(username, roomName string) LeaveRoomResponse {

	response := make(chan LeaveRoomResponse)

	leaveRoomChan <- LeaveRoomRequest{
		Username: username,
		RoomName: roomName,
		Response: response,
	}

	return <-response
}

func getMyRooms(username string) MyRoomsResponse {

	response := make(chan MyRoomsResponse)

	myRoomsChan <- MyRoomsRequest{
		Username: username,
		Response: response,
	}

	return <-response
}

func joinRoom(username, roomName string) JoinRoomResponse {

	response := make(chan JoinRoomResponse)

	joinRoomChan <- JoinRoomRequest{
		Username: username,
		RoomName: roomName,
		Response: response,
	}

	return <-response
}

func getRooms() []string {
	response := make(chan []string)
	roomListChan <- RoomListRequest{
		Response: response,
	}
	return <-response
}

func createRoom(roomName string) bool {

	response := make(chan bool)

	createRoomChan <- CreateRoomRequest{
		RoomName: roomName,
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
                /quit               to leave the server
                /create <channel_name>            to make a channel
                /listrooms          to list all active channels
                /join <channel_name>              to join a channel
                /leave <channel_name>              to leave a channel
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

		if msg == "/quit" {

			conn.Write([]byte("Goodbye!\n"))

			leaveChan <- client

			servermsgchan <- ServerMessage{
				Text: username + " left the chat",
			}

			fmt.Printf("%s disconnected\n", username)

			return
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
			client.Username = username

			servermsgchan <- ServerMessage{
				Text: fmt.Sprintf("%s is now known as %s",
					oldUsername,
					newUsername,
				),
			}

			continue
		}

		if msg == "/listrooms" {

			rooms := getRooms()

			conn.Write([]byte(
				fmt.Sprintf(
					"Available Rooms: %s\n",
					strings.Join(rooms, ", "),
				),
			))

			continue
		}

		if strings.HasPrefix(msg, "/create ") {

			parts := strings.SplitN(msg, " ", 2)

			if len(parts) < 2 {
				conn.Write([]byte("Usage: /create <room>\n"))
				continue
			}

			roomName := strings.TrimSpace(parts[1])
			roomName = strings.ToLower(roomName)

			success := createRoom(roomName)

			if success {
				conn.Write([]byte(
					fmt.Sprintf("Room %s created.\n", roomName),
				))
			} else {
				conn.Write([]byte(
					fmt.Sprintf("Room %s already exists.\n", roomName),
				))
			}

			continue
		}

		if strings.HasPrefix(msg, "/join ") {

			parts := strings.SplitN(msg, " ", 2)

			if len(parts) < 2 {
				conn.Write([]byte("Usage: /join <room>\n"))
				continue
			}

			roomName := strings.TrimSpace(parts[1])
			roomName = strings.ToLower(roomName)

			response := joinRoom(username, roomName)

			conn.Write([]byte(response.Message + "\n"))

			continue
		}

		if msg == "/myrooms" {

			response := getMyRooms(username)

			conn.Write([]byte(
				fmt.Sprintf(
					"My Rooms: %s\nActive Room: %s\n",
					strings.Join(response.Rooms, ", "),
					response.ActiveRoom,
				),
			))

			continue
		}

		if strings.HasPrefix(msg, "/leave ") {

			parts := strings.SplitN(msg, " ", 2)

			if len(parts) < 2 {
				conn.Write([]byte("Usage: /leave <room>\n"))
				continue
			}

			roomName := strings.TrimSpace(parts[1])
			roomName = strings.ToLower(roomName)

			response := leaveRoom(username, roomName)

			conn.Write([]byte(response.Message + "\n"))

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
	rooms := map[string]bool{
		"lobby": true,
	}

	userRooms := make(map[string]map[string]bool)

	activeRoom := make(map[string]string)

	for {
		select {
		case client := <-joinchan:

			clients = append(clients, client)
			userRooms[client.Username] = map[string]bool{
				"lobby": true,
			}

			activeRoom[client.Username] = "lobby"

			fmt.Println("[MANAGER] Added:", client.Username)

		case client := <-leaveChan:
			for i, c := range clients {
				if c.Username == client.Username {
					clients = append(clients[:i], clients[i+1:]...)
					break
				}
			}
			delete(userRooms, client.Username)
			delete(activeRoom, client.Username)
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

				senderRoom := activeRoom[msg.Sender]

				formattedmsg := fmt.Sprintf(
					"[%s] [%s] %s: %s\n",
					getTimestamp(),
					senderRoom,
					msg.Sender,
					msg.Text,
				)

				for _, c := range clients {

					if userRooms[c.Username][senderRoom] {

						c.Conn.Write([]byte(formattedmsg))
					}
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

		case req := <-roomListChan:
			var roomList []string

			for room := range rooms {
				roomList = append(roomList, room)
			}

			sort.Strings(roomList)

			req.Response <- roomList

		case req := <-myRoomsChan:

			var rooms []string

			for room := range userRooms[req.Username] {
				rooms = append(rooms, room)
			}

			sort.Strings(rooms)

			req.Response <- MyRoomsResponse{
				Rooms:      rooms,
				ActiveRoom: activeRoom[req.Username],
			}

		case req := <-renameChan:

			found := false

			for _, c := range clients {
				if c.Username == req.NewUsername {
					found = true
					break
				}
			}

			if found {
				req.Response <- false
				continue
			}

			for i := range clients {
				if clients[i].Username == req.OldUsername {
					clients[i].Username = req.NewUsername
					break
				}
			}

			userRooms[req.NewUsername] = userRooms[req.OldUsername]
			delete(userRooms, req.OldUsername)

			activeRoom[req.NewUsername] = activeRoom[req.OldUsername]
			delete(activeRoom, req.OldUsername)

			req.Response <- true

		case req := <-joinRoomChan:
			if !rooms[req.RoomName] {
				req.Response <- JoinRoomResponse{
					Success: false,
					Message: "Room does not exist.",
				}
			} else if userRooms[req.Username][req.RoomName] {
				activeRoom[req.Username] = req.RoomName
				req.Response <- JoinRoomResponse{
					Success: true,
					Message: fmt.Sprintf(
						"Already a member of %s.\nActive room: %s.",
						req.RoomName,
						req.RoomName,
					),
				}
			} else {
				userRooms[req.Username][req.RoomName] = true
				activeRoom[req.Username] = req.RoomName

				req.Response <- JoinRoomResponse{
					Success: true,
					Message: fmt.Sprintf(
						"Joined %s.\nActive room: %s.",
						req.RoomName,
						req.RoomName,
					),
				}
			}

		case req := <-leaveRoomChan:

			if req.RoomName == "lobby" {

				req.Response <- LeaveRoomResponse{
					Success: false,
					Message: "Cannot leave lobby.",
				}

				continue
			}

			if !userRooms[req.Username][req.RoomName] {

				req.Response <- LeaveRoomResponse{
					Success: false,
					Message: fmt.Sprintf(
						"You are not a member of %s.",
						req.RoomName,
					),
				}

				continue
			}

			delete(userRooms[req.Username], req.RoomName)

			if activeRoom[req.Username] == req.RoomName {

				activeRoom[req.Username] = "lobby"

				req.Response <- LeaveRoomResponse{
					Success: true,
					Message: fmt.Sprintf(
						"Left %s.\nActive room: lobby.",
						req.RoomName,
					),
				}

			} else {

				req.Response <- LeaveRoomResponse{
					Success: true,
					Message: fmt.Sprintf(
						"Left %s.",
						req.RoomName,
					),
				}
			}

		case req := <-createRoomChan:

			if rooms[req.RoomName] {
				req.Response <- false
			} else {
				rooms[req.RoomName] = true
				req.Response <- true
			}
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
