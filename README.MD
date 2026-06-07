# TCP Chat Server (Go)

A lightweight multi-client LAN chat application built in Go using TCP sockets, goroutines, and channel-based concurrency.

This project was created to learn and implement real-world Go networking concepts such as TCP communication, client-server architecture, goroutines, channels, event-driven design, and concurrent programming.

---

## Features

### Multi-Client Chat

* Multiple clients can connect simultaneously.
* Each client runs in its own goroutine.
* Real-time message broadcasting.

### Username Management

* Unique usernames required.
* Duplicate usernames are automatically rejected.

### Join & Leave Notifications

* Users are notified when someone joins.
* Users are notified when someone disconnects.

### Event-Driven Architecture

The server uses channels to process events:

* User Join
* User Leave
* Chat Message
* Server Message
* Username Validation

### Channel-Based Client Manager

The server follows Go's philosophy:

> "Don't communicate by sharing memory. Share memory by communicating."

A dedicated `clientManager()` goroutine owns all client state and processes events through channels.

### Request-Response Pattern

Username validation is implemented using request-response channels, allowing goroutines to communicate without directly accessing shared state.

---

## Technologies Used

* Go (Golang)
* TCP Networking
* Goroutines
* Channels
* Event-Driven Architecture
* Concurrent Programming

---

## Architecture

```text
                    +----------------+
                    | clientManager  |
                    +----------------+
                            |
                            |
                     owns clients[]
                            ^
                            |
----------------------------------------------------
|               |               |                  |
|               |               |                  |
joinChan     leaveChan     messageChan     usernameCheckChan
|               |               |                  |
|               |               |                  |
handleClient handleClient handleClient handleClient
```

The `clientManager()` goroutine is the only component that owns and modifies the client list.

---

## Project Evolution

### V1

* Basic TCP server
* Multi-client chat

### V2

* Username support

### V3

* Disconnect handling

### V4

* Mutex-protected shared state
* Race condition prevention

### V5

* Channel-based client manager
* Event-driven architecture
* Request-response username validation
* Removal of shared-state mutex ownership

---

## Running the Project

### Start the Server

```bash
go run cmd/server/main.go
```

### Start a Client

```bash
go run cmd/client/main.go
```

Open multiple terminals and run multiple clients to test real-time communication.

---

## Example Session

```text
Enter username: bhavya

Connected to server

[SERVER] bhavya joined the chat

bhavya: hello everyone

ABC: hi

[SERVER] ABC left the chat
```

---

## Concepts Learned

* TCP Networking
* Client-Server Architecture
* Goroutines
* Channels
* Mutexes
* Event-Driven Design
* Request-Response Communication
* Concurrency Patterns
* Shared State vs Ownership Models
* Go Networking Best Practices

---

## Future Roadmap

### V6

* Private Messaging (`/msg username message`)

### V7

* Chat Rooms

### V8

* Message History

### V9

* Timestamps

### V10

* Persistent Storage

---

## Author

Bhavya 
Built as a learning project to gain hands-on experience with Go networking, concurrency, and scalable server design.
