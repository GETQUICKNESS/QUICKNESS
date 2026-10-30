package main

import (
	"fmt"
	"bufio"
	"strings"
	"github.com/gorilla/websocket"
	"log"
	"os"
	"net"
	"encoding/base64"
	"encoding/json"
	"strconv"
)

var URL string


func handle(conn net.Conn) {
    defer conn.Close()
    fmt.Println("new client:", conn.RemoteAddr())
	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Read Error:", err)
		return
	}
	line = strings.TrimSpace(line)
	fmt.Println("request line:", line)
	parts := strings.Fields(line)
	if len(parts) < 2 {
		fmt.Println("Bad request line.")
		return
	}
	method, target := parts[0], parts[1]
    fmt.Println("method:", method)
    fmt.Println("target:", target)
	for {
        h, err := reader.ReadString('\n')
        if err != nil {
            fmt.Println("header read error:", err)
            return
        }
        h = strings.TrimSpace(h)
        if h == "" {
            break
        }
        fmt.Println("header:", h)
    }

    if method == "CONNECT" {
    	host, port, _ := net.SplitHostPort(target)
    	fmt.Println("connect host:", host)
    	fmt.Println("connect port:", port)
    	portInt, _ := strconv.Atoi(port)
    	conn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))

    	wsconn, _, wserr := websocket.DefaultDialer.Dial(URL, nil)
    	if wserr != nil {
        	fmt.Println("dial error:", wserr)
        	return
    	}
    	defer wsconn.Close()

    	type Postcard struct {
        	SNI         string `json:"SNI"`
        	ConnectPort int    `json:"connect_port"`
        	Packet      string `json:"packet"`
    	}
    	go func() {
        	buf := make([]byte, 4096)
        	for {
            	n, err := reader.Read(buf)
            	if err != nil {
                	return
            	}
            	packet := base64.StdEncoding.EncodeToString(buf[:n])
            	msg, _ := json.Marshal(Postcard{
                	SNI:         host,
                	ConnectPort: portInt,
                	Packet:      packet,
            	})
            	if err := wsconn.WriteMessage(websocket.TextMessage, msg); err != nil {
                	return
            	}
        	}
    	}()

    	for {
        	_, resp, err := wsconn.ReadMessage()
        	if err != nil {
            	return
        	}
        	if _, err := conn.Write(resp); err != nil {
            	return
        	}
    	}
	
	}
}


func main(){
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Enter the worker url:  ")
	urlworker, _ := reader.ReadString('\n')
	urlworker = strings.TrimSpace(urlworker)
	urlworker = strings.TrimPrefix(urlworker, "wss://")
	urlworker = strings.TrimPrefix(urlworker, "https://")
	urlworker = strings.TrimPrefix(urlworker, "http://")
	urlworker = strings.TrimSuffix(urlworker, "/")
	URL = "wss://" + urlworker
	fmt.Println("1: 127.0.0.1")
	fmt.Println("2: 0.0.0.0")
	fmt.Print("Choose : ")
	choice, _:= reader.ReadString('\n')
	choice = strings.TrimSpace(choice)
	var addr string
	switch choice {
		case "1" :
			addr = "127.0.0.1:1080"
		case "2" :
			addr = "0.0.0.0:1080"
		
		default :
			fmt.Println("Incorrect only, 1,2 .")
			return
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	for {
		tcpconn, tcperr := ln.Accept()
		if tcperr != nil {
    		continue
		}
		go handle(tcpconn)
	}


}




