package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/coder/websocket"
)

// main connects to the in-app gateway and prints every message pushed to a user.
func main() {
	url := flag.String("url", "ws://localhost:8083", "gateway base url")
	key := flag.String("key", "demo-key", "API key")
	user := flag.String("user", "demo-user", "user id to listen for")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	c, _, err := websocket.Dial(ctx, *url+"/v1/ws?user_id="+*user, &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": {"ApiKey " + *key}}})
	if err != nil {
		fmt.Fprintln(os.Stderr, "connect failed:", err)
		os.Exit(1)
	}
	defer c.CloseNow()
	fmt.Printf("connected as %s, waiting for messages (Ctrl+C to stop)\n", *user)
	for {
		_, msg, err := c.Read(ctx)
		if err != nil {
			return
		}
		fmt.Printf("%s  %s\n", time.Now().Format("15:04:05"), msg)
	}
}
