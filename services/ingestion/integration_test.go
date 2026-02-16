package main

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestLiveIngestion(t *testing.T) {
	// 1. Run the Service
	go RunService()

	time.Sleep(5 * time.Second)
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}

	// 2. Connect Viewer
	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("Viewer connection failed: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream init failed: %v", err)
	}

	videoCount := 0

	fmt.Println("\n📺 PENTECOST VIEWER CONNECTED")
	fmt.Println("   ... Waiting for buffer to fill (60 seconds) ...")

	// FIX: Increased timeout to 3 minutes
	timeout := time.After(3 * time.Minute)
	dataCh := make(chan bool)

	_, err = js.Subscribe("livestream.video.raw", func(m *nats.Msg) {
		videoCount++
		if videoCount == 1 {
			fmt.Println("\n✅ [VIDEO] FIRST FRAME RECEIVED FROM BUFFER!")
		}
		if videoCount%60 == 0 {
			fmt.Printf("   [VIDEO] Frame %d (PTS: %s)\n", videoCount, m.Header.Get("pts"))
			select {
			case dataCh <- true:
			default:
			}
		}
	})

	// 3. Wait for SUCCESS
	for {
		select {
		case <-dataCh:
			if videoCount > 120 {
				fmt.Println("\n✅ SUCCESS: Stream Verified (Buffer > Broadcast working)")
				return
			}
		case <-timeout:
			t.Fatalf("❌ TIMEOUT. Video frames: %d. Did the buffer release?", videoCount)
		}
	}
}
