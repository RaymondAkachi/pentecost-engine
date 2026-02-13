package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	TestInputFile = "test_fixture.mp4"
	// REMOVED hardcoded TestNatsURL constant
)

// Helper to get the correct URL depending on where we are running
func getTestNatsURL() string {
	if url := os.Getenv("NATS_URL"); url != "" {
		return url // Use Docker service name (nats://nats:4222)
	}
	return "nats://localhost:4222" // Fallback for local testing
}

func TestFullIngestionPipeline(t *testing.T) {
	// 1. SETUP: Generate test media
	fmt.Println(">>> Generating test media fixture...")
	err := generateTestMedia(TestInputFile)
	if err != nil {
		t.Fatalf("Failed to generate test media: %v", err)
	}
	defer os.Remove(TestInputFile)

	// 2. SETUP: Connect to NATS (Using the dynamic URL)
	natsURL := getTestNatsURL()
	t.Logf("Connecting to NATS at: %s", natsURL) // Log connection attempt

	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream not enabled: %v", err)
	}

	// Clean/Create Stream
	js.DeleteStream("LIVESTREAM_RAW")
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "LIVESTREAM_RAW",
		Subjects: []string{"livestream.>"},
		Storage:  nats.MemoryStorage,
	})
	if err != nil {
		t.Fatalf("Failed to create NATS stream: %v", err)
	}

	// 3. SUBSCRIBE
	videoReceived := make(chan bool, 1)
	audioReceived := make(chan bool, 1)

	// Audio Validator
	_, err = js.Subscribe("livestream.audio.raw", func(m *nats.Msg) {
		if len(m.Data) != 3840 {
			t.Errorf("Audio Frame Size Violation! Expected 3840, got %d", len(m.Data))
			return
		}
		if m.Header.Get("PTS") == "" {
			t.Errorf("Audio missing PTS header")
			return
		}
		select {
		case audioReceived <- true:
		default:
		}
	}, nats.BindStream("LIVESTREAM_RAW"))
	if err != nil {
		t.Fatalf("Failed to sub to audio: %v", err)
	}

	// Video Validator
	_, err = js.Subscribe("livestream.video.raw", func(m *nats.Msg) {
		if len(m.Data) == 0 {
			t.Errorf("Received empty video frame")
			return
		}
		if m.Header.Get("PTS") == "" {
			t.Errorf("Video missing PTS header")
			return
		}
		select {
		case videoReceived <- true:
		default:
		}
	}, nats.BindStream("LIVESTREAM_RAW"))
	if err != nil {
		t.Fatalf("Failed to sub to video: %v", err)
	}

	// 4. EXECUTE (Pass the dynamic URL here too)
	fmt.Println(">>> Starting Ingestion Service...")
	service, err := NewIngestionService(natsURL, TestInputFile)
	if err != nil {
		t.Fatalf("Failed to init service: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second) // Increased timeout slightly
	defer cancel()

	go func() {
		if err := service.Start(ctx); err != nil {
			t.Logf("Service finished: %v", err)
		}
	}()

	// 5. ASSERT
	timeout := time.After(12 * time.Second)
	gotVideo, gotAudio := false, false

	for {
		select {
		case <-videoReceived:
			gotVideo = true
			fmt.Println("✅ Video Frame Verified (LatentSync Ready)")
		case <-audioReceived:
			gotAudio = true
			fmt.Println("✅ Audio Frame Verified (DeepFilterNet3 Ready)")
		case <-timeout:
			t.Fatalf("Timeout! Audio: %v, Video: %v. Did FFmpeg start?", gotAudio, gotVideo)
		}

		if gotVideo && gotAudio {
			break
		}
	}

	fmt.Println(">>> Integration Test Passed: Architecture Compliance Verified.")
}

func generateTestMedia(filename string) error {
	// Adjusted to 24fps to match Pentecost Architecture targets
	cmd := exec.Command("ffmpeg",
		"-y",
		"-f", "lavfi", "-i", "testsrc=size=1920x1080:rate=24",
		"-f", "lavfi", "-i", "sine=frequency=1000:sample_rate=48000",
		"-c:v", "libx264", "-pix_fmt", "yuv420p",
		"-c:a", "aac", "-b:a", "128k",
		"-t", "5",
		filename,
	)
	return cmd.Run()
}
