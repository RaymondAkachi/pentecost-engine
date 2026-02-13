package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// Architecture Constants
const (
	VideoSubject        = "livestream.video.raw"
	AudioSubject        = "livestream.audio.raw"
	AudioSampleRate     = 48000
	AudioFrameSize      = 960 // 20ms @ 48kHz
	AudioBytesPerSample = 4   // Float32 = 4 bytes
)

type IngestionService struct {
	nc        *nats.Conn
	js        nats.JetStreamContext
	inputURL  string
	startTime time.Time
}

func NewIngestionService(natsURL, inputURL string) (*IngestionService, error) {
	log.Printf("🔌 Connecting to NATS at %s...", natsURL)
	nc, err := nats.Connect(natsURL)
	if err != nil {
		return nil, fmt.Errorf("nats connect: %w", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		return nil, fmt.Errorf("jetstream init: %w", err)
	}
	log.Println("✅ NATS Connected & JetStream Ready")

	return &IngestionService{
		nc:       nc,
		js:       js,
		inputURL: inputURL,
	}, nil
}

func (s *IngestionService) Start(ctx context.Context) error {
	// 1. Create OS Pipes
	vRead, vWrite, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("video pipe error: %w", err)
	}
	aRead, aWrite, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("audio pipe error: %w", err)
	}

	// 2. Configure FFmpeg (DEBUG MODE ENABLED)
	log.Printf("🎥 Starting FFmpeg for input: %s", s.inputURL)
	ffmpeg := exec.CommandContext(ctx, "ffmpeg",
		"-hide_banner",
		"-loglevel", "info", // 👈 CHANGED: Enable Info logs to see what's happening
		"-re", // Read at native speed
		"-i", s.inputURL,

		// Video Output
		"-map", "0:v:0",
		"-c:v", "copy",
		"-bsf:v", "h264_mp4toannexb",
		"-f", "h264",
		"pipe:3",

		// Audio Output
		"-map", "0:a:0",
		"-c:a", "pcm_f32le",
		"-ar", "48000",
		"-ac", "1",
		"-f", "f32le",
		"pipe:4",
	)

	// 3. Capture FFmpeg Stderr to see errors
	ffmpeg.Stderr = os.Stderr
	ffmpeg.Stdout = os.Stdout

	// 4. Attach Pipes (FD 3 and 4)
	ffmpeg.ExtraFiles = []*os.File{vWrite, aWrite}

	s.startTime = time.Now().UTC()

	if err := ffmpeg.Start(); err != nil {
		return fmt.Errorf("ffmpeg start failed: %w", err)
	}
	log.Println("🚀 FFmpeg Process Started")

	// Close write ends in Go so we get EOF when FFmpeg finishes
	vWrite.Close()
	aWrite.Close()

	var wg sync.WaitGroup
	wg.Add(2)

	// Video Routine
	go func() {
		defer wg.Done()
		log.Println("📺 Starting Video Processor...")
		s.processVideo(vRead)
		log.Println("🛑 Video Processor Finished")
	}()

	// Audio Routine
	go func() {
		defer wg.Done()
		log.Println("🔊 Starting Audio Processor...")
		s.processAudio(aRead)
		log.Println("🛑 Audio Processor Finished")
	}()

	err = ffmpeg.Wait()
	log.Printf("🏁 FFmpeg Process Exited. Error: %v", err)
	wg.Wait()
	return err
}

func (s *IngestionService) processVideo(reader *os.File) {
	defer reader.Close()
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 1024*1024), 10*1024*1024)
	scanner.Split(splitNALUnits)

	count := 0
	for scanner.Scan() {
		count++
		frameData := scanner.Bytes()
		payload := make([]byte, len(frameData))
		copy(payload, frameData)

		elapsed := time.Since(s.startTime)
		pts := int64(elapsed.Seconds() * 90000)

		msg := &nats.Msg{
			Subject: VideoSubject,
			Data:    payload,
			Header: nats.Header{
				"PTS":       []string{fmt.Sprintf("%d", pts)},
				"Timestamp": []string{time.Now().UTC().Format(time.RFC3339Nano)},
			},
		}

		if _, err := s.js.PublishMsg(msg); err != nil {
			log.Printf("❌ Video Publish Error: %v", err)
		} else if count%24 == 0 {
			log.Printf("📤 Published Video Frame #%d (PTS: %d)", count, pts)
		}
	}
	if err := scanner.Err(); err != nil {
		log.Printf("❌ Video Scan Error: %v", err)
	}
}

func (s *IngestionService) processAudio(reader *os.File) {
	defer reader.Close()
	frameSize := AudioFrameSize * AudioBytesPerSample
	buf := make([]byte, frameSize)
	count := 0

	for {
		_, err := io.ReadFull(reader, buf)
		if err != nil {
			if err != io.EOF {
				log.Printf("❌ Audio Read Error: %v", err)
			}
			break
		}
		count++

		payload := make([]byte, frameSize)
		copy(payload, buf)

		elapsed := time.Since(s.startTime)
		pts := int64(elapsed.Seconds() * 90000)

		msg := &nats.Msg{
			Subject: AudioSubject,
			Data:    payload,
			Header: nats.Header{
				"PTS":         []string{fmt.Sprintf("%d", pts)},
				"Sample-Rate": []string{"48000"},
			},
		}

		if _, err := s.js.PublishMsg(msg); err != nil {
			log.Printf("❌ Audio Publish Error: %v", err)
		} else if count%50 == 0 { // Log every 1 second of audio
			log.Printf("📤 Published Audio Chunk #%d (PTS: %d)", count, pts)
		}
	}
}

func splitNALUnits(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	startSeq := []byte{0, 0, 0, 1}
	offset := 0
	if bytes.HasPrefix(data, startSeq) {
		offset = 4
	}
	next := bytes.Index(data[offset:], startSeq)
	if next == -1 {
		if atEOF {
			return len(data), data, nil
		}
		return 0, nil, nil
	}
	fullFrameLen := offset + next
	return fullFrameLen, data[:fullFrameLen], nil
}

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds) // High precision timestamps

	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}
	inputURL := os.Getenv("INPUT_URL")
	if inputURL == "" {
		log.Fatal("INPUT_URL environment variable required")
	}

	service, err := NewIngestionService(natsURL, inputURL)
	if err != nil {
		log.Fatalf("❌ FATAL: Could not start service: %v", err)
	}

	ctx := context.Background()
	if err := service.Start(ctx); err != nil {
		log.Fatalf("❌ FATAL: Runtime error: %v", err)
	}
}
