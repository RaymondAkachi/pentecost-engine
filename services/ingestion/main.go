package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

// --- CONFIGURATION (Pentecost v2.0) ---
const (
	NatsURL        = "nats://nats:4222"
	VideoSubject   = "livestream.video.raw"
	AudioSubject   = "livestream.audio.raw"
	VideoPort      = ":4000"
	AudioPort      = ":4001"
	BufferDuration = 60 * time.Second // The "Pentecost Buffer"
)

// --- PENTECOST BUFFER IMPLEMENTATION ---
type StreamSegment struct {
	Data       []byte
	Timestamp  time.Time
	IsKeyframe bool
	PTS        int64
}

type PentecostBuffer struct {
	mu       sync.Mutex
	segments []*StreamSegment
	duration time.Duration
}

func NewPentecostBuffer(duration time.Duration) *PentecostBuffer {
	return &PentecostBuffer{
		duration: duration,
		segments: make([]*StreamSegment, 0, 1000),
	}
}

func (pb *PentecostBuffer) Add(seg *StreamSegment) {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	pb.segments = append(pb.segments, seg)

	// Safety: Prevent OOM if consumer dies
	if len(pb.segments) > 3000 {
		pb.segments = pb.segments[1:] // Drop oldest
	}
}

func (pb *PentecostBuffer) PopReady() []*StreamSegment {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	now := time.Now()
	var ready []*StreamSegment
	cutoffIndex := 0

	for i, seg := range pb.segments {
		if now.Sub(seg.Timestamp) >= pb.duration {
			ready = append(ready, seg)
			cutoffIndex = i + 1
		} else {
			break
		}
	}

	if cutoffIndex > 0 {
		pb.segments = pb.segments[cutoffIndex:]
	}
	return ready
}

// --- SERVICE ---
type IngestionService struct {
	nc          *nats.Conn
	js          nats.JetStreamContext
	inputURL    string
	videoBuffer *PentecostBuffer
	audioBuffer *PentecostBuffer
}

func main() {
	if len(os.Args) > 1 && strings.HasPrefix(os.Args[1], "-test") {
		return
	}
	RunService()
}

func RunService() {
	log.Println("🚀 Starting Pentecost Ingestion Service v2.0")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("🛑 Shutdown signal received")
		cancel()
	}()

	// 1. Connect to NATS
	var nc *nats.Conn
	var err error
	for i := 0; i < 10; i++ {
		nc, err = nats.Connect(os.Getenv("NATS_URL"), nats.Name("Pentecost-Ingest"))
		if err == nil {
			break
		}
		log.Printf("⚠️ NATS unavailable, retrying (%d/10)...", i+1)
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		log.Fatal("❌ NATS Connection Failed:", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		log.Fatal("❌ JetStream Init Failed:", err)
	}

	createStream(js, "LIVESTREAM_VIDEO", VideoSubject)
	createStream(js, "LIVESTREAM_AUDIO", AudioSubject)

	svc := &IngestionService{
		nc:          nc,
		js:          js,
		inputURL:    os.Getenv("INPUT_URL"),
		videoBuffer: NewPentecostBuffer(BufferDuration),
		audioBuffer: NewPentecostBuffer(BufferDuration),
	}

	if svc.inputURL == "" {
		svc.inputURL = "https://www.youtube.com/watch?v=jfKfPfyJRdk"
	}

	// 3. Start Broadcaster
	go svc.startBroadcaster(ctx)

	// 4. Start Ingestion
	var wg sync.WaitGroup
	videoReady := make(chan struct{})
	audioReady := make(chan struct{})

	wg.Add(2)
	go func() { defer wg.Done(); svc.startVideoServer(ctx, videoReady) }()
	go func() { defer wg.Done(); svc.startAudioServer(ctx, audioReady) }()

	log.Println("⏳ Waiting for TCP listeners...")
	<-videoReady
	<-audioReady
	log.Println("✅ TCP Listeners Active")

	// 5. Run FFmpeg Loop
	wg.Add(1)
	go func() {
		defer wg.Done()
		svc.runFFmpegLoop(ctx)
	}()

	wg.Wait()
}

func (s *IngestionService) startBroadcaster(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	log.Printf("⏳ Pentecost Buffer Active: Holding streams for %v...", BufferDuration)

	hasBroadcast := false

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			readyVideo := s.videoBuffer.PopReady()
			for _, seg := range readyVideo {
				msg := &nats.Msg{
					Subject: VideoSubject,
					Data:    seg.Data,
					Header:  nats.Header{},
				}
				msg.Header.Add("pts", fmt.Sprintf("%d", seg.PTS))
				msg.Header.Add("keyframe", fmt.Sprintf("%t", seg.IsKeyframe))
				s.js.PublishMsgAsync(msg)
			}

			// Only log sparsely to avoid spam
			if len(readyVideo) > 0 {
				if !hasBroadcast {
					log.Println("📡 BROADCAST LIVE: First frames released from buffer!")
					hasBroadcast = true
				}
			}

			readyAudio := s.audioBuffer.PopReady()
			for _, seg := range readyAudio {
				msg := &nats.Msg{
					Subject: AudioSubject,
					Data:    seg.Data,
					Header:  nats.Header{},
				}
				msg.Header.Add("pts", fmt.Sprintf("%d", seg.PTS))
				s.js.PublishMsgAsync(msg)
			}
		}
	}
}

func (s *IngestionService) runFFmpegLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			streamURL := s.inputURL
			if strings.Contains(s.inputURL, "youtube.com") || strings.Contains(s.inputURL, "youtu.be") {
				log.Println("🔍 Resolving YouTube URL with yt-dlp...")
				// Force HLS to handle live streams better
				cmd := exec.CommandContext(ctx, "yt-dlp", "-f", "best[height<=720]", "-g", s.inputURL)
				out, err := cmd.Output()
				if err != nil {
					log.Printf("❌ yt-dlp failed: %v. Retrying in 5s...", err)
					time.Sleep(5 * time.Second)
					continue
				}
				streamURL = strings.TrimSpace(string(out))
				log.Println("✅ Resolved Stream URL")
			}

			log.Println("🎬 Starting FFmpeg Ingestion...")

			args := []string{
				"-loglevel", "error", // FIX: Silence the "Skip AD" spam
				"-re",
				"-i", streamURL,
				"-map", "0:v", "-c:v", "copy", "-bsf:v", "h264_mp4toannexb", "-f", "h264", "tcp://127.0.0.1:4000",
				"-map", "0:a", "-c:a", "pcm_f32le", "-ar", "16000", "-ac", "1", "-f", "f32le", "tcp://127.0.0.1:4001",
			}

			cmd := exec.CommandContext(ctx, "ffmpeg", args...)
			cmd.Stderr = os.Stderr
			if err := cmd.Run(); err != nil {
				log.Printf("⚠️ FFmpeg exited: %v", err)
			}

			time.Sleep(2 * time.Second)
		}
	}
}

func (s *IngestionService) startVideoServer(ctx context.Context, ready chan<- struct{}) {
	ln, err := net.Listen("tcp", VideoPort)
	if err != nil {
		log.Fatal(err)
	}
	close(ready)
	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go s.handleVideo(conn)
	}
}

func (s *IngestionService) handleVideo(conn net.Conn) {
	defer conn.Close()
	log.Println("🔌 Video Stream Connected! (Buffering starts now)") // FIX: Immediate Feedback

	scanner := bufio.NewScanner(conn)
	buf := make([]byte, 10*1024*1024)
	scanner.Buffer(buf, 10*1024*1024)

	scanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		delimiter := []byte{0, 0, 0, 1}
		if i := bytes.Index(data, delimiter); i >= 0 {
			if i > 0 {
				return i, data[:i], nil
			}
			if j := bytes.Index(data[4:], delimiter); j >= 0 {
				return j + 4, data[:j+4], nil
			}
		}
		if atEOF {
			return len(data), data, nil
		}
		return 0, nil, nil
	})

	start := time.Now()
	for scanner.Scan() {
		frame := make([]byte, len(scanner.Bytes()))
		copy(frame, scanner.Bytes())

		pts := time.Since(start).Milliseconds()
		isKey := false
		if len(frame) > 4 {
			nal := frame[4] & 0x1F
			if nal == 5 || nal == 7 || nal == 8 {
				isKey = true
			}
		}

		s.videoBuffer.Add(&StreamSegment{
			Data:       frame,
			Timestamp:  time.Now(),
			PTS:        pts,
			IsKeyframe: isKey,
		})
	}
}

func (s *IngestionService) startAudioServer(ctx context.Context, ready chan<- struct{}) {
	ln, err := net.Listen("tcp", AudioPort)
	if err != nil {
		log.Fatal(err)
	}
	close(ready)
	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go s.handleAudio(conn)
	}
}

func (s *IngestionService) handleAudio(conn net.Conn) {
	defer conn.Close()
	log.Println("🔌 Audio Stream Connected!") // FIX: Immediate Feedback

	// 16kHz * 1ch * 4bytes = 64000 bytes/sec
	// 0.1s chunk = 6400 bytes
	const ChunkSize = 6400
	buf := make([]byte, ChunkSize)
	var totalBytes int64 = 0

	for {
		n, err := io.ReadFull(conn, buf) // FIX: Use ReadFull
		if err != nil {
			return
		}

		dataCopy := make([]byte, n)
		copy(dataCopy, buf[:n])

		// Correct PTS calculation
		pts := (totalBytes * 1000) / (16000 * 4 * 1)
		totalBytes += int64(n)

		s.audioBuffer.Add(&StreamSegment{
			Data:      dataCopy,
			Timestamp: time.Now(),
			PTS:       pts,
		})
	}
}

func createStream(js nats.JetStreamContext, name, subject string) {
	_, err := js.StreamInfo(name)
	if err != nil {
		js.AddStream(&nats.StreamConfig{
			Name:     name,
			Subjects: []string{subject},
			MaxAge:   5 * time.Minute,
			Storage:  nats.MemoryStorage,
		})
	}
}
