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

// --- CONFIGURATION (Pentecost v2.1 - Fixed Audio) ---
const (
	NatsURL        = "nats://nats:4222"
	VideoSubject   = "livestream.video.raw"
	AudioSubject   = "livestream.audio.raw"
	VideoPort      = ":4000"
	AudioPort      = ":4001"
	BufferDuration = 60 * time.Second

	// AUDIO CONSTANTS (Crucial for Sync)
	SampleRate   = 16000
	Channels     = 1
	BytesPerSamp = 4    // Float32 = 4 bytes
	AudioChunkSz = 4096 // 4KB chunks = ~64ms of audio
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
		segments: make([]*StreamSegment, 0, 5000),
	}
}

func (pb *PentecostBuffer) Add(seg *StreamSegment) {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	pb.segments = append(pb.segments, seg)
	// Safety cap to prevent memory leaks
	if len(pb.segments) > 10000 {
		pb.segments = pb.segments[1:]
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
	RunService()
}

func RunService() {
	log.Println("🚀 Starting Pentecost Ingestion Service v2.1 (Audio Fix)")

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
		url := os.Getenv("NATS_URL")
		if url == "" {
			url = NatsURL
		}
		nc, err = nats.Connect(url, nats.Name("Pentecost-Ingest"))
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

	createStream(js, "LIVESTREAM_RAW", []string{VideoSubject, AudioSubject})

	svc := &IngestionService{
		nc:          nc,
		js:          js,
		inputURL:    os.Getenv("INPUT_URL"),
		videoBuffer: NewPentecostBuffer(BufferDuration),
		audioBuffer: NewPentecostBuffer(BufferDuration),
	}

	if svc.inputURL == "" {
		svc.inputURL = "https://www.youtube.com/watch?v=jL8uDJJBjMA" // Al Jazeera default
	}

	// 2. Start Broadcaster
	go svc.startBroadcaster(ctx)

	// 3. Start TCP Listeners for FFmpeg
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

	// 4. Run FFmpeg Loop
	wg.Add(1)
	go func() {
		defer wg.Done()
		svc.runFFmpegLoop(ctx)
	}()

	wg.Wait()
}

func (s *IngestionService) startBroadcaster(ctx context.Context) {
	ticker := time.NewTicker(50 * time.Millisecond) // Fast tick for smooth playback
	defer ticker.Stop()

	log.Printf("⏳ Pentecost Buffer Active: Holding streams for %v...", BufferDuration)
	hasBroadcast := false

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// 1. Process Video
			readyVideo := s.videoBuffer.PopReady()
			for _, seg := range readyVideo {
				msg := &nats.Msg{
					Subject: VideoSubject,
					Data:    seg.Data,
					Header:  nats.Header{},
				}
				msg.Header.Add("pts", fmt.Sprintf("%d", seg.PTS))
				s.js.PublishMsgAsync(msg)
			}

			// 2. Process Audio
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

			// Flush Async Buffer
			select {
			case <-s.js.PublishAsyncComplete():
			default:
			}

			if len(readyVideo) > 0 || len(readyAudio) > 0 {
				if !hasBroadcast {
					log.Println("📡 BROADCAST LIVE: First frames released from buffer!")
					hasBroadcast = true
				}
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

			// Resolve YouTube URL
			if strings.Contains(s.inputURL, "youtube.com") || strings.Contains(s.inputURL, "youtu.be") {
				log.Println("🔍 Resolving YouTube URL with yt-dlp...")
				// Force HLS (m3u8) for better stability
				cmd := exec.CommandContext(ctx, "yt-dlp", "-f", "95/94/93/best", "-g", s.inputURL)
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

			// CRITICAL FFMPEG ARGS FOR SYNC & AUDIO QUALITY
			args := []string{
				"-hide_banner", "-loglevel", "error",
				"-re", // Read input at native frame rate
				"-i", streamURL,

				// VIDEO: Copy H.264 stream directly to TCP 4000
				"-map", "0:v",
				"-c:v", "copy",
				"-bsf:v", "h264_mp4toannexb", // Essential for raw H.264 stream
				"-f", "h264",
				"tcp://127.0.0.1:4000",

				// AUDIO: Transcode to PCM Float32LE @ 16000Hz Mono
				"-map", "0:a",
				"-c:a", "pcm_f32le",
				"-ar", "16000", // FORCE 16k
				"-ac", "1", // FORCE Mono
				"-f", "f32le", // Raw float32 stream
				"tcp://127.0.0.1:4001",
			}

			cmd := exec.CommandContext(ctx, "ffmpeg", args...)
			cmd.Stderr = os.Stderr // Pipe FFmpeg errors to Docker logs

			if err := cmd.Run(); err != nil {
				log.Printf("⚠️ FFmpeg exited: %v", err)
			}

			// If FFmpeg dies, wait before restarting
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

// Split H.264 stream into NAL units
func (s *IngestionService) handleVideo(conn net.Conn) {
	defer conn.Close()
	log.Println("🔌 Video Stream Connected!")

	// 10MB Buffer for HD frames
	scanner := bufio.NewScanner(conn)
	buf := make([]byte, 10*1024*1024)
	scanner.Buffer(buf, 10*1024*1024)

	// Custom Split function for H.264 NAL start codes (00 00 00 01)
	scanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if atEOF && len(data) == 0 {
			return 0, nil, nil
		}

		// Find NAL start code
		delimiter := []byte{0, 0, 0, 1}
		if i := bytes.Index(data, delimiter); i >= 0 {
			// Found start code
			if i == 0 {
				// We are at the start, find NEXT start code
				if j := bytes.Index(data[4:], delimiter); j >= 0 {
					return j + 4, data[:j+4], nil
				}
				// Need more data to find end of frame
				if atEOF {
					return len(data), data, nil
				}
				return 0, nil, nil
			}
			// We found a start code later in buffer, return up to it
			return i, data[:i], nil
		}

		if atEOF {
			return len(data), data, nil
		}
		return 0, nil, nil
	})

	start := time.Now()
	for scanner.Scan() {
		frame := make([]byte, len(scanner.Bytes()))
		copy(frame, scanner.Bytes()) // Copy strictly

		pts := time.Since(start).Milliseconds() * 90 // 90kHz clock for consistency

		isKey := false
		if len(frame) > 4 {
			nalType := frame[4] & 0x1F
			if nalType == 5 || nalType == 7 || nalType == 8 {
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
	log.Println("🔌 Audio Stream Connected!")

	// Chunk Size: 4096 bytes (1024 samples @ Float32)
	// At 16000Hz, 1024 samples = 64ms duration
	buf := make([]byte, AudioChunkSz)
	var totalSamples int64 = 0

	for {
		// ReadFull ensures we get a complete chunk.
		// Previous code used Read() which returns partial chunks -> SPEEDUP BUG
		n, err := io.ReadFull(conn, buf)
		if err != nil {
			if err != io.EOF {
				log.Printf("Audio read error: %v", err)
			}
			return
		}

		dataCopy := make([]byte, n)
		copy(dataCopy, buf[:n])

		// Calculate PTS based on sample count
		// PTS = (TotalSamples / SampleRate) * 1000ms
		// We use simple sample counting for exact sync

		pts := (totalSamples * 1000) / int64(SampleRate)
		totalSamples += int64(n / BytesPerSamp)

		s.audioBuffer.Add(&StreamSegment{
			Data:      dataCopy,
			Timestamp: time.Now(),
			PTS:       pts,
		})
	}
}

func createStream(js nats.JetStreamContext, name string, subjects []string) {
	_, err := js.StreamInfo(name)
	if err != nil {
		log.Printf("Creating Stream: %s", name)
		_, err = js.AddStream(&nats.StreamConfig{
			Name:      name,
			Subjects:  subjects,
			MaxAge:    5 * time.Minute,
			Storage:   nats.MemoryStorage,
			Retention: nats.LimitsPolicy,
		})
		if err != nil {
			log.Printf("⚠️ Stream Create Warn: %v", err)
		}
	}
}
