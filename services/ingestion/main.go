package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

// --- CONFIGURATION ---
const (
	NatsURL         = "nats://nats:4222"
	VideoSubject    = "livestream.video.raw"
	AudioSubject    = "livestream.audio.raw"
	AudioPort       = ":4001"
	SharedChunkPath = "/shared/chunks"

	SampleRate     = 16000
	Channels       = 1
	BytesPerSamp   = 4
	BatchDuration  = 5
	AudioBatchSize = SampleRate * Channels * BytesPerSamp * BatchDuration
	BufferDelay    = 60 * time.Second
)

type ChunkPayload struct {
	ChunkID      string  `json:"chunk_id"`
	PTS          int64   `json:"pts"`
	Duration     float64 `json:"duration"`
	FilePath     string  `json:"file_path"`
	ThumbnailDir string  `json:"thumbnail_dir,omitempty"` // NEW: The thumbnail subfolder path
	Data         string  `json:"data,omitempty"`
}

// --- PENTECOST BUFFER ---
type BufferedPayload struct {
	Subject   string
	Payload   []byte
	Timestamp time.Time
}

type PentecostBuffer struct {
	mu       sync.Mutex
	queue    []BufferedPayload
	duration time.Duration
	js       nats.JetStreamContext
}

func NewPentecostBuffer(js nats.JetStreamContext, duration time.Duration) *PentecostBuffer {
	return &PentecostBuffer{
		queue:    make([]BufferedPayload, 0),
		duration: duration,
		js:       js,
	}
}

func (pb *PentecostBuffer) Add(subject string, payload []byte) {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	pb.queue = append(pb.queue, BufferedPayload{
		Subject:   subject,
		Payload:   payload,
		Timestamp: time.Now(),
	})
}

func (pb *PentecostBuffer) Start(ctx context.Context) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pb.mu.Lock()
			now := time.Now()
			var cutoff int

			for i, item := range pb.queue {
				if now.Sub(item.Timestamp) >= pb.duration {
					pb.js.PublishAsync(item.Subject, item.Payload)
					log.Printf("📡 BROADCAST LIVE: 60s Buffer Released | Subject: %s", item.Subject)
					cutoff = i + 1
				} else {
					break
				}
			}

			if cutoff > 0 {
				pb.queue = pb.queue[cutoff:]
			}
			pb.mu.Unlock()
		}
	}
}

// --- INGESTION SERVICE ---
type IngestionService struct {
	nc       *nats.Conn
	js       nats.JetStreamContext
	inputURL string
	buffer   *PentecostBuffer

	mu           sync.Mutex
	audioSamples int64
	videoSeq     int
}

func main() {
	log.Println("🚀 Starting Pentecost Ingestion Service v2.2 (Multi-Output Chunker)")

	if err := os.MkdirAll(SharedChunkPath, 0755); err != nil {
		log.Fatalf("❌ Failed to create shared chunk directory: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		cancel()
	}()

	nc, err := nats.Connect(
		os.Getenv("NATS_URL"),
		nats.Name("Pentecost-Batch-Ingest"),
		nats.MaxReconnects(-1),
		nats.RetryOnFailedConnect(true),
	)
	if err != nil {
		log.Fatal("❌ NATS Connection Failed:", err)
	}
	defer nc.Close()

	js, err := nc.JetStream(nats.PublishAsyncErrHandler(func(js nats.JetStream, msg *nats.Msg, err error) {
		log.Printf("❌ CRITICAL: NATS Async Publish Failed [Subj: %s]: %v", msg.Subject, err)
	}))
	if err != nil {
		log.Fatal("❌ JetStream Init Failed:", err)
	}

	createStream(js, "LIVESTREAM_RAW", []string{VideoSubject, AudioSubject})

	svc := &IngestionService{
		nc:       nc,
		js:       js,
		inputURL: os.Getenv("INPUT_URL"),
		buffer:   NewPentecostBuffer(js, BufferDelay),
	}

	if svc.inputURL == "" {
		svc.inputURL = "https://www.youtube.com/watch?v=jL8uDJJBjMA"
	}

	var wg sync.WaitGroup
	audioReady := make(chan struct{})

	wg.Add(1)
	go func() { defer wg.Done(); svc.buffer.Start(ctx) }()

	wg.Add(1)
	go func() { defer wg.Done(); svc.diskJanitor(ctx) }()

	wg.Add(1)
	go func() { defer wg.Done(); svc.startAudioServer(ctx, audioReady) }()
	<-audioReady

	wg.Add(1)
	go func() { defer wg.Done(); svc.videoStabilizationWatcher(ctx) }()

	wg.Add(1)
	go func() { defer wg.Done(); svc.runFFmpegLoop(ctx) }()

	wg.Wait()
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
				cmd := exec.CommandContext(ctx, "yt-dlp",
					// "-f", "95/94/93/b",
					"-f", "best[ext=mp4]/best", // FIX: Used for facial recognition testing This grabs standard 1080p videos AND live streams
					"-g",
					"--no-warnings",
					"--no-cache-dir",
					"--extractor-args", "youtube:player_client=android",
					s.inputURL,
				)

				var stderr bytes.Buffer
				cmd.Stderr = &stderr

				out, err := cmd.Output()

				if err != nil {
					log.Printf("❌ yt-dlp failed (Retrying in 5s):\nError: %v\nDetails: %s", err, stderr.String())
					time.Sleep(5 * time.Second)
					continue
				}

				urls := strings.Split(strings.TrimSpace(string(out)), "\n")
				streamURL = strings.TrimSpace(urls[0])

				if streamURL == "" {
					log.Printf("❌ yt-dlp returned an empty URL. Retrying in 5s...")
					time.Sleep(5 * time.Second)
					continue
				}
				log.Println("✅ Resolved Stream URL")
			}

			s.mu.Lock()
			startSeq := s.videoSeq
			s.mu.Unlock()

			// Calculate the continuous image sequence start number
			imageStartNum := (startSeq * 15) + 1

			args := []string{
				"-hide_banner", "-loglevel", "error",
				"-re", "-i", streamURL,

				// OUTPUT A: Video MP4 Segments
				"-map", "0:v",
				"-c:v", "libx264", "-preset", "veryfast",
				"-force_key_frames", "expr:gte(t,n_forced*5)",
				"-f", "segment", "-segment_time", "5",
				"-segment_start_number", fmt.Sprintf("%d", startSeq),
				"-segment_format", "mp4", "-reset_timestamps", "1",
				fmt.Sprintf("%s/chunk_v_%%d.mp4", SharedChunkPath),

				// OUTPUT B: Thumbnails (3 FPS JPEGs)
				"-map", "0:v",
				"-vf", "fps=3",
				"-c:v", "mjpeg",
				"-q:v", "2", // High quality JPEGs
				"-f", "image2",
				"-start_number", fmt.Sprintf("%d", imageStartNum), // Aligns with video sequence
				fmt.Sprintf("%s/frame_%%08d.jpg", SharedChunkPath),

				// OUTPUT C: Audio TCP Stream
				"-map", "0:a",
				"-c:a", "pcm_f32le", "-ar", "16000", "-ac", "1",
				"-f", "f32le", "tcp://127.0.0.1:4001",
			}

			cmd := exec.CommandContext(ctx, "ffmpeg", args...)
			cmd.Stderr = os.Stderr
			cmd.Run()

			log.Println("⚠️ FFmpeg connection lost. Restarting in 2s...")
			time.Sleep(2 * time.Second)
		}
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
	log.Println("🔌 Audio Stream Connected! (Batch Master Clock)")

	buf := make([]byte, AudioBatchSize)

	for {
		_, err := io.ReadFull(conn, buf)
		if err != nil {
			return
		}

		s.mu.Lock()
		pts := (s.audioSamples * 1000) / int64(SampleRate)
		s.audioSamples += int64(AudioBatchSize / BytesPerSamp)
		s.mu.Unlock()

		payload := ChunkPayload{
			ChunkID:  fmt.Sprintf("a_%d", pts),
			PTS:      pts,
			Duration: float64(BatchDuration),
			Data:     base64.StdEncoding.EncodeToString(buf),
		}

		payloadBytes, _ := json.Marshal(payload)

		s.buffer.Add(AudioSubject, payloadBytes)
		log.Printf("🎵 Audio Batch queued for 60s delay | PTS: %d", pts)
	}
}

func (s *IngestionService) videoStabilizationWatcher(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(500 * time.Millisecond):
			s.mu.Lock()
			currentSeq := s.videoSeq
			s.mu.Unlock()

			// Check if N+1 exists. If so, N is completely written.
			triggerFilePath := fmt.Sprintf("%s/chunk_v_%d.mp4", SharedChunkPath, currentSeq+1)

			if _, err := os.Stat(triggerFilePath); err == nil {
				pts := int64(currentSeq * 5000)
				currentFilePath := fmt.Sprintf("%s/chunk_v_%d.mp4", SharedChunkPath, currentSeq)

				// CREATE THE THUMBNAIL SUBFOLDER
				thumbDir := fmt.Sprintf("%s/thumbs_v_%d", SharedChunkPath, pts)
				os.MkdirAll(thumbDir, 0755)

				// MOVE EXACTLY 15 FRAMES INTO THE SUBFOLDER
				startFrame := currentSeq*15 + 1
				endFrame := startFrame + 14
				for f := startFrame; f <= endFrame; f++ {
					src := fmt.Sprintf("%s/frame_%08d.jpg", SharedChunkPath, f)
					dst := fmt.Sprintf("%s/frame_%02d.jpg", thumbDir, f-startFrame+1)
					os.Rename(src, dst) // Will safely ignore if a frame dropped
				}

				payload := ChunkPayload{
					ChunkID:      fmt.Sprintf("v_%d", pts),
					PTS:          pts,
					Duration:     float64(BatchDuration),
					FilePath:     currentFilePath,
					ThumbnailDir: thumbDir, // ATTACH TO JSON PAYLOAD
				}

				payloadBytes, _ := json.Marshal(payload)

				s.buffer.Add(VideoSubject, payloadBytes)
				log.Printf("📦 Video Batch & Thumbs queued | PTS: %d | Path: %s", pts, thumbDir)

				s.mu.Lock()
				s.videoSeq++
				s.mu.Unlock()
			}
		}
	}
}

// diskJanitor cleans up MP4s, Subdirectories, and any orphaned image files
func (s *IngestionService) diskJanitor(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()

			// 1. Clean old MP4s
			files, err := filepath.Glob(fmt.Sprintf("%s/chunk_v_*.mp4", SharedChunkPath))
			if err == nil {
				for _, file := range files {
					info, err := os.Stat(file)
					if err == nil && now.Sub(info.ModTime()) > 180*time.Second {
						os.Remove(file)
					}
				}
			}

			// 2. Clean old Thumbnail Subdirectories
			dirs, err := filepath.Glob(fmt.Sprintf("%s/thumbs_v_*", SharedChunkPath))
			if err == nil {
				for _, dir := range dirs {
					info, err := os.Stat(dir)
					if err == nil && info.IsDir() && now.Sub(info.ModTime()) > 180*time.Second {
						os.RemoveAll(dir) // Recursively delete directory and images
					}
				}
			}

			// 3. Clean orphaned frames (in case a segment was incomplete)
			orphans, err := filepath.Glob(fmt.Sprintf("%s/frame_*.jpg", SharedChunkPath))
			if err == nil {
				for _, orphan := range orphans {
					info, err := os.Stat(orphan)
					if err == nil && now.Sub(info.ModTime()) > 180*time.Second {
						os.Remove(orphan)
					}
				}
			}
		}
	}
}

func createStream(js nats.JetStreamContext, name string, subjects []string) {
	_, err := js.StreamInfo(name)
	if err != nil {
		js.AddStream(&nats.StreamConfig{
			Name:      name,
			Subjects:  subjects,
			MaxAge:    10 * time.Minute,
			Storage:   nats.MemoryStorage,
			Retention: nats.LimitsPolicy,
		})
	}
}
