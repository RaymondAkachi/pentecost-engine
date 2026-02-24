package main

import (
	"log"
	"os"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	// Look for Temporal server URL in environment, fallback to local
	temporalURL := os.Getenv("TEMPORAL_URL")
	if temporalURL == "" {
		temporalURL = "temporal:7233" 
	}

	log.Printf("Dialing Temporal Server at %s...", temporalURL)
	c, err := client.Dial(client.Options{
		HostPort: temporalURL,
	})
	if err != nil {
		log.Fatalln("❌ Unable to create Temporal client", err)
	}
	defer c.Close()

	// Create a Worker listening on the dedicated Pentecost queue
	w := worker.New(c, "PENTECOST_TASK_QUEUE", worker.Options{})

	// 1. Register Workflows
	w.RegisterWorkflow(PentecostChunkWorkflow)

	// 2. Register Activities
	w.RegisterActivity(DetectFaces)
	w.RegisterActivity(CleanAudioRNNoise)
	w.RegisterActivity(TranslateAndVerify)
	w.RegisterActivity(SynthesizeVoice)
	w.RegisterActivity(SynthesizeVoiceFallback)
	w.RegisterActivity(GenerateInfiniteTalk)
	w.RegisterActivity(ApplyGoldenLayer)
	w.RegisterActivity(RemuxAudioOnly)

	// 3. Start the engine
	log.Println("✅ Pentecost Orchestrator Worker started. Listening for tasks...")
	err = w.Run(worker.InterruptCh())
	if err != nil {
		log.Fatalln("❌ Unable to start Worker", err)
	}
}