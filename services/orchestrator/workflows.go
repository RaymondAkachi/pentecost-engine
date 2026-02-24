package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// --- DATA MODELS ---
type ChunkTask struct {
	ChunkID         string
	VideoPath       string
	AudioPath       string
	TargetLanguages []string
}

type ProcessingResult struct {
	ChunkID         string
	Success         bool
	FinalVideoPaths map[string]string
}

type FaceDetectionResult struct {
	Confidence float64
	FaceRatio  float64
}

// LangFuture pairs a language with its async execution thread deterministically
type LangFuture struct {
	Lang   string
	Future workflow.Future
}

// --- WORKFLOW STATE ---
type PentecostWorkflowState struct {
	ICLAudioBuffer []string // 10-second rolling buffer
}

func (s *PentecostWorkflowState) UpdateICLBuffer(newPath string) {
	s.ICLAudioBuffer = append(s.ICLAudioBuffer, newPath)
	if len(s.ICLAudioBuffer) > 2 { // Keep last 2 chunks (5s each)
		s.ICLAudioBuffer = s.ICLAudioBuffer[1:]
	}
}

// --- THE ORCHESTRATOR WORKFLOW ---
func PentecostChunkWorkflow(ctx workflow.Context, task ChunkTask) (ProcessingResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("🚀 Orchestrating Chunk v2.0", "ChunkID", task.ChunkID)

	state := &PentecostWorkflowState{
		ICLAudioBuffer: make([]string, 0),
	}

	// 1. SCENE BYPASS TRIGGER
	ctxFace := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	})

	var faceData FaceDetectionResult
	err := workflow.ExecuteActivity(ctxFace, DetectFaces, task.VideoPath).Get(ctx, &faceData)
	if err != nil {
		return ProcessingResult{}, err
	}

	requiresFullVideo := faceData.Confidence > 0.7 && faceData.FaceRatio > 0.15
	logger.Info("   ↳ Scene Bypass Analysis", "FullVideoRequired", requiresFullVideo)

	// 2. AUDIO SEPARATION (RNNoise)
	ctxClean := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 15 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 2},
	})

	var cleanAudioPath string
	err = workflow.ExecuteActivity(ctxClean, CleanAudioRNNoise, task.AudioPath).Get(ctx, &cleanAudioPath)
	if err != nil {
		return ProcessingResult{}, err
	}

	state.UpdateICLBuffer(cleanAudioPath)
	currentReferenceAudio := make([]string, len(state.ICLAudioBuffer))
	copy(currentReferenceAudio, state.ICLAudioBuffer)

	// 3. PARALLEL DIALECT PROCESSING (Deterministic Fix)
	finalPaths := make(map[string]string)
	var langFutures []LangFuture

	for _, lang := range task.TargetLanguages {
		l := lang 
		future, settable := workflow.NewFuture(ctx)
		
		// Append to an ordered slice, NOT a map
		langFutures = append(langFutures, LangFuture{Lang: l, Future: future})

		workflow.Go(ctx, func(gCtx workflow.Context) {
			path, err := processDialectV2(gCtx, task, cleanAudioPath, currentReferenceAudio, l, requiresFullVideo)
			settable.Set(path, err)
		})
	}

	// 4. GATHER RESULTS
	for _, lf := range langFutures {
		var dialectPath string
		err := lf.Future.Get(ctx, &dialectPath)
		if err != nil {
			logger.Error("⚠️ Failed to process dialect", "Lang", lf.Lang, "Error", err)
			continue
		}
		finalPaths[lf.Lang] = dialectPath
	}

	return ProcessingResult{
		ChunkID:         task.ChunkID,
		Success:         true,
		FinalVideoPaths: finalPaths,
	}, nil
}

// processDialectV2 handles translation -> synthesis -> video -> upscaling
func processDialectV2(ctx workflow.Context, task ChunkTask, sourceAudio string, refAudio []string, lang string, requiresFullVideo bool) (string, error) {
	ctxTrans := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 15 * time.Second})
	var translatedText string
	err := workflow.ExecuteActivity(ctxTrans, TranslateAndVerify, sourceAudio, lang).Get(ctx, &translatedText)
	if err != nil { return "", err }

	ctxSynth := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 20 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 2},
	})
	
	var synthAudioPath string
	err = workflow.ExecuteActivity(ctxSynth, SynthesizeVoice, translatedText, refAudio, lang).Get(ctx, &synthAudioPath)
	if err != nil {
		workflow.GetLogger(ctx).Warn("⚠️ Fish Speech failed, falling back to GPT-SoVITS", "Lang", lang)
		err = workflow.ExecuteActivity(ctxSynth, SynthesizeVoiceFallback, translatedText, lang).Get(ctx, &synthAudioPath)
		if err != nil { return "", err }
	}

	var finalMedia string
	if requiresFullVideo {
		ctxVideo := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 90 * time.Second,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 2},
		})
		var infiniteTalkVideo string
		err = workflow.ExecuteActivity(ctxVideo, GenerateInfiniteTalk, task.VideoPath, synthAudioPath).Get(ctx, &infiniteTalkVideo)
		if err != nil { return "", err }

		ctxGolden := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 60 * time.Second,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 2},
		})
		err = workflow.ExecuteActivity(ctxGolden, ApplyGoldenLayer, infiniteTalkVideo, synthAudioPath).Get(ctx, &finalMedia)
	} else {
		ctxRemux := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Second})
		err = workflow.ExecuteActivity(ctxRemux, RemuxAudioOnly, task.VideoPath, synthAudioPath).Get(ctx, &finalMedia)
	}

	return finalMedia, err
}

// --- ACTIVITIES (Mocks for NATS/Python integration) ---
func DetectFaces(ctx context.Context, videoPath string) (FaceDetectionResult, error) {
	return FaceDetectionResult{Confidence: 0.85, FaceRatio: 0.22}, nil
}
func CleanAudioRNNoise(ctx context.Context, audioPath string) (string, error) {
	return fmt.Sprintf("/shared/rnnoise_%s", extractFilename(audioPath)), nil
}
func TranslateAndVerify(ctx context.Context, audioPath, lang string) (string, error) {
	return fmt.Sprintf("Translated %s", lang), nil
}
func SynthesizeVoice(ctx context.Context, text string, refPaths []string, lang string) (string, error) {
	return fmt.Sprintf("/shared/fishspeech_%s.wav", lang), nil
}
func SynthesizeVoiceFallback(ctx context.Context, text, lang string) (string, error) {
	return fmt.Sprintf("/shared/sovits_%s.wav", lang), nil
}
func GenerateInfiniteTalk(ctx context.Context, videoPath, audioPath string) (string, error) {
	return fmt.Sprintf("/shared/infinite_%s", extractFilename(videoPath)), nil
}
func ApplyGoldenLayer(ctx context.Context, videoPath, audioPath string) (string, error) {
	return fmt.Sprintf("/shared/golden_4k_%s", extractFilename(videoPath)), nil
}
func RemuxAudioOnly(ctx context.Context, videoPath, audioPath string) (string, error) {
	return fmt.Sprintf("/shared/remux_%s", extractFilename(videoPath)), nil
}
func extractFilename(path string) string {
	parts := strings.Split(path, "/")
	return parts[len(parts)-1]
}