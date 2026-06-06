package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// MultipartUpload represents an active multipart upload session.
type MultipartUpload struct {
	UploadID   string
	Bucket     string
	Object     string
	Parts      map[int][]byte
	LastActive time.Time
	mu         sync.Mutex
	aborted    bool
}

// UploadManager manages multipart uploads.
type UploadManager struct {
	uploads         map[string]*MultipartUpload
	mu              sync.RWMutex
	expiryThreshold time.Duration
}

// NewUploadManager creates a new UploadManager.
func NewUploadManager(expiryThreshold time.Duration) *UploadManager {
	return &UploadManager{
		uploads:         make(map[string]*MultipartUpload),
		expiryThreshold: expiryThreshold,
	}
}

// NewMultipartUpload initiates a new multipart upload.
func (m *UploadManager) NewMultipartUpload(bucket, object string) string {
	m.mu.Lock()
	defer m.mu.Unlock()

	uploadID := fmt.Sprintf("%s-%s-%d", bucket, object, time.Now().UnixNano())
	m.uploads[uploadID] = &MultipartUpload{
		UploadID:   uploadID,
		Bucket:     bucket,
		Object:     object,
		Parts:      make(map[int][]byte),
		LastActive: time.Now(),
	}
	return uploadID
}

// PutObjectPart uploads a part. It respects context cancellation.
func (m *UploadManager) PutObjectPart(ctx context.Context, uploadID string, partNumber int, data []byte) error {
	m.mu.RLock()
	upload, exists := m.uploads[uploadID]
	m.mu.RUnlock()

	if !exists {
		return errors.New("upload not found")
	}

	upload.mu.Lock()
	if upload.aborted {
		upload.mu.Unlock()
		return errors.New("upload already aborted")
	}
	upload.LastActive = time.Now()
	upload.mu.Unlock()

	// Simulate a chunked write to demonstrate context propagation during write
	chunkSize := 10
	written := 0
	tempData := make([]byte, 0, len(data))

	for written < len(data) {
		select {
		case <-ctx.Done():
			// Context cancelled (e.g. client disconnected).
			// Abort and clean up immediately.
			m.AbortMultipartUpload(uploadID)
			return ctx.Err()
		default:
			end := written + chunkSize
			if end > len(data) {
				end = len(data)
			}
			tempData = append(tempData, data[written:end]...)
			written = end
			// Simulate network/disk latency
			time.Sleep(10 * time.Millisecond)
		}
	}

	upload.mu.Lock()
	defer upload.mu.Unlock()
	if upload.aborted {
		return errors.New("upload aborted during write")
	}
	upload.Parts[partNumber] = tempData
	upload.LastActive = time.Now()
	return nil
}

// AbortMultipartUpload aborts the upload and releases all resources.
func (m *UploadManager) AbortMultipartUpload(uploadID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	upload, exists := m.uploads[uploadID]
	if !exists {
		return errors.New("upload not found")
	}

	upload.mu.Lock()
	upload.aborted = true
	// Clear parts to release memory/resources immediately
	upload.Parts = nil
	upload.mu.Unlock()

	delete(m.uploads, uploadID)
	return nil
}

// ListMultipartUploads lists active upload IDs.
func (m *UploadManager) ListMultipartUploads() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var list []string
	for id, upload := range m.uploads {
		upload.mu.Lock()
		if !upload.aborted {
			list = append(list, id)
		}
		upload.mu.Unlock()
	}
	return list
}

// StartCleanupWorker periodically prunes stale uploads.
func (m *UploadManager) StartCleanupWorker(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.mu.Lock()
				now := time.Now()
				for id, upload := range m.uploads {
					upload.mu.Lock()
					if now.Sub(upload.LastActive) > m.expiryThreshold {
						// Clean up resources
						upload.aborted = true
						upload.Parts = nil
						delete(m.uploads, id)
						fmt.Printf("[Cleanup] Pruned stale upload: %s\n", id)
					}
					upload.mu.Unlock()
				}
				m.mu.Unlock()
			}
		}
	}()
}

func main() {
	fmt.Println("Starting Multipart Upload Manager Demonstration...")

	// Initialize manager with 100ms expiry threshold for testing
	mgr := NewUploadManager(100 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start background cleanup worker checking every 50ms
	mgr.StartCleanupWorker(ctx, 50*time.Millisecond)

	// 1. Test Context Propagation (Client Disconnection)
	fmt.Println("\n--- Test 1: Context Propagation on Client Disconnection ---")
	uploadID1 := mgr.NewMultipartUpload("bucket1", "object1")
	fmt.Printf("Initiated upload: %s\n", uploadID1)

	putCtx, putCancel := context.WithCancel(context.Background())
	// Cancel context mid-upload to simulate client disconnection
	go func() {
		time.Sleep(15 * time.Millisecond)
		fmt.Println("Client disconnected (cancelling context)...")
		putCancel()
	}()

	largeData := make([]byte, 100)
	err := mgr.PutObjectPart(putCtx, uploadID1, 1, largeData)
	if err != nil {
		fmt.Printf("PutObjectPart failed as expected: %v\n", err)
	}

	// Verify that the upload is cleaned up immediately and not listed
	uploads := mgr.ListMultipartUploads()
	fmt.Printf("Active uploads after disconnection: %v\n", uploads)

	// 2. Test Stale Upload Cleanup
	fmt.Println("\n--- Test 2: Background Cleanup of Stale Uploads ---")
	uploadID2 := mgr.NewMultipartUpload("bucket1", "object2")
	fmt.Printf("Initiated upload: %s\n", uploadID2)

	// Let it sit idle to exceed the 100ms expiry threshold
	fmt.Println("Waiting for upload to become stale...")
	time.Sleep(200 * time.Millisecond)

	uploads = mgr.ListMultipartUploads()
	fmt.Printf("Active uploads after cleanup: %v\n", uploads)

	fmt.Println("\nDemonstration completed successfully.")
}