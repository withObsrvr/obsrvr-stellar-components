package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// DataLifecycleManager handles data retention and lifecycle policies
type DataLifecycleManager struct {
	config    *DataLifecycleConfig
	policies  map[string]*RetentionPolicy
	scheduler *LifecycleScheduler
	
	// State management
	mu         sync.RWMutex
	running    bool
	ctx        context.Context
	cancel     context.CancelFunc
	
	// Metrics
	metrics    *LifecycleMetrics
}

// DataLifecycleConfig configures data lifecycle management
type DataLifecycleConfig struct {
	// General settings
	Enabled                bool          `mapstructure:"enabled"`
	CheckInterval         time.Duration `mapstructure:"check_interval"`
	ConcurrentWorkers     int           `mapstructure:"concurrent_workers"`
	
	// Storage paths
	DataPaths             []string      `mapstructure:"data_paths"`
	ArchivePath           string        `mapstructure:"archive_path"`
	TempPath              string        `mapstructure:"temp_path"`
	
	// Default retention policies
	DefaultRetentionDays  int           `mapstructure:"default_retention_days"`
	DefaultArchivalDays   int           `mapstructure:"default_archival_days"`
	
	// Cleanup settings
	CleanupEmptyDirs      bool          `mapstructure:"cleanup_empty_dirs"`
	MinFreeSpacePercent   float64       `mapstructure:"min_free_space_percent"`
	MaxStorageGB          int64         `mapstructure:"max_storage_gb"`
	
	// Compression settings
	EnableCompression     bool          `mapstructure:"enable_compression"`
	CompressionType       string        `mapstructure:"compression_type"`
	CompressionLevel      int           `mapstructure:"compression_level"`
	
	// Notification settings
	NotifyOnCleanup       bool          `mapstructure:"notify_on_cleanup"`
	NotifyOnArchive       bool          `mapstructure:"notify_on_archive"`
	NotifyOnError         bool          `mapstructure:"notify_on_error"`
}

// RetentionPolicy defines data retention rules
type RetentionPolicy struct {
	Name              string        `json:"name"`
	Description       string        `json:"description"`
	
	// Matching criteria
	PathPattern       string        `json:"path_pattern"`
	FilePattern       string        `json:"file_pattern"`
	DataType          string        `json:"data_type"`
	
	// Retention rules
	RetentionDays     int           `json:"retention_days"`
	ArchivalDays      int           `json:"archival_days"`
	DeleteAfterDays   int           `json:"delete_after_days"`
	
	// Actions
	EnableArchival    bool          `json:"enable_archival"`
	EnableCompression bool          `json:"enable_compression"`
	EnableDeletion    bool          `json:"enable_deletion"`
	
	// Advanced settings
	MinFileSize       int64         `json:"min_file_size"`
	MaxFileSize       int64         `json:"max_file_size"`
	PreserveStructure bool          `json:"preserve_structure"`
	
	// Scheduling
	Schedule          string        `json:"schedule"` // Cron expression
	Priority          int           `json:"priority"`
	
	// Metadata
	CreatedAt         time.Time     `json:"created_at"`
	UpdatedAt         time.Time     `json:"updated_at"`
	Active            bool          `json:"active"`
}

// LifecycleScheduler manages scheduled lifecycle tasks
type LifecycleScheduler struct {
	tasks    map[string]*ScheduledTask
	executor *TaskExecutor
	mu       sync.RWMutex
}

// ScheduledTask represents a scheduled lifecycle task
type ScheduledTask struct {
	ID           string                 `json:"id"`
	Name         string                 `json:"name"`
	Policy       *RetentionPolicy       `json:"policy"`
	Schedule     string                 `json:"schedule"`
	NextRun      time.Time              `json:"next_run"`
	LastRun      time.Time              `json:"last_run"`
	Status       TaskStatus             `json:"status"`
	RunCount     int64                  `json:"run_count"`
	ErrorCount   int64                  `json:"error_count"`
	LastError    string                 `json:"last_error,omitempty"`
}

// TaskStatus represents the status of a lifecycle task
type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"
	TaskStatusRunning   TaskStatus = "running"
	TaskStatusCompleted TaskStatus = "completed"
	TaskStatusFailed    TaskStatus = "failed"
	TaskStatusCancelled TaskStatus = "cancelled"
)

// TaskExecutor executes lifecycle tasks
type TaskExecutor struct {
	workerPool chan struct{}
	wg         sync.WaitGroup
}

// LifecycleMetrics tracks lifecycle management metrics
type LifecycleMetrics struct {
	// File metrics
	TotalFilesProcessed   int64         `json:"total_files_processed"`
	FilesArchived        int64         `json:"files_archived"`
	FilesDeleted         int64         `json:"files_deleted"`
	FilesCompressed      int64         `json:"files_compressed"`
	
	// Size metrics
	TotalBytesProcessed  int64         `json:"total_bytes_processed"`
	BytesArchived        int64         `json:"bytes_archived"`
	BytesDeleted         int64         `json:"bytes_deleted"`
	BytesFreed           int64         `json:"bytes_freed"`
	
	// Task metrics
	TasksExecuted        int64         `json:"tasks_executed"`
	TasksSucceeded       int64         `json:"tasks_succeeded"`
	TasksFailed          int64         `json:"tasks_failed"`
	
	// Performance metrics
	AverageTaskDuration  time.Duration `json:"average_task_duration"`
	LastCleanupDuration  time.Duration `json:"last_cleanup_duration"`
	
	// Error metrics
	ErrorCount           int64         `json:"error_count"`
	LastError            string        `json:"last_error,omitempty"`
	LastErrorTime        time.Time     `json:"last_error_time,omitempty"`
	
	// Storage metrics
	CurrentStorageBytes  int64         `json:"current_storage_bytes"`
	FreeSpaceBytes       int64         `json:"free_space_bytes"`
	FreeSpacePercent     float64       `json:"free_space_percent"`
	
	mu                   sync.RWMutex
}

// FileInfo represents information about a file for lifecycle management
type FileInfo struct {
	Path         string    `json:"path"`
	Name         string    `json:"name"`
	Size         int64     `json:"size"`
	ModTime      time.Time `json:"mod_time"`
	IsDir        bool      `json:"is_dir"`
	Age          time.Duration `json:"age"`
	DataType     string    `json:"data_type"`
	Compressed   bool      `json:"compressed"`
	Archived     bool      `json:"archived"`
}

// LifecycleAction represents an action to take on a file
type LifecycleAction struct {
	Type        ActionType  `json:"type"`
	FilePath    string      `json:"file_path"`
	TargetPath  string      `json:"target_path,omitempty"`
	Policy      string      `json:"policy"`
	Reason      string      `json:"reason"`
	Timestamp   time.Time   `json:"timestamp"`
	Priority    int         `json:"priority"`
}

// ActionType defines the type of lifecycle action
type ActionType string

const (
	ActionArchive   ActionType = "archive"
	ActionCompress  ActionType = "compress"
	ActionDelete    ActionType = "delete"
	ActionMove      ActionType = "move"
	ActionVerify    ActionType = "verify"
)

// NewDataLifecycleManager creates a new data lifecycle manager
func NewDataLifecycleManager(config *DataLifecycleConfig) *DataLifecycleManager {
	ctx, cancel := context.WithCancel(context.Background())
	
	dlm := &DataLifecycleManager{
		config:    config,
		policies:  make(map[string]*RetentionPolicy),
		scheduler: NewLifecycleScheduler(config.ConcurrentWorkers),
		ctx:       ctx,
		cancel:    cancel,
		metrics:   &LifecycleMetrics{},
	}
	
	// Initialize default policies
	dlm.initializeDefaultPolicies()
	
	log.Info().
		Bool("enabled", config.Enabled).
		Dur("check_interval", config.CheckInterval).
		Int("concurrent_workers", config.ConcurrentWorkers).
		Msg("Data lifecycle manager initialized")
	
	return dlm
}

// initializeDefaultPolicies sets up default retention policies
func (dlm *DataLifecycleManager) initializeDefaultPolicies() {
	// Parquet data policy
	parquetPolicy := &RetentionPolicy{
		Name:              "parquet_retention",
		Description:       "Retention policy for Parquet files",
		PathPattern:       "*/parquet/*",
		FilePattern:       "*.parquet",
		DataType:          "parquet",
		RetentionDays:     dlm.config.DefaultRetentionDays,
		ArchivalDays:      dlm.config.DefaultArchivalDays,
		DeleteAfterDays:   dlm.config.DefaultRetentionDays + dlm.config.DefaultArchivalDays,
		EnableArchival:    true,
		EnableCompression: dlm.config.EnableCompression,
		EnableDeletion:    true,
		PreserveStructure: true,
		Schedule:          "0 2 * * *", // Daily at 2 AM
		Priority:          1,
		CreatedAt:         time.Now(),
		Active:            true,
	}
	
	// JSON data policy
	jsonPolicy := &RetentionPolicy{
		Name:              "json_retention",
		Description:       "Retention policy for JSON files",
		PathPattern:       "*/json/*",
		FilePattern:       "*.json",
		DataType:          "json",
		RetentionDays:     7,  // Shorter retention for JSON
		ArchivalDays:      14,
		DeleteAfterDays:   21,
		EnableArchival:    true,
		EnableCompression: true,
		EnableDeletion:    true,
		PreserveStructure: false,
		Schedule:          "0 3 * * *", // Daily at 3 AM
		Priority:          2,
		CreatedAt:         time.Now(),
		Active:            true,
	}
	
	// CSV data policy
	csvPolicy := &RetentionPolicy{
		Name:              "csv_retention",
		Description:       "Retention policy for CSV files",
		PathPattern:       "*/csv/*",
		FilePattern:       "*.csv",
		DataType:          "csv",
		RetentionDays:     7,
		ArchivalDays:      14,
		DeleteAfterDays:   21,
		EnableArchival:    true,
		EnableCompression: true,
		EnableDeletion:    true,
		PreserveStructure: false,
		Schedule:          "0 4 * * *", // Daily at 4 AM
		Priority:          3,
		CreatedAt:         time.Now(),
		Active:            true,
	}
	
	// Log files policy
	logPolicy := &RetentionPolicy{
		Name:              "log_retention",
		Description:       "Retention policy for log files",
		PathPattern:       "*/logs/*",
		FilePattern:       "*.log",
		DataType:          "logs",
		RetentionDays:     30,
		ArchivalDays:      0, // No archival for logs
		DeleteAfterDays:   30,
		EnableArchival:    false,
		EnableCompression: false,
		EnableDeletion:    true,
		PreserveStructure: false,
		Schedule:          "0 1 * * *", // Daily at 1 AM
		Priority:          4,
		CreatedAt:         time.Now(),
		Active:            true,
	}
	
	// Register policies
	dlm.policies["parquet_retention"] = parquetPolicy
	dlm.policies["json_retention"] = jsonPolicy
	dlm.policies["csv_retention"] = csvPolicy
	dlm.policies["log_retention"] = logPolicy
	
	log.Info().
		Int("policies_registered", len(dlm.policies)).
		Msg("Default retention policies initialized")
}

// Start starts the data lifecycle manager
func (dlm *DataLifecycleManager) Start() error {
	if !dlm.config.Enabled {
		log.Info().Msg("Data lifecycle management disabled")
		return nil
	}
	
	dlm.mu.Lock()
	if dlm.running {
		dlm.mu.Unlock()
		return fmt.Errorf("data lifecycle manager already running")
	}
	dlm.running = true
	dlm.mu.Unlock()
	
	// Start scheduler
	if err := dlm.scheduler.Start(dlm.ctx); err != nil {
		return fmt.Errorf("failed to start scheduler: %w", err)
	}
	
	// Schedule policy tasks
	for _, policy := range dlm.policies {
		if policy.Active && policy.Schedule != "" {
			task := &ScheduledTask{
				ID:       fmt.Sprintf("task-%s", policy.Name),
				Name:     fmt.Sprintf("Lifecycle task for %s", policy.Name),
				Policy:   policy,
				Schedule: policy.Schedule,
				Status:   TaskStatusPending,
			}
			
			dlm.scheduler.ScheduleTask(task)
		}
	}
	
	// Start periodic cleanup worker
	go dlm.periodicCleanupWorker()
	
	// Start storage monitoring
	go dlm.storageMonitorWorker()
	
	log.Info().Msg("Data lifecycle manager started")
	return nil
}

// Stop stops the data lifecycle manager
func (dlm *DataLifecycleManager) Stop() {
	dlm.mu.Lock()
	if !dlm.running {
		dlm.mu.Unlock()
		return
	}
	dlm.running = false
	dlm.mu.Unlock()
	
	dlm.cancel()
	dlm.scheduler.Stop()
	
	log.Info().Msg("Data lifecycle manager stopped")
}

// AddRetentionPolicy adds a new retention policy
func (dlm *DataLifecycleManager) AddRetentionPolicy(policy *RetentionPolicy) error {
	dlm.mu.Lock()
	defer dlm.mu.Unlock()
	
	if _, exists := dlm.policies[policy.Name]; exists {
		return fmt.Errorf("policy %s already exists", policy.Name)
	}
	
	policy.CreatedAt = time.Now()
	policy.UpdatedAt = time.Now()
	dlm.policies[policy.Name] = policy
	
	// Schedule task if scheduler is running
	if dlm.running && policy.Active && policy.Schedule != "" {
		task := &ScheduledTask{
			ID:       fmt.Sprintf("task-%s", policy.Name),
			Name:     fmt.Sprintf("Lifecycle task for %s", policy.Name),
			Policy:   policy,
			Schedule: policy.Schedule,
			Status:   TaskStatusPending,
		}
		
		dlm.scheduler.ScheduleTask(task)
	}
	
	log.Info().
		Str("policy_name", policy.Name).
		Msg("Retention policy added")
	
	return nil
}

// RemoveRetentionPolicy removes a retention policy
func (dlm *DataLifecycleManager) RemoveRetentionPolicy(policyName string) error {
	dlm.mu.Lock()
	defer dlm.mu.Unlock()
	
	if _, exists := dlm.policies[policyName]; !exists {
		return fmt.Errorf("policy %s not found", policyName)
	}
	
	delete(dlm.policies, policyName)
	
	// Remove scheduled task
	taskID := fmt.Sprintf("task-%s", policyName)
	dlm.scheduler.RemoveTask(taskID)
	
	log.Info().
		Str("policy_name", policyName).
		Msg("Retention policy removed")
	
	return nil
}

// ExecuteCleanup executes cleanup for all active policies
func (dlm *DataLifecycleManager) ExecuteCleanup(ctx context.Context) error {
	startTime := time.Now()
	defer func() {
		dlm.metrics.mu.Lock()
		dlm.metrics.LastCleanupDuration = time.Since(startTime)
		dlm.metrics.mu.Unlock()
	}()
	
	log.Info().Msg("Starting data lifecycle cleanup")
	
	// Get all files to process
	filesToProcess, err := dlm.scanFiles()
	if err != nil {
		return fmt.Errorf("failed to scan files: %w", err)
	}
	
	log.Info().
		Int("files_found", len(filesToProcess)).
		Msg("Files scanned for lifecycle processing")
	
	// Process files according to policies
	actions := dlm.planActions(filesToProcess)
	
	log.Info().
		Int("actions_planned", len(actions)).
		Msg("Lifecycle actions planned")
	
	// Execute actions
	if err := dlm.executeActions(ctx, actions); err != nil {
		return fmt.Errorf("failed to execute actions: %w", err)
	}
	
	// Update storage metrics
	dlm.updateStorageMetrics()
	
	log.Info().
		Dur("duration", time.Since(startTime)).
		Msg("Data lifecycle cleanup completed")
	
	return nil
}

// scanFiles scans configured paths for files
func (dlm *DataLifecycleManager) scanFiles() ([]*FileInfo, error) {
	var allFiles []*FileInfo
	
	for _, dataPath := range dlm.config.DataPaths {
		files, err := dlm.scanPath(dataPath)
		if err != nil {
			log.Error().
				Err(err).
				Str("path", dataPath).
				Msg("Failed to scan path")
			continue
		}
		allFiles = append(allFiles, files...)
	}
	
	return allFiles, nil
}

// scanPath recursively scans a path for files
func (dlm *DataLifecycleManager) scanPath(rootPath string) ([]*FileInfo, error) {
	var files []*FileInfo
	
	err := filepath.Walk(rootPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		
		// Skip directories for now (could be configurable)
		if info.IsDir() {
			return nil
		}
		
		fileInfo := &FileInfo{
			Path:      path,
			Name:      info.Name(),
			Size:      info.Size(),
			ModTime:   info.ModTime(),
			IsDir:     info.IsDir(),
			Age:       time.Since(info.ModTime()),
			DataType:  dlm.inferDataType(path),
			Compressed: dlm.isCompressed(path),
			Archived:  dlm.isArchived(path),
		}
		
		files = append(files, fileInfo)
		return nil
	})
	
	return files, err
}

// planActions determines what actions to take on files
func (dlm *DataLifecycleManager) planActions(files []*FileInfo) []*LifecycleAction {
	var actions []*LifecycleAction
	
	for _, file := range files {
		// Find matching policies
		matchingPolicies := dlm.findMatchingPolicies(file)
		
		for _, policy := range matchingPolicies {
			action := dlm.planActionForFile(file, policy)
			if action != nil {
				actions = append(actions, action)
			}
		}
	}
	
	// Sort actions by priority
	sort.Slice(actions, func(i, j int) bool {
		return actions[i].Priority < actions[j].Priority
	})
	
	return actions
}

// findMatchingPolicies finds policies that match a file
func (dlm *DataLifecycleManager) findMatchingPolicies(file *FileInfo) []*RetentionPolicy {
	var matching []*RetentionPolicy
	
	for _, policy := range dlm.policies {
		if !policy.Active {
			continue
		}
		
		if dlm.policyMatches(policy, file) {
			matching = append(matching, policy)
		}
	}
	
	return matching
}

// policyMatches checks if a policy matches a file
func (dlm *DataLifecycleManager) policyMatches(policy *RetentionPolicy, file *FileInfo) bool {
	// Check data type
	if policy.DataType != "" && policy.DataType != file.DataType {
		return false
	}
	
	// Check path pattern
	if policy.PathPattern != "" {
		matched, _ := filepath.Match(policy.PathPattern, file.Path)
		if !matched {
			return false
		}
	}
	
	// Check file pattern
	if policy.FilePattern != "" {
		matched, _ := filepath.Match(policy.FilePattern, file.Name)
		if !matched {
			return false
		}
	}
	
	// Check file size constraints
	if policy.MinFileSize > 0 && file.Size < policy.MinFileSize {
		return false
	}
	
	if policy.MaxFileSize > 0 && file.Size > policy.MaxFileSize {
		return false
	}
	
	return true
}

// planActionForFile determines what action to take for a file
func (dlm *DataLifecycleManager) planActionForFile(file *FileInfo, policy *RetentionPolicy) *LifecycleAction {
	ageDays := int(file.Age.Hours() / 24)
	
	// Check if file should be deleted
	if policy.EnableDeletion && ageDays >= policy.DeleteAfterDays {
		return &LifecycleAction{
			Type:      ActionDelete,
			FilePath:  file.Path,
			Policy:    policy.Name,
			Reason:    fmt.Sprintf("File age %d days exceeds deletion threshold %d days", ageDays, policy.DeleteAfterDays),
			Timestamp: time.Now(),
			Priority:  policy.Priority,
		}
	}
	
	// Check if file should be archived
	if policy.EnableArchival && !file.Archived && ageDays >= policy.ArchivalDays {
		targetPath := dlm.getArchivePath(file, policy)
		return &LifecycleAction{
			Type:       ActionArchive,
			FilePath:   file.Path,
			TargetPath: targetPath,
			Policy:     policy.Name,
			Reason:     fmt.Sprintf("File age %d days exceeds archival threshold %d days", ageDays, policy.ArchivalDays),
			Timestamp:  time.Now(),
			Priority:   policy.Priority,
		}
	}
	
	// Check if file should be compressed
	if policy.EnableCompression && !file.Compressed && ageDays >= policy.RetentionDays {
		return &LifecycleAction{
			Type:      ActionCompress,
			FilePath:  file.Path,
			Policy:    policy.Name,
			Reason:    fmt.Sprintf("File age %d days exceeds compression threshold %d days", ageDays, policy.RetentionDays),
			Timestamp: time.Now(),
			Priority:  policy.Priority,
		}
	}
	
	return nil
}

// executeActions executes planned lifecycle actions
func (dlm *DataLifecycleManager) executeActions(ctx context.Context, actions []*LifecycleAction) error {
	// Use worker pool for parallel execution
	workerCount := dlm.config.ConcurrentWorkers
	actionChan := make(chan *LifecycleAction, len(actions))
	resultChan := make(chan error, len(actions))
	
	// Start workers
	for i := 0; i < workerCount; i++ {
		go dlm.actionWorker(ctx, actionChan, resultChan)
	}
	
	// Send actions to workers
	for _, action := range actions {
		actionChan <- action
	}
	close(actionChan)
	
	// Collect results
	var errors []error
	for i := 0; i < len(actions); i++ {
		if err := <-resultChan; err != nil {
			errors = append(errors, err)
		}
	}
	
	if len(errors) > 0 {
		return fmt.Errorf("action execution failed with %d errors: %v", len(errors), errors[0])
	}
	
	return nil
}

// actionWorker executes lifecycle actions
func (dlm *DataLifecycleManager) actionWorker(ctx context.Context, actionChan <-chan *LifecycleAction, resultChan chan<- error) {
	for action := range actionChan {
		select {
		case <-ctx.Done():
			resultChan <- ctx.Err()
			return
		default:
		}
		
		err := dlm.executeAction(action)
		if err != nil {
			log.Error().
				Err(err).
				Str("action_type", string(action.Type)).
				Str("file_path", action.FilePath).
				Msg("Failed to execute lifecycle action")
			
			dlm.metrics.mu.Lock()
			dlm.metrics.ErrorCount++
			dlm.metrics.LastError = err.Error()
			dlm.metrics.LastErrorTime = time.Now()
			dlm.metrics.mu.Unlock()
		}
		
		resultChan <- err
	}
}

// executeAction executes a single lifecycle action
func (dlm *DataLifecycleManager) executeAction(action *LifecycleAction) error {
	switch action.Type {
	case ActionDelete:
		return dlm.deleteFile(action.FilePath)
	case ActionArchive:
		return dlm.archiveFile(action.FilePath, action.TargetPath)
	case ActionCompress:
		return dlm.compressFile(action.FilePath)
	case ActionMove:
		return dlm.moveFile(action.FilePath, action.TargetPath)
	case ActionVerify:
		return dlm.verifyFile(action.FilePath)
	default:
		return fmt.Errorf("unknown action type: %s", action.Type)
	}
}

// deleteFile deletes a file
func (dlm *DataLifecycleManager) deleteFile(filePath string) error {
	info, err := os.Stat(filePath)
	if err != nil {
		return err
	}
	
	if err := os.Remove(filePath); err != nil {
		return err
	}
	
	// Update metrics
	dlm.metrics.mu.Lock()
	dlm.metrics.FilesDeleted++
	dlm.metrics.BytesDeleted += info.Size()
	dlm.metrics.BytesFreed += info.Size()
	dlm.metrics.mu.Unlock()
	
	log.Info().
		Str("file", filePath).
		Int64("size", info.Size()).
		Msg("File deleted")
	
	return nil
}

// archiveFile moves a file to archive location
func (dlm *DataLifecycleManager) archiveFile(filePath, targetPath string) error {
	// Create archive directory if it doesn't exist
	if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
		return err
	}
	
	info, err := os.Stat(filePath)
	if err != nil {
		return err
	}
	
	if err := os.Rename(filePath, targetPath); err != nil {
		return err
	}
	
	// Update metrics
	dlm.metrics.mu.Lock()
	dlm.metrics.FilesArchived++
	dlm.metrics.BytesArchived += info.Size()
	dlm.metrics.mu.Unlock()
	
	log.Info().
		Str("source", filePath).
		Str("target", targetPath).
		Int64("size", info.Size()).
		Msg("File archived")
	
	return nil
}

// compressFile compresses a file in place
func (dlm *DataLifecycleManager) compressFile(filePath string) error {
	// This would implement actual compression based on dlm.config.CompressionType
	// For now, just log the action
	
	info, err := os.Stat(filePath)
	if err != nil {
		return err
	}
	
	// Update metrics
	dlm.metrics.mu.Lock()
	dlm.metrics.FilesCompressed++
	dlm.metrics.mu.Unlock()
	
	log.Info().
		Str("file", filePath).
		Int64("size", info.Size()).
		Str("compression_type", dlm.config.CompressionType).
		Msg("File compressed (placeholder)")
	
	return nil
}

// moveFile moves a file to a different location
func (dlm *DataLifecycleManager) moveFile(source, target string) error {
	if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
		return err
	}
	
	return os.Rename(source, target)
}

// verifyFile verifies file integrity
func (dlm *DataLifecycleManager) verifyFile(filePath string) error {
	// Implement file integrity verification
	// For now, just check if file exists and is readable
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	
	return nil
}

// Helper methods
func (dlm *DataLifecycleManager) inferDataType(filePath string) string {
	ext := filepath.Ext(filePath)
	switch ext {
	case ".parquet":
		return "parquet"
	case ".json":
		return "json"
	case ".csv":
		return "csv"
	case ".log":
		return "logs"
	case ".gz", ".zip", ".bz2":
		return "compressed"
	default:
		return "unknown"
	}
}

func (dlm *DataLifecycleManager) isCompressed(filePath string) bool {
	ext := filepath.Ext(filePath)
	return ext == ".gz" || ext == ".zip" || ext == ".bz2"
}

func (dlm *DataLifecycleManager) isArchived(filePath string) bool {
	return filepath.Dir(filePath) == dlm.config.ArchivePath
}

func (dlm *DataLifecycleManager) getArchivePath(file *FileInfo, policy *RetentionPolicy) string {
	if policy.PreserveStructure {
		// Preserve directory structure in archive
		relPath, _ := filepath.Rel(dlm.config.DataPaths[0], file.Path)
		return filepath.Join(dlm.config.ArchivePath, relPath)
	}
	
	// Flat structure in archive
	return filepath.Join(dlm.config.ArchivePath, file.Name)
}

// periodicCleanupWorker runs periodic cleanup tasks
func (dlm *DataLifecycleManager) periodicCleanupWorker() {
	ticker := time.NewTicker(dlm.config.CheckInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			if err := dlm.ExecuteCleanup(dlm.ctx); err != nil {
				log.Error().Err(err).Msg("Periodic cleanup failed")
			}
		case <-dlm.ctx.Done():
			return
		}
	}
}

// storageMonitorWorker monitors storage usage
func (dlm *DataLifecycleManager) storageMonitorWorker() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			dlm.updateStorageMetrics()
			dlm.checkStorageLimits()
		case <-dlm.ctx.Done():
			return
		}
	}
}

// updateStorageMetrics updates current storage metrics
func (dlm *DataLifecycleManager) updateStorageMetrics() {
	// This would implement actual storage usage calculation
	// For now, use placeholder values
	
	dlm.metrics.mu.Lock()
	defer dlm.metrics.mu.Unlock()
	
	// Update storage metrics (placeholder implementation)
	dlm.metrics.CurrentStorageBytes = 1024 * 1024 * 1024 // 1GB placeholder
	dlm.metrics.FreeSpaceBytes = 10 * 1024 * 1024 * 1024 // 10GB placeholder
	dlm.metrics.FreeSpacePercent = 90.0 // 90% free placeholder
}

// checkStorageLimits checks if storage limits are exceeded
func (dlm *DataLifecycleManager) checkStorageLimits() {
	dlm.metrics.mu.RLock()
	freeSpacePercent := dlm.metrics.FreeSpacePercent
	currentStorageGB := dlm.metrics.CurrentStorageBytes / (1024 * 1024 * 1024)
	dlm.metrics.mu.RUnlock()
	
	// Check free space threshold
	if freeSpacePercent < dlm.config.MinFreeSpacePercent {
		log.Warn().
			Float64("free_space_percent", freeSpacePercent).
			Float64("min_threshold", dlm.config.MinFreeSpacePercent).
			Msg("Free space below threshold, triggering emergency cleanup")
		
		// Trigger emergency cleanup
		go func() {
			if err := dlm.ExecuteCleanup(context.Background()); err != nil {
				log.Error().Err(err).Msg("Emergency cleanup failed")
			}
		}()
	}
	
	// Check storage size limit
	if dlm.config.MaxStorageGB > 0 && currentStorageGB > dlm.config.MaxStorageGB {
		log.Warn().
			Int64("current_storage_gb", currentStorageGB).
			Int64("max_storage_gb", dlm.config.MaxStorageGB).
			Msg("Storage size limit exceeded")
	}
}

// GetMetrics returns current lifecycle metrics
func (dlm *DataLifecycleManager) GetMetrics() *LifecycleMetrics {
	dlm.metrics.mu.RLock()
	defer dlm.metrics.mu.RUnlock()
	
	// Return a copy of metrics
	metrics := *dlm.metrics
	return &metrics
}

// GetPolicies returns all retention policies
func (dlm *DataLifecycleManager) GetPolicies() map[string]*RetentionPolicy {
	dlm.mu.RLock()
	defer dlm.mu.RUnlock()
	
	// Return a copy of policies
	policies := make(map[string]*RetentionPolicy)
	for name, policy := range dlm.policies {
		policyCopy := *policy
		policies[name] = &policyCopy
	}
	
	return policies
}

// NewLifecycleScheduler creates a new lifecycle scheduler
func NewLifecycleScheduler(workers int) *LifecycleScheduler {
	return &LifecycleScheduler{
		tasks:    make(map[string]*ScheduledTask),
		executor: &TaskExecutor{
			workerPool: make(chan struct{}, workers),
		},
	}
}

// Start starts the lifecycle scheduler
func (ls *LifecycleScheduler) Start(ctx context.Context) error {
	go ls.schedulerWorker(ctx)
	return nil
}

// Stop stops the lifecycle scheduler
func (ls *LifecycleScheduler) Stop() {
	ls.executor.wg.Wait()
}

// ScheduleTask schedules a new task
func (ls *LifecycleScheduler) ScheduleTask(task *ScheduledTask) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	
	ls.tasks[task.ID] = task
	ls.calculateNextRun(task)
	
	log.Info().
		Str("task_id", task.ID).
		Time("next_run", task.NextRun).
		Msg("Task scheduled")
}

// RemoveTask removes a scheduled task
func (ls *LifecycleScheduler) RemoveTask(taskID string) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	
	delete(ls.tasks, taskID)
	
	log.Info().
		Str("task_id", taskID).
		Msg("Task removed from schedule")
}

// schedulerWorker runs the task scheduler
func (ls *LifecycleScheduler) schedulerWorker(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			ls.processPendingTasks(ctx)
		case <-ctx.Done():
			return
		}
	}
}

// processPendingTasks processes tasks that are ready to run
func (ls *LifecycleScheduler) processPendingTasks(ctx context.Context) {
	ls.mu.RLock()
	now := time.Now()
	var tasksToRun []*ScheduledTask
	
	for _, task := range ls.tasks {
		if task.Status == TaskStatusPending && now.After(task.NextRun) {
			tasksToRun = append(tasksToRun, task)
		}
	}
	ls.mu.RUnlock()
	
	// Execute ready tasks
	for _, task := range tasksToRun {
		select {
		case ls.executor.workerPool <- struct{}{}:
			ls.executor.wg.Add(1)
			go ls.executeTask(ctx, task)
		default:
			log.Warn().
				Str("task_id", task.ID).
				Msg("No available workers, task execution delayed")
		}
	}
}

// executeTask executes a scheduled task
func (ls *LifecycleScheduler) executeTask(ctx context.Context, task *ScheduledTask) {
	defer func() {
		<-ls.executor.workerPool
		ls.executor.wg.Done()
	}()
	
	ls.mu.Lock()
	task.Status = TaskStatusRunning
	task.LastRun = time.Now()
	ls.mu.Unlock()
	
	log.Info().
		Str("task_id", task.ID).
		Str("policy", task.Policy.Name).
		Msg("Executing lifecycle task")
	
	// Task execution would happen here
	// For now, just simulate execution
	time.Sleep(100 * time.Millisecond)
	
	ls.mu.Lock()
	task.Status = TaskStatusCompleted
	task.RunCount++
	ls.calculateNextRun(task)
	ls.mu.Unlock()
	
	log.Info().
		Str("task_id", task.ID).
		Time("next_run", task.NextRun).
		Msg("Task execution completed")
}

// calculateNextRun calculates the next run time for a task
func (ls *LifecycleScheduler) calculateNextRun(task *ScheduledTask) {
	// Simple implementation - would use proper cron parser in production
	task.NextRun = time.Now().Add(24 * time.Hour)
}