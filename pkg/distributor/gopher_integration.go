package distributor

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"

	"go.uber.org/zap"
	"k8s.io/client-go/kubernetes"

	"github.com/sgl-project/ome/pkg/xet"
)

// P2PGopher wraps a ModelDistributor to provide P2P-enabled model downloading
// that can be integrated into the existing model-agent architecture.
type P2PGopher struct {
	distributor    *ModelDistributor
	metainfoServer *MetainfoServer
	hfAdapter      *HFAdapter
	config         Config
	logger         *zap.SugaredLogger
	enabled        bool
}

// P2PGopherConfig holds configuration for the P2P-enabled Gopher.
type P2PGopherConfig struct {
	// DataDir is the root directory for models
	DataDir string
	// Namespace is the Kubernetes namespace
	Namespace string
	// PodName is the name of the current pod
	PodName string
	// PodIP is the IP address of the current pod
	PodIP string
	// PeersService is the DNS name of the headless service
	PeersService string
	// EnableP2P controls whether P2P distribution is enabled
	EnableP2P bool
	// XetConfig is the HuggingFace xet configuration
	XetConfig *xet.Config
}

// NewP2PGopher creates a new P2P-enabled Gopher.
func NewP2PGopher(cfg P2PGopherConfig, k8s kubernetes.Interface, logger *zap.SugaredLogger) (*P2PGopher, error) {
	if !cfg.EnableP2P {
		return &P2PGopher{
			enabled: false,
			logger:  logger,
		}, nil
	}

	distConfig := Config{
		DataDir:         cfg.DataDir,
		Namespace:       cfg.Namespace,
		PodName:         cfg.PodName,
		PodIP:           cfg.PodIP,
		PeersService:    cfg.PeersService,
		TorrentPort:     6881,
		MetainfoPort:    8081,
		MaxDownloadRate: 500 * 1024 * 1024, // 500 MB/s
		MaxUploadRate:   500 * 1024 * 1024, // 500 MB/s
		EnableP2P:       true,
	}

	if err := distConfig.Validate(); err != nil {
		logger.Warnf("P2P config validation failed, disabling P2P: %v", err)
		return &P2PGopher{
			enabled: false,
			logger:  logger,
		}, nil
	}

	distributor, err := New(distConfig, k8s, logger)
	if err != nil {
		logger.Warnf("Failed to create P2P distributor, disabling P2P: %v", err)
		return &P2PGopher{
			enabled: false,
			logger:  logger,
		}, nil
	}

	metainfoServer := NewMetainfoServer(cfg.DataDir, distConfig.MetainfoPort, distributor, logger)
	hfAdapter := NewHFAdapter(cfg.XetConfig, k8s, cfg.Namespace, logger)

	return &P2PGopher{
		distributor:    distributor,
		metainfoServer: metainfoServer,
		hfAdapter:      hfAdapter,
		config:         distConfig,
		logger:         logger,
		enabled:        true,
	}, nil
}

// Start starts the P2P services (metainfo server).
func (g *P2PGopher) Start(ctx context.Context) error {
	if !g.enabled {
		return nil
	}

	go func() {
		if err := g.metainfoServer.ServeWithContext(ctx); err != nil {
			g.logger.Errorf("Metainfo server error: %v", err)
		}
	}()

	g.logger.Info("P2P services started")
	return nil
}

// Stop stops the P2P services.
func (g *P2PGopher) Stop() {
	if !g.enabled {
		return
	}

	if g.distributor != nil {
		g.distributor.Close()
	}

	g.logger.Info("P2P services stopped")
}

// DownloadModel downloads a model using P2P if enabled, falling back to HuggingFace.
func (g *P2PGopher) DownloadModel(ctx context.Context, modelID, destPath string, token string) error {
	if !g.enabled {
		// P2P disabled, use direct HF download
		return g.hfAdapter.DownloadWithToken(ctx, modelID, destPath, token)
	}

	// Generate model hash from the model ID
	modelHash := g.generateModelHash(modelID)

	// Create the HF download function with token
	hfDownloadFunc := g.hfAdapter.CreateHFDownloadFuncWithToken(token)

	// Use P2P distributor
	_, err := g.distributor.DownloadModel(ctx, modelID, modelHash, hfDownloadFunc)
	if err != nil {
		return fmt.Errorf("P2P download failed: %w", err)
	}

	// Create symlink from expected destPath to the P2P-managed path if different
	p2pPath := filepath.Join(g.config.DataDir, modelHash)
	if p2pPath != destPath {
		// Ensure parent directory exists
		if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
			return fmt.Errorf("failed to create destination directory: %w", err)
		}
		// Create symlink
		if err := os.Symlink(p2pPath, destPath); err != nil && !os.IsExist(err) {
			g.logger.Warnf("Failed to create symlink from %s to %s: %v", p2pPath, destPath, err)
		}
	}

	return nil
}

// DownloadModelWithRevision downloads a specific revision using P2P if enabled.
func (g *P2PGopher) DownloadModelWithRevision(ctx context.Context, modelID, destPath, revision, token string) error {
	if !g.enabled {
		// P2P disabled, use direct HF download
		return g.hfAdapter.DownloadWithRevision(ctx, modelID, destPath, revision, token)
	}

	// Generate model hash from model ID and revision
	modelHash := g.generateModelHashWithRevision(modelID, revision)

	// Create the HF download function with revision and token
	hfDownloadFunc := g.hfAdapter.CreateHFDownloadFuncWithRevision(revision, token)

	// Use P2P distributor
	_, err := g.distributor.DownloadModel(ctx, modelID, modelHash, hfDownloadFunc)
	if err != nil {
		return fmt.Errorf("P2P download failed: %w", err)
	}

	// Create symlink from expected destPath to the P2P-managed path if different
	p2pPath := filepath.Join(g.config.DataDir, modelHash)
	if p2pPath != destPath {
		if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
			return fmt.Errorf("failed to create destination directory: %w", err)
		}
		if err := os.Symlink(p2pPath, destPath); err != nil && !os.IsExist(err) {
			g.logger.Warnf("Failed to create symlink from %s to %s: %v", p2pPath, destPath, err)
		}
	}

	return nil
}

// IsEnabled returns whether P2P distribution is enabled.
func (g *P2PGopher) IsEnabled() bool {
	return g.enabled
}

// GetStats returns P2P distribution statistics.
func (g *P2PGopher) GetStats() *DistributorStats {
	if !g.enabled || g.distributor == nil {
		return nil
	}
	stats := g.distributor.GetStats()
	return &stats
}

// IsSeeding returns whether the given model is being seeded.
func (g *P2PGopher) IsSeeding(modelID string) bool {
	if !g.enabled || g.distributor == nil {
		return false
	}
	modelHash := g.generateModelHash(modelID)
	return g.distributor.IsSeeding(modelHash)
}

// SeedExistingModel starts seeding an existing model.
func (g *P2PGopher) SeedExistingModel(modelPath, modelID string) error {
	if !g.enabled || g.distributor == nil {
		return nil
	}
	modelHash := g.generateModelHash(modelID)
	return g.distributor.seedExisting(modelPath, modelHash)
}

// generateModelHash generates a deterministic hash for a model ID.
func (g *P2PGopher) generateModelHash(modelID string) string {
	h := sha256.Sum256([]byte(modelID))
	return fmt.Sprintf("%x", h)[:32]
}

// generateModelHashWithRevision generates a deterministic hash for a model ID with revision.
func (g *P2PGopher) generateModelHashWithRevision(modelID, revision string) string {
	h := sha256.Sum256([]byte(modelID + "@" + revision))
	return fmt.Sprintf("%x", h)[:32]
}

// P2PGopherFromEnv creates a P2PGopher from environment variables.
func P2PGopherFromEnv(xetConfig *xet.Config, k8s kubernetes.Interface, logger *zap.SugaredLogger) (*P2PGopher, error) {
	cfg := P2PGopherConfig{
		DataDir:      os.Getenv("MODEL_DIR"),
		Namespace:    os.Getenv("POD_NAMESPACE"),
		PodName:      os.Getenv("POD_NAME"),
		PodIP:        os.Getenv("POD_IP"),
		PeersService: os.Getenv("PEERS_SERVICE"),
		EnableP2P:    os.Getenv("P2P_ENABLED") != "false",
		XetConfig:    xetConfig,
	}

	if cfg.DataDir == "" {
		cfg.DataDir = "/mnt/models"
	}
	if cfg.Namespace == "" {
		cfg.Namespace = "ome"
	}

	return NewP2PGopher(cfg, k8s, logger)
}
