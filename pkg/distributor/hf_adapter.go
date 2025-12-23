package distributor

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	"k8s.io/client-go/kubernetes"

	"github.com/sgl-project/ome/pkg/xet"
)

// HFAdapter adapts the HuggingFace download functionality for use with P2P distribution.
// It provides the HFDownloadFunc implementation that the ModelDistributor needs.
type HFAdapter struct {
	xetConfig  *xet.Config
	logger     *zap.SugaredLogger
	kubeClient kubernetes.Interface
	namespace  string
}

// NewHFAdapter creates a new HuggingFace adapter for P2P distribution.
func NewHFAdapter(xetConfig *xet.Config, kubeClient kubernetes.Interface, namespace string, logger *zap.SugaredLogger) *HFAdapter {
	return &HFAdapter{
		xetConfig:  xetConfig,
		logger:     logger,
		kubeClient: kubeClient,
		namespace:  namespace,
	}
}

// Download implements HFDownloadFunc and downloads a model from HuggingFace.
func (a *HFAdapter) Download(ctx context.Context, modelID, destPath string) error {
	if a.xetConfig == nil {
		return fmt.Errorf("xet config not initialized")
	}

	config := a.xetConfig.ToDownloadConfig()
	config.LocalDir = destPath
	config.RepoID = modelID

	a.logger.Infof("P2P: Downloading model %s from HuggingFace to %s", modelID, destPath)

	// Perform the download using xet
	downloadPath, err := xet.SnapshotDownload(ctx, config)
	if err != nil {
		a.logger.Errorf("P2P: Failed to download model %s from HuggingFace: %v", modelID, err)
		return fmt.Errorf("failed to download from HuggingFace: %w", err)
	}

	a.logger.Infof("P2P: Successfully downloaded model %s to %s", modelID, downloadPath)
	return nil
}

// DownloadWithToken downloads a model using an authentication token.
func (a *HFAdapter) DownloadWithToken(ctx context.Context, modelID, destPath, token string) error {
	if a.xetConfig == nil {
		return fmt.Errorf("xet config not initialized")
	}

	config := a.xetConfig.ToDownloadConfig()
	config.LocalDir = destPath
	config.RepoID = modelID
	if token != "" {
		config.Token = token
	}

	a.logger.Infof("P2P: Downloading model %s from HuggingFace (with token) to %s", modelID, destPath)

	downloadPath, err := xet.SnapshotDownload(ctx, config)
	if err != nil {
		a.logger.Errorf("P2P: Failed to download model %s from HuggingFace: %v", modelID, err)
		return fmt.Errorf("failed to download from HuggingFace: %w", err)
	}

	a.logger.Infof("P2P: Successfully downloaded model %s to %s", modelID, downloadPath)
	return nil
}

// DownloadWithRevision downloads a specific revision of a model.
func (a *HFAdapter) DownloadWithRevision(ctx context.Context, modelID, destPath, revision, token string) error {
	if a.xetConfig == nil {
		return fmt.Errorf("xet config not initialized")
	}

	config := a.xetConfig.ToDownloadConfig()
	config.LocalDir = destPath
	config.RepoID = modelID
	if revision != "" {
		config.Revision = revision
	}
	if token != "" {
		config.Token = token
	}

	a.logger.Infof("P2P: Downloading model %s (revision: %s) from HuggingFace to %s", modelID, revision, destPath)

	downloadPath, err := xet.SnapshotDownload(ctx, config)
	if err != nil {
		a.logger.Errorf("P2P: Failed to download model %s from HuggingFace: %v", modelID, err)
		return fmt.Errorf("failed to download from HuggingFace: %w", err)
	}

	a.logger.Infof("P2P: Successfully downloaded model %s to %s", modelID, downloadPath)
	return nil
}

// CreateHFDownloadFunc creates an HFDownloadFunc from the adapter.
func (a *HFAdapter) CreateHFDownloadFunc() HFDownloadFunc {
	return a.Download
}

// CreateHFDownloadFuncWithToken creates an HFDownloadFunc that uses the provided token.
func (a *HFAdapter) CreateHFDownloadFuncWithToken(token string) HFDownloadFunc {
	return func(ctx context.Context, modelID, destPath string) error {
		return a.DownloadWithToken(ctx, modelID, destPath, token)
	}
}

// CreateHFDownloadFuncWithRevision creates an HFDownloadFunc that uses the provided revision and token.
func (a *HFAdapter) CreateHFDownloadFuncWithRevision(revision, token string) HFDownloadFunc {
	return func(ctx context.Context, modelID, destPath string) error {
		return a.DownloadWithRevision(ctx, modelID, destPath, revision, token)
	}
}
