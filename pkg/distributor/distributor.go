// Package distributor implements P2P model distribution using BitTorrent protocol.
// It provides efficient model distribution across Kubernetes cluster nodes by:
// - Using BitTorrent for peer-to-peer file transfer
// - Coordinating initial downloads via Kubernetes Lease API
// - Discovering peers via headless Kubernetes Service DNS
// - Supporting hostPath storage for resume capability
package distributor

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
	"k8s.io/client-go/kubernetes"
)

// ModelDistributor coordinates P2P model distribution across cluster nodes.
// It manages the BitTorrent client, peer discovery, and HuggingFace fallback.
type ModelDistributor struct {
	torrentClient *torrent.Client
	k8s           kubernetes.Interface
	dataDir       string
	namespace     string
	podName       string
	podIP         string
	peersService  string // headless service DNS for peer discovery
	torrentPort   int
	metainfoPort  int // HTTP port for sharing .torrent files
	logger        *zap.SugaredLogger

	// Active torrents for seeding
	activeTorrents map[string]*torrent.Torrent
	torrentsMu     sync.RWMutex

	// Lease manager for coordination
	leaseManager *LeaseManager

	// Metrics collector
	metrics *Metrics

	// HTTP client for metainfo fetching
	httpClient *http.Client
}

// New creates a new ModelDistributor with the given configuration.
func New(cfg Config, k8s kubernetes.Interface, logger *zap.SugaredLogger) (*ModelDistributor, error) {
	if logger == nil {
		return nil, fmt.Errorf("logger is required")
	}

	torrentCfg := torrent.NewDefaultClientConfig()
	torrentCfg.DataDir = cfg.DataDir
	torrentCfg.Seed = true
	torrentCfg.ListenPort = cfg.TorrentPort
	torrentCfg.NoDHT = true           // use k8s DNS for discovery instead
	torrentCfg.DisableTrackers = true // no external trackers needed

	// Enable header obfuscation for enhanced security if configured
	if cfg.EnableEncryption {
		torrentCfg.HeaderObfuscationPolicy.Preferred = true
		torrentCfg.HeaderObfuscationPolicy.RequirePreferred = cfg.RequireEncryption
	}

	// Rate limiting to avoid saturating cluster network
	if cfg.MaxDownloadRate > 0 {
		torrentCfg.DownloadRateLimiter = rate.NewLimiter(rate.Limit(cfg.MaxDownloadRate), int(cfg.MaxDownloadRate))
	}
	if cfg.MaxUploadRate > 0 {
		torrentCfg.UploadRateLimiter = rate.NewLimiter(rate.Limit(cfg.MaxUploadRate), int(cfg.MaxUploadRate))
	}

	client, err := torrent.NewClient(torrentCfg)
	if err != nil {
		return nil, fmt.Errorf("create torrent client: %w", err)
	}

	leaseManager := NewLeaseManager(k8s, cfg.Namespace, cfg.PodName, logger)

	metrics := NewMetrics(cfg.Namespace)

	httpClient := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        10,
			MaxIdleConnsPerHost: 5,
			IdleConnTimeout:     60 * time.Second,
		},
	}

	return &ModelDistributor{
		torrentClient:  client,
		k8s:            k8s,
		dataDir:        cfg.DataDir,
		namespace:      cfg.Namespace,
		podName:        cfg.PodName,
		podIP:          cfg.PodIP,
		peersService:   cfg.PeersService,
		torrentPort:    cfg.TorrentPort,
		metainfoPort:   cfg.MetainfoPort,
		logger:         logger,
		activeTorrents: make(map[string]*torrent.Torrent),
		leaseManager:   leaseManager,
		metrics:        metrics,
		httpClient:     httpClient,
	}, nil
}

// Close releases all resources held by the distributor.
func (d *ModelDistributor) Close() {
	d.logger.Info("Shutting down P2P model distributor")

	// Close all active torrents
	d.torrentsMu.Lock()
	for hash, t := range d.activeTorrents {
		d.logger.Infof("Dropping torrent for model %s", hash)
		t.Drop()
	}
	d.activeTorrents = make(map[string]*torrent.Torrent)
	d.torrentsMu.Unlock()

	// Close torrent client
	if d.torrentClient != nil {
		d.torrentClient.Close()
	}

	d.logger.Info("P2P model distributor shutdown complete")
}

// DownloadModel downloads a model using P2P with HF fallback.
// The flow is:
// 1. Check if model already exists locally (from previous download or hostPath)
// 2. Try P2P download from peers
// 3. Acquire lease to become the HF downloader
// 4. Download from HF and seed to other nodes
func (d *ModelDistributor) DownloadModel(ctx context.Context, modelID, modelHash string, hfDownloader HFDownloadFunc) (string, error) {
	modelPath := filepath.Join(d.dataDir, modelHash)

	d.logger.Infof("Starting P2P download for model %s (hash: %s)", modelID, modelHash)
	d.metrics.RecordDownloadStart(modelHash)
	startTime := time.Now()

	// 1. Already have it locally?
	if exists(modelPath) {
		d.logger.Infof("Model %s already exists at %s, starting to seed", modelID, modelPath)
		if err := d.seedExisting(modelPath, modelHash); err != nil {
			d.logger.Warnf("Failed to seed existing model %s: %v", modelID, err)
		}
		d.metrics.RecordDownloadComplete(modelHash, "local", time.Since(startTime))
		return modelPath, nil
	}

	// 2. Try P2P (maybe someone is already seeding)
	d.logger.Infof("Attempting P2P download for model %s", modelID)
	if err := d.tryP2PDownload(ctx, modelHash, modelPath, 30*time.Second); err == nil {
		d.logger.Infof("Successfully downloaded model %s via P2P", modelID)
		d.metrics.RecordDownloadComplete(modelHash, "p2p", time.Since(startTime))
		return modelPath, nil
	} else {
		d.logger.Infof("P2P download not available for %s: %v, trying lease acquisition", modelID, err)
	}

	// 3. Try to become the HF downloader via lease
	leaseName := fmt.Sprintf("ome-model-%s", truncateHash(modelHash, 16))

	acquired, err := d.leaseManager.TryAcquire(ctx, leaseName)
	if err != nil {
		return "", fmt.Errorf("lease error: %w", err)
	}

	if acquired {
		d.logger.Infof("Acquired lease for model %s, downloading from HuggingFace", modelID)
		d.metrics.RecordLeaseAcquired(modelHash)

		// I'm the downloader
		if err := d.downloadFromHFWithRenewal(ctx, leaseName, modelID, modelPath, hfDownloader); err != nil {
			d.leaseManager.Release(ctx, leaseName)
			d.metrics.RecordDownloadFailed(modelHash, "hf_download_error")
			return "", err
		}

		d.leaseManager.MarkComplete(ctx, leaseName)
		if err := d.seedExisting(modelPath, modelHash); err != nil {
			d.logger.Warnf("Failed to start seeding after HF download: %v", err)
		}

		d.metrics.RecordDownloadComplete(modelHash, "hf", time.Since(startTime))
		return modelPath, nil
	}

	// 4. Someone else is downloading - wait for P2P
	d.logger.Infof("Another node is downloading model %s, waiting for P2P availability", modelID)
	d.metrics.RecordWaitingForP2P(modelHash)

	if err := d.waitForP2P(ctx, leaseName, modelHash, modelPath, modelID, hfDownloader); err != nil {
		d.metrics.RecordDownloadFailed(modelHash, "p2p_wait_timeout")
		return "", err
	}

	d.metrics.RecordDownloadComplete(modelHash, "p2p", time.Since(startTime))
	return modelPath, nil
}

// DownloadModelWithVerification downloads and verifies SHA256 checksum.
func (d *ModelDistributor) DownloadModelWithVerification(ctx context.Context, modelID, modelHash, expectedSHA256 string, hfDownloader HFDownloadFunc) (string, error) {
	path, err := d.DownloadModel(ctx, modelID, modelHash, hfDownloader)
	if err != nil {
		return "", err
	}

	if expectedSHA256 != "" {
		d.logger.Infof("Verifying SHA256 checksum for model %s", modelID)
		if err := d.verifyModel(path, expectedSHA256); err != nil {
			d.logger.Errorf("SHA256 verification failed for model %s: %v", modelID, err)
			os.RemoveAll(path)
			d.metrics.RecordVerificationFailed(modelHash)
			return "", err
		}
		d.logger.Infof("SHA256 verification passed for model %s", modelID)
	}

	return path, nil
}

// HFDownloadFunc is a function that downloads a model from HuggingFace.
type HFDownloadFunc func(ctx context.Context, modelID, destPath string) error

// tryP2PDownload attempts to download the model from peers.
func (d *ModelDistributor) tryP2PDownload(ctx context.Context, modelHash, destPath string, timeout time.Duration) error {
	peers, err := d.discoverPeers()
	if err != nil || len(peers) == 0 {
		return fmt.Errorf("no peers available: %v", err)
	}

	d.logger.Infof("Discovered %d peers for P2P download", len(peers))
	d.metrics.RecordPeersDiscovered(modelHash, len(peers))

	// Try to get metainfo from a peer
	mi, err := d.fetchMetainfoFromPeer(ctx, peers, modelHash)
	if err != nil {
		return fmt.Errorf("failed to fetch metainfo: %w", err)
	}

	t, err := d.torrentClient.AddTorrent(mi)
	if err != nil {
		return fmt.Errorf("failed to add torrent: %w", err)
	}

	// Add discovered peers
	peerInfos := make([]torrent.PeerInfo, len(peers))
	for i, p := range peers {
		peerInfos[i] = p
	}
	t.AddPeers(peerInfos)

	// Wait for download with timeout
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	select {
	case <-t.GotInfo():
		t.DownloadAll()
		if !d.waitForComplete(ctx, t) {
			t.Drop()
			return fmt.Errorf("download incomplete within timeout")
		}

		// Store the torrent for seeding
		d.torrentsMu.Lock()
		d.activeTorrents[modelHash] = t
		d.torrentsMu.Unlock()

		return nil
	case <-ctx.Done():
		t.Drop()
		return ctx.Err()
	}
}

// discoverPeers uses DNS to find other pods in the headless service.
func (d *ModelDistributor) discoverPeers() ([]torrent.PeerInfo, error) {
	if d.peersService == "" {
		return nil, fmt.Errorf("peers service not configured")
	}

	ips, err := net.LookupIP(d.peersService)
	if err != nil {
		return nil, fmt.Errorf("DNS lookup failed for %s: %w", d.peersService, err)
	}

	var peers []torrent.PeerInfo
	for _, ip := range ips {
		ipStr := ip.String()
		if ipStr == d.podIP {
			continue // skip self
		}

		addr, err := netip.ParseAddr(ipStr)
		if err != nil {
			d.logger.Warnf("Failed to parse IP %s: %v", ipStr, err)
			continue
		}

		addrPort := netip.AddrPortFrom(addr, uint16(d.torrentPort))
		peers = append(peers, torrent.PeerInfo{
			Addr: addrPort,
		})
	}

	return peers, nil
}

// fetchMetainfoFromPeer tries each peer until one responds with metainfo.
func (d *ModelDistributor) fetchMetainfoFromPeer(ctx context.Context, peers []torrent.PeerInfo, modelHash string) (*metainfo.MetaInfo, error) {
	for _, peer := range peers {
		url := fmt.Sprintf("http://%s:%d/metainfo/%s", peer.Addr.Addr().String(), d.metainfoPort, modelHash)

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			continue
		}

		resp, err := d.httpClient.Do(req)
		if err != nil {
			d.logger.Debugf("Failed to fetch metainfo from %s: %v", url, err)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			d.logger.Debugf("Peer %s returned status %d for model %s", peer.Addr, resp.StatusCode, modelHash)
			continue
		}

		mi, err := metainfo.Load(resp.Body)
		if err != nil {
			d.logger.Warnf("Failed to parse metainfo from %s: %v", url, err)
			continue
		}

		d.logger.Infof("Successfully fetched metainfo for %s from peer %s", modelHash, peer.Addr)
		return mi, nil
	}

	return nil, fmt.Errorf("no peer has metainfo for %s", modelHash)
}

// waitForComplete polls until torrent download is complete.
func (d *ModelDistributor) waitForComplete(ctx context.Context, t *torrent.Torrent) bool {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
			if t.Complete.Bool() {
				return true
			}
			// Log progress
			stats := t.Stats()
			d.logger.Debugf("Download progress: %d/%d bytes", stats.BytesReadData.Int64(), t.Length())
		}
	}
}

// waitForP2P waits for another node to complete download and become available for P2P.
func (d *ModelDistributor) waitForP2P(ctx context.Context, leaseName, modelHash, modelPath, modelID string, hfDownloader HFDownloadFunc) error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	maxWaitTime := 30 * time.Minute
	startTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Check if we've waited too long
			if time.Since(startTime) > maxWaitTime {
				d.logger.Warnf("P2P wait timeout for model %s, attempting lease takeover", modelID)
			}

			// Check if download complete
			lease, err := d.leaseManager.Get(ctx, leaseName)
			if err != nil {
				d.logger.Debugf("Failed to get lease status: %v", err)
				continue
			}

			// If lease holder failed/expired, try to take over
			if d.leaseManager.IsExpired(lease) && !d.leaseManager.IsComplete(lease) {
				d.logger.Infof("Lease expired for model %s, attempting takeover", modelID)
				acquired, err := d.leaseManager.TryAcquire(ctx, leaseName)
				if err != nil {
					d.logger.Warnf("Failed to acquire expired lease: %v", err)
					continue
				}

				if acquired {
					d.logger.Infof("Acquired expired lease for model %s, downloading from HF", modelID)
					if err := d.downloadFromHFWithRenewal(ctx, leaseName, modelID, modelPath, hfDownloader); err != nil {
						d.leaseManager.Release(ctx, leaseName)
						continue
					}
					d.leaseManager.MarkComplete(ctx, leaseName)
					if err := d.seedExisting(modelPath, modelHash); err != nil {
						d.logger.Warnf("Failed to start seeding: %v", err)
					}
					return nil
				}
			}

			// Try P2P download
			if err := d.tryP2PDownload(ctx, modelHash, modelPath, 30*time.Second); err == nil {
				d.logger.Infof("Successfully downloaded model %s via P2P after waiting", modelID)
				if err := d.seedExisting(modelPath, modelHash); err != nil {
					d.logger.Warnf("Failed to start seeding: %v", err)
				}
				return nil
			}
		}
	}
}

// seedExisting creates a torrent for an existing model directory and starts seeding.
func (d *ModelDistributor) seedExisting(path, hash string) error {
	d.torrentsMu.Lock()
	defer d.torrentsMu.Unlock()

	// Check if already seeding
	if _, exists := d.activeTorrents[hash]; exists {
		d.logger.Debugf("Already seeding model %s", hash)
		return nil
	}

	mi, err := d.createMetainfo(path, hash)
	if err != nil {
		return fmt.Errorf("failed to create metainfo: %w", err)
	}

	t, err := d.torrentClient.AddTorrent(mi)
	if err != nil {
		return fmt.Errorf("failed to add torrent for seeding: %w", err)
	}

	<-t.GotInfo()
	d.activeTorrents[hash] = t
	d.metrics.RecordSeeding(hash)

	d.logger.Infof("Started seeding model %s at %s", hash, path)
	return nil
}

// createMetainfo builds a torrent metainfo for the given path.
func (d *ModelDistributor) createMetainfo(path, name string) (*metainfo.MetaInfo, error) {
	info := metainfo.Info{
		PieceLength: 4 * 1024 * 1024, // 4MB pieces for large model files
		Name:        name,
	}

	if err := info.BuildFromFilePath(path); err != nil {
		return nil, fmt.Errorf("failed to build info from path: %w", err)
	}

	infoBytes, err := info.MarshalBencode()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal info: %w", err)
	}

	return &metainfo.MetaInfo{
		InfoBytes: infoBytes,
	}, nil
}

// downloadFromHFWithRenewal downloads from HuggingFace while keeping the lease renewed.
func (d *ModelDistributor) downloadFromHFWithRenewal(ctx context.Context, leaseName, modelID, destPath string, hfDownloader HFDownloadFunc) error {
	renewCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Renew lease in background
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-renewCtx.Done():
				return
			case <-ticker.C:
				if err := d.leaseManager.Renew(renewCtx, leaseName); err != nil {
					d.logger.Warnf("Failed to renew lease %s: %v", leaseName, err)
				}
			}
		}
	}()

	if hfDownloader == nil {
		return fmt.Errorf("HuggingFace downloader function not provided")
	}

	return hfDownloader(ctx, modelID, destPath)
}

// verifyModel checks the SHA256 hash of a downloaded model.
func (d *ModelDistributor) verifyModel(path, expectedSHA256 string) error {
	// For directories, compute hash of all files
	info, err := os.Stat(path)
	if err != nil {
		return err
	}

	if info.IsDir() {
		return d.verifyDirectory(path, expectedSHA256)
	}

	return d.verifyFile(path, expectedSHA256)
}

func (d *ModelDistributor) verifyFile(path, expectedSHA256 string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return err
	}

	actual := fmt.Sprintf("%x", h.Sum(nil))
	if actual != expectedSHA256 {
		return fmt.Errorf("hash mismatch: expected %s, got %s", expectedSHA256, actual)
	}
	return nil
}

func (d *ModelDistributor) verifyDirectory(path, expectedSHA256 string) error {
	h := sha256.New()

	err := filepath.Walk(path, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		f, err := os.Open(filePath)
		if err != nil {
			return err
		}
		defer f.Close()

		if _, err := io.Copy(h, f); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	actual := fmt.Sprintf("%x", h.Sum(nil))
	if actual != expectedSHA256 {
		return fmt.Errorf("hash mismatch: expected %s, got %s", expectedSHA256, actual)
	}
	return nil
}

// GetMetainfo returns the metainfo for a model if it's being seeded.
func (d *ModelDistributor) GetMetainfo(modelHash string) (*metainfo.MetaInfo, bool) {
	d.torrentsMu.RLock()
	defer d.torrentsMu.RUnlock()

	t, exists := d.activeTorrents[modelHash]
	if !exists {
		return nil, false
	}

	info := t.Info()
	if info == nil {
		return nil, false
	}

	infoBytes, err := info.MarshalBencode()
	if err != nil {
		return nil, false
	}

	return &metainfo.MetaInfo{
		InfoBytes: infoBytes,
	}, true
}

// IsSeeding returns true if the distributor is seeding the given model.
func (d *ModelDistributor) IsSeeding(modelHash string) bool {
	d.torrentsMu.RLock()
	defer d.torrentsMu.RUnlock()
	_, exists := d.activeTorrents[modelHash]
	return exists
}

// GetStats returns current statistics about P2P distribution.
func (d *ModelDistributor) GetStats() DistributorStats {
	d.torrentsMu.RLock()
	defer d.torrentsMu.RUnlock()

	stats := DistributorStats{
		ActiveTorrents: len(d.activeTorrents),
	}

	for _, t := range d.activeTorrents {
		ts := t.Stats()
		stats.TotalBytesUploaded += ts.BytesWrittenData.Int64()
		stats.TotalBytesDownloaded += ts.BytesReadData.Int64()
		stats.ActivePeers += ts.ActivePeers
	}

	return stats
}

// DistributorStats contains statistics about P2P distribution.
type DistributorStats struct {
	ActiveTorrents       int
	TotalBytesUploaded   int64
	TotalBytesDownloaded int64
	ActivePeers          int
}

// Helper functions

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func truncateHash(hash string, length int) string {
	if len(hash) <= length {
		return hash
	}
	return hash[:length]
}
