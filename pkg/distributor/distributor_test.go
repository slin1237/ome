package distributor

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
)

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name: "valid config",
			config: Config{
				DataDir:      "/mnt/models",
				Namespace:    "ome",
				PodName:      "test-pod",
				PodIP:        "10.0.0.1",
				TorrentPort:  6881,
				MetainfoPort: 8081,
			},
			wantErr: false,
		},
		{
			name: "missing data dir",
			config: Config{
				Namespace:    "ome",
				PodName:      "test-pod",
				PodIP:        "10.0.0.1",
				TorrentPort:  6881,
				MetainfoPort: 8081,
			},
			wantErr: true,
		},
		{
			name: "missing namespace",
			config: Config{
				DataDir:      "/mnt/models",
				PodName:      "test-pod",
				PodIP:        "10.0.0.1",
				TorrentPort:  6881,
				MetainfoPort: 8081,
			},
			wantErr: true,
		},
		{
			name: "missing pod name",
			config: Config{
				DataDir:      "/mnt/models",
				Namespace:    "ome",
				PodIP:        "10.0.0.1",
				TorrentPort:  6881,
				MetainfoPort: 8081,
			},
			wantErr: true,
		},
		{
			name: "missing pod IP",
			config: Config{
				DataDir:      "/mnt/models",
				Namespace:    "ome",
				PodName:      "test-pod",
				TorrentPort:  6881,
				MetainfoPort: 8081,
			},
			wantErr: true,
		},
		{
			name: "same torrent and metainfo port",
			config: Config{
				DataDir:      "/mnt/models",
				Namespace:    "ome",
				PodName:      "test-pod",
				PodIP:        "10.0.0.1",
				TorrentPort:  6881,
				MetainfoPort: 6881,
			},
			wantErr: true,
		},
		{
			name: "invalid torrent port",
			config: Config{
				DataDir:      "/mnt/models",
				Namespace:    "ome",
				PodName:      "test-pod",
				PodIP:        "10.0.0.1",
				TorrentPort:  -1,
				MetainfoPort: 8081,
			},
			wantErr: true,
		},
		{
			name: "negative download rate",
			config: Config{
				DataDir:         "/mnt/models",
				Namespace:       "ome",
				PodName:         "test-pod",
				PodIP:           "10.0.0.1",
				TorrentPort:     6881,
				MetainfoPort:    8081,
				MaxDownloadRate: -1,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConfigWithDefaults(t *testing.T) {
	cfg := Config{}
	result := cfg.WithDefaults()

	assert.Equal(t, "/mnt/models", result.DataDir)
	assert.Equal(t, "ome", result.Namespace)
	assert.Equal(t, 6881, result.TorrentPort)
	assert.Equal(t, 8081, result.MetainfoPort)
	assert.Equal(t, int64(500*1024*1024), result.MaxDownloadRate)
	assert.Equal(t, int64(500*1024*1024), result.MaxUploadRate)
}

func TestLeaseManager(t *testing.T) {
	ctx := context.Background()
	fakeClient := fake.NewSimpleClientset()
	logger := zaptest.NewLogger(t).Sugar()

	lm := NewLeaseManager(fakeClient, "ome", "test-pod", logger)

	t.Run("acquire new lease", func(t *testing.T) {
		acquired, err := lm.TryAcquire(ctx, "test-lease-1")
		require.NoError(t, err)
		assert.True(t, acquired)

		// Verify lease was created
		lease, err := fakeClient.CoordinationV1().Leases("ome").Get(ctx, "test-lease-1", metav1.GetOptions{})
		require.NoError(t, err)
		assert.Equal(t, "test-pod", *lease.Spec.HolderIdentity)
	})

	t.Run("cannot acquire existing lease", func(t *testing.T) {
		// Create a lease held by another pod
		now := metav1.NowMicro()
		existingLease := &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-lease-2",
			},
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity:       ptr.To("other-pod"),
				AcquireTime:          &now,
				RenewTime:            &now,
				LeaseDurationSeconds: ptr.To[int32](120),
			},
		}
		_, err := fakeClient.CoordinationV1().Leases("ome").Create(ctx, existingLease, metav1.CreateOptions{})
		require.NoError(t, err)

		acquired, err := lm.TryAcquire(ctx, "test-lease-2")
		require.NoError(t, err)
		assert.False(t, acquired)
	})

	t.Run("acquire expired lease", func(t *testing.T) {
		// Create an expired lease
		expiredTime := metav1.NewMicroTime(time.Now().Add(-5 * time.Minute))
		expiredLease := &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-lease-3",
			},
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity:       ptr.To("dead-pod"),
				AcquireTime:          &expiredTime,
				RenewTime:            &expiredTime,
				LeaseDurationSeconds: ptr.To[int32](120),
			},
		}
		_, err := fakeClient.CoordinationV1().Leases("ome").Create(ctx, expiredLease, metav1.CreateOptions{})
		require.NoError(t, err)

		acquired, err := lm.TryAcquire(ctx, "test-lease-3")
		require.NoError(t, err)
		assert.True(t, acquired)

		// Verify we took over
		lease, err := fakeClient.CoordinationV1().Leases("ome").Get(ctx, "test-lease-3", metav1.GetOptions{})
		require.NoError(t, err)
		assert.Equal(t, "test-pod", *lease.Spec.HolderIdentity)
	})

	t.Run("mark lease complete", func(t *testing.T) {
		err := lm.MarkComplete(ctx, "test-lease-1")
		require.NoError(t, err)

		lease, err := fakeClient.CoordinationV1().Leases("ome").Get(ctx, "test-lease-1", metav1.GetOptions{})
		require.NoError(t, err)
		assert.Equal(t, LeaseStatusComplete, lease.Labels[LeaseStatusLabel])
	})

	t.Run("is complete check", func(t *testing.T) {
		lease, err := fakeClient.CoordinationV1().Leases("ome").Get(ctx, "test-lease-1", metav1.GetOptions{})
		require.NoError(t, err)
		assert.True(t, lm.IsComplete(lease))
	})

	t.Run("release lease", func(t *testing.T) {
		err := lm.Release(ctx, "test-lease-1")
		require.NoError(t, err)

		_, err = fakeClient.CoordinationV1().Leases("ome").Get(ctx, "test-lease-1", metav1.GetOptions{})
		assert.Error(t, err) // Should be not found
	})
}

func TestLeaseExpiration(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar()
	lm := NewLeaseManager(nil, "ome", "test-pod", logger)

	now := metav1.NowMicro()
	expired := metav1.NewMicroTime(time.Now().Add(-5 * time.Minute))

	tests := []struct {
		name        string
		lease       *coordinationv1.Lease
		wantExpired bool
	}{
		{
			name: "active lease",
			lease: &coordinationv1.Lease{
				Spec: coordinationv1.LeaseSpec{
					RenewTime:            &now,
					LeaseDurationSeconds: ptr.To[int32](120),
				},
			},
			wantExpired: false,
		},
		{
			name: "expired lease",
			lease: &coordinationv1.Lease{
				Spec: coordinationv1.LeaseSpec{
					RenewTime:            &expired,
					LeaseDurationSeconds: ptr.To[int32](120),
				},
			},
			wantExpired: true,
		},
		{
			name: "nil renew time",
			lease: &coordinationv1.Lease{
				Spec: coordinationv1.LeaseSpec{
					LeaseDurationSeconds: ptr.To[int32](120),
				},
			},
			wantExpired: true,
		},
		{
			name: "nil lease duration",
			lease: &coordinationv1.Lease{
				Spec: coordinationv1.LeaseSpec{
					RenewTime: &now,
				},
			},
			wantExpired: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := lm.IsExpired(tt.lease)
			assert.Equal(t, tt.wantExpired, result)
		})
	}
}

func TestExistsHelper(t *testing.T) {
	// Create a temp directory for testing
	tempDir, err := os.MkdirTemp("", "p2p-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	testFile := filepath.Join(tempDir, "test-file")
	err = os.WriteFile(testFile, []byte("test"), 0644)
	require.NoError(t, err)

	assert.True(t, exists(testFile))
	assert.True(t, exists(tempDir))
	assert.False(t, exists(filepath.Join(tempDir, "nonexistent")))
}

func TestTruncateHash(t *testing.T) {
	tests := []struct {
		hash     string
		length   int
		expected string
	}{
		{"abc123def456", 6, "abc123"},
		{"abc", 6, "abc"},
		{"", 6, ""},
		{"abcdefghijklmnop", 8, "abcdefgh"},
	}

	for _, tt := range tests {
		t.Run(tt.hash, func(t *testing.T) {
			result := truncateHash(tt.hash, tt.length)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMetrics(t *testing.T) {
	metrics := NewMetrics("test")
	require.NotNil(t, metrics)

	// Test recording various metrics (should not panic)
	metrics.RecordDownloadStart("hash1")
	metrics.RecordDownloadComplete("hash1", "p2p", 10*time.Second)
	metrics.RecordDownloadFailed("hash2", "timeout")
	metrics.RecordVerificationFailed("hash3")
	metrics.RecordPeersDiscovered("hash1", 5)
	metrics.RecordPeersConnected("hash1", 3)
	metrics.RecordLeaseAcquired("hash1")
	metrics.RecordWaitingForP2P("hash1")
	metrics.RecordSeeding("hash1")
	metrics.RecordBytesUploaded(1000)
	metrics.RecordBytesDownloaded(2000)
	metrics.RecordP2PDownloadBytes("hash1", 1000)
	metrics.RecordHFDownloadBytes("hash1", 5000)
	metrics.RecordMetainfoRequest("success")
	metrics.RecordMetainfoLatency("hash1", 100*time.Millisecond)
	metrics.UpdateP2PRatio(8, 10)
}

func TestP2PGopherDisabled(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar()
	fakeClient := fake.NewSimpleClientset()

	cfg := P2PGopherConfig{
		EnableP2P: false,
	}

	gopher, err := NewP2PGopher(cfg, fakeClient, logger)
	require.NoError(t, err)
	assert.NotNil(t, gopher)
	assert.False(t, gopher.IsEnabled())
	assert.Nil(t, gopher.GetStats())
}

func TestP2PGopherHashGeneration(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar()

	gopher := &P2PGopher{
		logger: logger,
	}

	// Same model ID should produce same hash
	hash1 := gopher.generateModelHash("meta-llama/Llama-2-7b-hf")
	hash2 := gopher.generateModelHash("meta-llama/Llama-2-7b-hf")
	assert.Equal(t, hash1, hash2)

	// Different model ID should produce different hash
	hash3 := gopher.generateModelHash("meta-llama/Llama-2-13b-hf")
	assert.NotEqual(t, hash1, hash3)

	// Hash with revision should be different from hash without
	hashWithRev := gopher.generateModelHashWithRevision("meta-llama/Llama-2-7b-hf", "main")
	assert.NotEqual(t, hash1, hashWithRev)

	// Hash length should be 32
	assert.Equal(t, 32, len(hash1))
}

func TestDistributorStats(t *testing.T) {
	stats := DistributorStats{
		ActiveTorrents:       5,
		TotalBytesUploaded:   1000000,
		TotalBytesDownloaded: 2000000,
		ActivePeers:          10,
	}

	assert.Equal(t, 5, stats.ActiveTorrents)
	assert.Equal(t, int64(1000000), stats.TotalBytesUploaded)
	assert.Equal(t, int64(2000000), stats.TotalBytesDownloaded)
	assert.Equal(t, 10, stats.ActivePeers)
}

// Integration test helpers

func createTestLogger(t *testing.T) *zap.SugaredLogger {
	return zaptest.NewLogger(t).Sugar()
}

func createTestConfig(dataDir string) Config {
	return Config{
		DataDir:                   dataDir,
		Namespace:                 "test",
		PodName:                   "test-pod",
		PodIP:                     "10.0.0.1",
		PeersService:              "test-peers.test.svc.cluster.local",
		TorrentPort:               16881,
		MetainfoPort:              18081,
		MaxDownloadRate:           100 * 1024 * 1024,
		MaxUploadRate:             100 * 1024 * 1024,
		EnableEncryption:          false,
		LeaseDurationSeconds:      60,
		LeaseRenewIntervalSeconds: 15,
		P2PTimeoutSeconds:         10,
		EnableP2P:                 true,
	}
}
