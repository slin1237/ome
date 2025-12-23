package distributor

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"
)

const (
	// LeaseTypeLabel identifies the lease as a model download lease.
	LeaseTypeLabel = "ome.io/type"
	// LeaseTypeValue is the value for model download leases.
	LeaseTypeValue = "model-download"
	// LeaseStatusLabel indicates the status of the download.
	LeaseStatusLabel = "ome.io/status"
	// LeaseStatusComplete indicates the download is complete.
	LeaseStatusComplete = "complete"
	// LeaseModelHashLabel stores the model hash for the lease.
	LeaseModelHashLabel = "ome.io/model-hash"
)

// LeaseManager handles Kubernetes Lease-based coordination for model downloads.
// It ensures only one node downloads from HuggingFace at a time while others wait
// for P2P availability.
type LeaseManager struct {
	k8s                  kubernetes.Interface
	namespace            string
	holderIdentity       string
	leaseDurationSeconds int32
	logger               *zap.SugaredLogger
}

// NewLeaseManager creates a new LeaseManager.
func NewLeaseManager(k8s kubernetes.Interface, namespace, holderIdentity string, logger *zap.SugaredLogger) *LeaseManager {
	return &LeaseManager{
		k8s:                  k8s,
		namespace:            namespace,
		holderIdentity:       holderIdentity,
		leaseDurationSeconds: 120, // 2 minutes default
		logger:               logger,
	}
}

// WithLeaseDuration sets a custom lease duration.
func (m *LeaseManager) WithLeaseDuration(seconds int32) *LeaseManager {
	m.leaseDurationSeconds = seconds
	return m
}

// TryAcquire attempts to acquire a lease for the given name.
// Returns true if the lease was acquired, false if another holder has it.
func (m *LeaseManager) TryAcquire(ctx context.Context, name string) (bool, error) {
	now := metav1.NowMicro()

	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				LeaseTypeLabel: LeaseTypeValue,
			},
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &m.holderIdentity,
			AcquireTime:          &now,
			RenewTime:            &now,
			LeaseDurationSeconds: ptr.To(m.leaseDurationSeconds),
		},
	}

	// Try to create the lease
	_, err := m.k8s.CoordinationV1().Leases(m.namespace).Create(ctx, lease, metav1.CreateOptions{})
	if err == nil {
		m.logger.Infof("Successfully acquired lease %s", name)
		return true, nil
	}

	if !errors.IsAlreadyExists(err) {
		return false, fmt.Errorf("failed to create lease: %w", err)
	}

	// Lease exists, check if we can take it over
	existing, getErr := m.k8s.CoordinationV1().Leases(m.namespace).Get(ctx, name, metav1.GetOptions{})
	if getErr != nil {
		return false, fmt.Errorf("failed to get existing lease: %w", getErr)
	}

	// If completed, no need to acquire
	if m.IsComplete(existing) {
		m.logger.Infof("Lease %s is already marked complete", name)
		return false, nil
	}

	// If expired, try to take over
	if m.IsExpired(existing) {
		m.logger.Infof("Lease %s is expired, attempting takeover", name)
		existing.Spec.HolderIdentity = &m.holderIdentity
		existing.Spec.AcquireTime = &now
		existing.Spec.RenewTime = &now
		existing.Spec.LeaseDurationSeconds = ptr.To(m.leaseDurationSeconds)

		_, updateErr := m.k8s.CoordinationV1().Leases(m.namespace).Update(ctx, existing, metav1.UpdateOptions{})
		if updateErr == nil {
			m.logger.Infof("Successfully took over expired lease %s", name)
			return true, nil
		}

		// Someone else took it, that's fine
		if errors.IsConflict(updateErr) {
			m.logger.Infof("Lease %s was taken by another node", name)
			return false, nil
		}

		return false, fmt.Errorf("failed to update lease: %w", updateErr)
	}

	m.logger.Infof("Lease %s is held by %s", name, *existing.Spec.HolderIdentity)
	return false, nil
}

// Renew renews the lease to extend its duration.
func (m *LeaseManager) Renew(ctx context.Context, name string) error {
	lease, err := m.k8s.CoordinationV1().Leases(m.namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get lease for renewal: %w", err)
	}

	// Only renew if we're the holder
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != m.holderIdentity {
		return fmt.Errorf("cannot renew lease: not the holder")
	}

	now := metav1.NowMicro()
	lease.Spec.RenewTime = &now

	_, err = m.k8s.CoordinationV1().Leases(m.namespace).Update(ctx, lease, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to renew lease: %w", err)
	}

	m.logger.Debugf("Renewed lease %s", name)
	return nil
}

// MarkComplete marks the lease as complete, indicating the download finished.
func (m *LeaseManager) MarkComplete(ctx context.Context, name string) error {
	lease, err := m.k8s.CoordinationV1().Leases(m.namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get lease: %w", err)
	}

	if lease.Labels == nil {
		lease.Labels = make(map[string]string)
	}
	lease.Labels[LeaseStatusLabel] = LeaseStatusComplete

	_, err = m.k8s.CoordinationV1().Leases(m.namespace).Update(ctx, lease, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to mark lease complete: %w", err)
	}

	m.logger.Infof("Marked lease %s as complete", name)
	return nil
}

// Release deletes the lease, allowing others to acquire it.
func (m *LeaseManager) Release(ctx context.Context, name string) error {
	err := m.k8s.CoordinationV1().Leases(m.namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("failed to delete lease: %w", err)
	}

	m.logger.Infof("Released lease %s", name)
	return nil
}

// Get retrieves the current lease state.
func (m *LeaseManager) Get(ctx context.Context, name string) (*coordinationv1.Lease, error) {
	return m.k8s.CoordinationV1().Leases(m.namespace).Get(ctx, name, metav1.GetOptions{})
}

// IsExpired checks if a lease has expired based on renew time and duration.
func (m *LeaseManager) IsExpired(lease *coordinationv1.Lease) bool {
	if lease.Spec.RenewTime == nil || lease.Spec.LeaseDurationSeconds == nil {
		return true
	}

	expiry := lease.Spec.RenewTime.Add(time.Duration(*lease.Spec.LeaseDurationSeconds) * time.Second)
	return time.Now().After(expiry)
}

// IsComplete checks if a lease is marked as complete.
func (m *LeaseManager) IsComplete(lease *coordinationv1.Lease) bool {
	if lease.Labels == nil {
		return false
	}
	return lease.Labels[LeaseStatusLabel] == LeaseStatusComplete
}

// GetHolder returns the current holder identity of the lease.
func (m *LeaseManager) GetHolder(lease *coordinationv1.Lease) string {
	if lease.Spec.HolderIdentity == nil {
		return ""
	}
	return *lease.Spec.HolderIdentity
}

// CleanupExpiredLeases removes expired leases that haven't been completed.
// This is useful for cleanup after node failures.
func (m *LeaseManager) CleanupExpiredLeases(ctx context.Context) error {
	leases, err := m.k8s.CoordinationV1().Leases(m.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s", LeaseTypeLabel, LeaseTypeValue),
	})
	if err != nil {
		return fmt.Errorf("failed to list leases: %w", err)
	}

	var lastErr error
	for _, lease := range leases.Items {
		if m.IsExpired(&lease) && !m.IsComplete(&lease) {
			m.logger.Infof("Cleaning up expired lease %s", lease.Name)
			if err := m.Release(ctx, lease.Name); err != nil {
				m.logger.Warnf("Failed to cleanup expired lease %s: %v", lease.Name, err)
				lastErr = err
			}
		}
	}

	return lastErr
}
