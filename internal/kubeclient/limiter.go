package kubeclient

import (
	"context"
	"sync"
	"time"

	"golang.org/x/time/rate"
	"k8s.io/client-go/util/flowcontrol"
)

// limiter keeps one bucket for the lifetime of the business client. Updates
// preserve token debt; existing Wait reservations retain their original times.
// Both update parameters must be validated before calling set.
type limiter struct {
	mu     sync.Mutex
	bucket *rate.Limiter
}

func newLimiter(qps float64, burst int) *limiter {
	return &limiter{bucket: rate.NewLimiter(rate.Limit(qps), burst)}
}

func (l *limiter) set(qps float64, burst int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.bucket.Limit() == rate.Limit(qps) && l.bucket.Burst() == burst {
		return
	}
	now := time.Now()
	l.bucket.SetLimitAt(now, rate.Limit(qps))
	l.bucket.SetBurstAt(now, burst)
}

func (l *limiter) TryAccept() bool                { return l.bucket.Allow() }
func (l *limiter) Accept()                        { _ = l.bucket.Wait(context.Background()) }
func (l *limiter) Wait(ctx context.Context) error { return l.bucket.Wait(ctx) }
func (l *limiter) QPS() float32                   { return float32(l.bucket.Limit()) }

// Like client-go's token bucket, no background timer needs to be stopped.
// In-flight REST requests are canceled through their contexts.
func (l *limiter) Stop() {}

var _ flowcontrol.RateLimiter = (*limiter)(nil)
