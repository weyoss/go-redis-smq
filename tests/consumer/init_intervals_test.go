package consumer_test

import (
	"testing"
	"time"

	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	msg "github.com/weyoss/go-redis-smq/pkg/message"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// TestInit_WithWorkerIntervals verifies that a Coordinator constructed
// with fast intervals picks up a queue and spawns workers on the fast
// cadence — proving that WithWorkerIntervals actually reaches the
// coordinator, not just the option signature.
//
// The test creates a queue, produces a scheduled message with a small
// delay, and asserts it becomes pending in well under the production
// ScheduledPublisher cadence (5s). With TestIntervals (100ms), the
// transition happens in ~100-300ms.
func TestInit_WithWorkerIntervals(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-init-intervals")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	_, err := prod.Produce(ctx, msg.New().
		SetBody("interval-probe").
		SetQueue(params).
		SetScheduledDelay(50*time.Millisecond))
	if err != nil {
		t.Fatalf("produce: %v", err)
	}

	// ScheduledPublisher runs every 100ms under TestIntervals. The
	// message is due after 50ms, so within 200ms it should be in
	// Pending, not Scheduled.
	deadline := time.Now().Add(2*testutil.WorkerIntervals().ScheduledPublisher + 500*time.Millisecond)
	qm := redissmq.NewQueueManager()
	for time.Now().Before(deadline) {
		props, err := qm.Properties(ctx, params)
		if err != nil {
			t.Fatalf("properties: %v", err)
		}
		if props.PendingMessagesCount == 1 && props.ScheduledMessagesCount == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("message never became pending — worker intervals likely ignored")
}
