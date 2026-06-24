/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package keys

// System represents the global system domain
type System struct{}

// Config returns the key for system configuration
func (System) Config() string {
	return Key("main", "cfg")
}

// AllQueues returns the key for registry of all queues
func (System) AllQueues() string {
	return Key("main", "q")
}

// AllExchanges returns the key for registry of all exchanges
func (System) AllExchanges() string {
	return Key("main", "exs")
}

// AllNamespaces returns the key for registry of all namespaces
func (System) AllNamespaces() string {
	return Key("main", "ns")
}

// PurgeJobs returns the key for queue purging jobs
func (System) PurgeJobs() string {
	return Key("main", "pg-jobs")
}

// PendingPurgeJobs returns the key for pending purge jobs
func (System) PendingPurgeJobs() string {
	return Key("main", "pg-jobs", "pend")
}

// ActivePurgeJobs returns the key for currently processing purge jobs
func (System) ActivePurgeJobs() string {
	return Key("main", "pg-jobs", "proc")
}

// JobWorker returns the key for worker processing a specific job
func (System) JobWorker(jobID string) string {
	return Key("main", "jobs", jobID, "wrk")
}

// WorkerHeartbeat returns the key for worker health check
func (System) WorkerHeartbeat(workerID string) string {
	return Key("main", "wrk", workerID, "hb")
}

// ConsumerQueues returns the key for queues a consumer is subscribed to
func (System) ConsumerQueues(consumerID string) string {
	return Key("main", "cons", consumerID, "q")
}

// ConsumerHeartbeat returns the key for consumer health check
func (System) ConsumerHeartbeat(consumerID string) string {
	return Key("main", "cons", consumerID, "hb")
}
