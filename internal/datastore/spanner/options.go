package spanner

import (
	"fmt"
	"time"
)

type spannerOptions struct {
	watchBufferLength           uint16
	revisionQuantization        time.Duration
	followerReadDelay           time.Duration
	maxRevisionStalenessPercent float64
	credentialsFilePath         string
	emulatorHost                string
	disableStats                bool
	readMaxOpen                 int
	writeMaxOpen                int
	schemaWatchHeartbeat        time.Duration
}

const (
	errQuantizationTooLarge = "revision quantization (%s) must be less than (%s)"

	defaultRevisionQuantization        = 5 * time.Second
	defaultFollowerReadDelay           = 0 * time.Second
	defaultMaxRevisionStalenessPercent = 0.1
	defaultWatchBufferLength           = 128
	defaultDisableStats                = false
	maxRevisionQuantization            = 24 * time.Hour
	defaultReadMaxOpen                 = 4
	defaultWriteMaxOpen                = 4
	defaultSchemaWatchHeartbeat        = 100 * time.Millisecond
)

// Option provides the facility to configure how clients within the Spanner
// datastore interact with the running Spanner database.
type Option func(*spannerOptions)

func generateConfig(options []Option) (spannerOptions, error) {
	computed := spannerOptions{
		watchBufferLength:           defaultWatchBufferLength,
		revisionQuantization:        defaultRevisionQuantization,
		followerReadDelay:           defaultFollowerReadDelay,
		maxRevisionStalenessPercent: defaultMaxRevisionStalenessPercent,
		disableStats:                defaultDisableStats,
		readMaxOpen:                 defaultReadMaxOpen,
		writeMaxOpen:                defaultWriteMaxOpen,
		schemaWatchHeartbeat:        defaultSchemaWatchHeartbeat,
	}

	for _, option := range options {
		option(&computed)
	}

	// Run any checks on the config that need to be done
	// TODO set a limit to revision quantization?
	if computed.revisionQuantization >= maxRevisionQuantization {
		return computed, fmt.Errorf(
			errQuantizationTooLarge,
			computed.revisionQuantization,
			maxRevisionQuantization,
		)
	}

	return computed, nil
}

// WatchBufferLength is the number of entries that can be stored in the watch
// buffer while awaiting read by the client.
//
// This value defaults to 128.
func WatchBufferLength(watchBufferLength uint16) Option {
	return func(so *spannerOptions) {
		so.watchBufferLength = watchBufferLength
	}
}

// RevisionQuantization is the time bucket size to which advertised revisions
// will be rounded.
//
// This value defaults to 5 seconds.
func RevisionQuantization(bucketSize time.Duration) Option {
	return func(so *spannerOptions) {
		so.revisionQuantization = bucketSize
	}
}

// FollowerReadDelay is the time delay to apply to enable historial reads.
//
// This value defaults to 0 seconds.
func FollowerReadDelay(delay time.Duration) Option {
	return func(so *spannerOptions) {
		so.followerReadDelay = delay
	}
}

// MaxRevisionStalenessPercent is the amount of time, expressed as a percentage of
// the revision quantization window, that a previously computed rounded revision
// can still be advertised after the next rounded revision would otherwise be ready.
//
// This value defaults to 0.1 (10%).
func MaxRevisionStalenessPercent(stalenessPercent float64) Option {
	return func(so *spannerOptions) {
		so.maxRevisionStalenessPercent = stalenessPercent
	}
}

// CredentialsFile is the path to a file containing credentials for a service
// account that can access the cloud spanner instance
func CredentialsFile(path string) Option {
	return func(so *spannerOptions) {
		so.credentialsFilePath = path
	}
}

// EmulatorHost is the URI of a Spanner emulator to connect to for
// development and testing use
func EmulatorHost(uri string) Option {
	return func(so *spannerOptions) {
		so.emulatorHost = uri
	}
}

// DisableStats disables recording counts to the stats table
func DisableStats(disable bool) Option {
	return func(po *spannerOptions) {
		po.disableStats = disable
	}
}

// ReadConnsMaxOpen is the maximum size of the connection pool used for reads.
//
// This value defaults to having 20 connections.
func ReadConnsMaxOpen(conns int) Option {
	return func(po *spannerOptions) { po.readMaxOpen = conns }
}

// WriteConnsMaxOpen is the maximum size of the connection pool used for writes.
//
// This value defaults to having 10 connections.
func WriteConnsMaxOpen(conns int) Option {
	return func(po *spannerOptions) { po.writeMaxOpen = conns }
}

// SchemaWatchHeartbeat is the heartbeat to use for the schema watch, if enabled.
//
// A lower heartbeat means that the schema watch will be able to "catch up" faster.
//
// This value defaults to the minimum heartbeat time of 100ms.
func SchemaWatchHeartbeat(heartbeat time.Duration) Option {
	return func(po *spannerOptions) {
		// NOTE: 100ms is the minimum allowed.
		if heartbeat >= 100*time.Millisecond {
			po.schemaWatchHeartbeat = heartbeat
		}
	}
}
