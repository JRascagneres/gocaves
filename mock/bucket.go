package mock

import "github.com/couchbaselabs/gocaves/mock/mockdb"

// BucketType specifies the type of bucket
type BucketType uint

// The following lists the possible bucket types
const (
	BucketTypeMemcached       = BucketType(1)
	BucketTypeCouchbase       = BucketType(2)
	BucketTypeMemcachedString = "memcached"
	BucketTypeCouchbaseString = "membase"
)

func (b BucketType) Name() string {
	switch b {
	case BucketTypeMemcached:
		return BucketTypeMemcachedString
	case BucketTypeCouchbase:
		return BucketTypeCouchbaseString
	}

	return ""
}

func BucketTypeFromString(bucketTypeString string) BucketType {
	switch bucketTypeString {
	case BucketTypeMemcachedString:
		return BucketTypeMemcached
	case BucketTypeCouchbaseString:
		return BucketTypeCouchbase
	}

	// Default to Couchbase?
	return BucketTypeCouchbase
}

// NewBucketOptions allows you to specify initial options for a new bucket
type NewBucketOptions struct {
	Name        string
	Type        BucketType
	NumReplicas uint
}

// Bucket represents an instance of a bucket.
type Bucket interface {
	// ID returns the uuid of this bucket.
	ID() string

	// Name returns the name of this bucket
	Name() string

	// BucketType returns the type of bucket this is.
	BucketType() BucketType

	// NumReplicas returns the number of configured replicas for this bucket
	NumReplicas() uint

	// ConfigRev returns the current configuration revision for this bucket.
	ConfigRev() uint

	// CollectionManifest returns the collection manifest of this bucket.
	CollectionManifest() *CollectionManifest

	// Store returns the data-store for this bucket.
	Store() *mockdb.Bucket

	Flush()

	// UpdateVbMap will update the vbmap such that all vbuckets are assigned to the
	// specific nodes which are passed in.  Note that this rebalance is guarenteed to
	// be very explicit such that vbNode = (vbId % numNode), and replicas are just ++.
	UpdateVbMap(nodeList []string)

	// GetVbServerInfo returns the vb nodes, then the vb map, then the ordered list of all nodes
	GetVbServerInfo(reqNode ClusterNode) ([]ClusterNode, [][]int, []ClusterNode)

	// VbucketOwnership returns the replica index associated with the provided node.
	// A response of -1 means it does not own any replicas for that vbucket.
	VbucketOwnership(node ClusterNode) []int

	// ViewIndexManager returns the view index manager for this bucket.
	ViewIndexManager() ViewIndexManager
}
