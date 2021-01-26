package model

import "time"

type UpdateNodeStoreAsyncData struct {
	ID         string `json:"id"`
	TotalSpace int    `json:"total_space"`
	SizeChunk  int    `json:"size_chunk"`
	SizeBlk    int    `json:"siz_blk"`
}

type GCSEvent struct {
	Kind                    string                 `json:"kind"`
	ID                      string                 `json:"id"`
	SelfLink                string                 `json:"selfLink"`
	Name                    string                 `json:"name"`
	Bucket                  string                 `json:"bucket"`
	Generation              string                 `json:"generation"`
	Metageneration          string                 `json:"metageneration"`
	ContentType             string                 `json:"contentType"`
	TimeCreated             time.Time              `json:"timeCreated"`
	Updated                 time.Time              `json:"updated"`
	TemporaryHold           bool                   `json:"temporaryHold"`
	EventBasedHold          bool                   `json:"eventBasedHold"`
	RetentionExpirationTime time.Time              `json:"retentionExpirationTime"`
	StorageClass            string                 `json:"storageClass"`
	TimeStorageClassUpdated time.Time              `json:"timeStorageClassUpdated"`
	Size                    string                 `json:"size"`
	MD5Hash                 string                 `json:"md5Hash"`
	MediaLink               string                 `json:"mediaLink"`
	ContentEncoding         string                 `json:"contentEncoding"`
	ContentDisposition      string                 `json:"contentDisposition"`
	CacheControl            string                 `json:"cacheControl"`
	Metadata                map[string]interface{} `json:"metadata"`
	CRC32C                  string                 `json:"crc32c"`
	ComponentCount          int                    `json:"componentCount"`
	Etag                    string                 `json:"etag"`
	CustomerEncryption      struct {
		EncryptionAlgorithm string `json:"encryptionAlgorithm"`
		KeySha256           string `json:"keySha256"`
	}
	KMSKeyName    string `json:"kmsKeyName"`
	ResourceState string `json:"resourceState"`
}

const (
	PATH_TYPE     int = 0
	DOCUMENT_TYPE int = 1
)

type Node struct {
	ID                  string      `json:"id" datastore:"id"`
	Name                string      `json:"name" datastore:"name,omitempty"`
	ContentType         string      `json:"contentType" datastore:"contentType,omitempty"`
	ContentLength       int         `json:"contentLength" datastore:"contentLength,noindex,omitempty"`
	NodeType            int         `json:"type" datastore:"type,omitempty"`
	Owner               string      `json:"owner" datastore:"owner,omitempty"`
	Store               string      `json:"store" datastore:"store,omitempty"`
	Path                string      `json:"path" datastore:"path,omitempty"`
	CreatedDate         string      `json:"createdDate" datastore:"createdDate,omitempty"`
	LastModifiedDate    string      `json:"lastModifiedDate" datastore:"lastModifiedDate,omitempty"`
	ActiveVersionNumber int         `json:"activeVersionNumber" datastore:"activeVersionNumber,omitempty"`
	StorageClass        string      `json:"storageClass" datastore:"storgeClass,omitempty"` // There is an error in the name of this property in the entity in datastore
	Uri                 string      `json:"uri" datastore:"uri,omitempty"`
	Metadata            interface{} `json:"metadata" datastore:"metadata,omitempty"`
	Checksum            string      `json:"checksum" datastore:"checksum,omitempty"`
	Chunks              []Chunk     `json:"chunks" datastore:"chunks,omitempty"`
}

type Chunk struct {
	Offset int    `json:"offset" datastore:"offset"`
	Length int    `json:"length" datastore:"length"`
	ID     string `json:"id" datastore:"id"`
}

type NodeStore struct {
	ID         string `json:"id" datastore:"-"`
	Name       string `json:"name" datastore:"name"`
	NodeType   string `json:"type" datastore:"type,noindex"`
	Path       string `json:"path" datastore:"path,noindex"`
	ReadOnly   bool   `json:"readOnly" datastore:"readOnly,noindex"`
	TotalSpace int    `json:"totalSpace" datastore:"totalSpace"`
	SizeChunk  int    `json:"sizeChunk" datastore:"sizeChunk"`
	SizeBlk    int    `json:"sizeBlk" datastore:"sizeBlk"`
}

type NodeVersion struct {
	ID            string   `json:"id" datastore:"id"`
	NodeID        string   `json:"nodeId" datastore:"nodeId"`
	ContentType   string   `json:"contentType" datastore:"contentType"`
	ContentLength int      `json:"contentLength" datastore:"contentLength,noindex"`
	Store         string   `json:"store" datastore:"store"`
	CreatedDate   string   `json:"createdDate" datastore:"createdDate"`
	Deleted       bool     `json:"deleted" datastore:"deleted"`
	VersionNumber int      `json:"versionNumber" datastore:"versionNumber"`
	StorageClass  string   `json:"storageClass" datastore:"storageClass"`
	Uri           string   `json:"uri" datastore:"uri"`
	Checksum      string   `json:"checksum" datastore:"checksum"`
	Chunks        []*Chunk `json:"chunks" datastore:"chunks"`
}
