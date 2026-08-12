package s3

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"sort"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Config holds S3-compatible storage connection parameters.
type Config struct {
	Endpoint  string `yaml:"endpoint"`
	Bucket    string `yaml:"bucket"`
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
	Region    string `yaml:"region"`
	UseSSL    bool   `yaml:"use_ssl"`
}

// Object represents a single S3 object listing entry.
type Object struct {
	Key  string
	Size int64
}

// Client wraps minio-go for the four operations needed by Agent backup.
type Client struct {
	client *minio.Client
	bucket string
}

// New creates an S3 client. Returns nil (no error) when endpoint is empty,
// allowing Agent to start without S3 configured.
func New(cfg Config) (*Client, error) {
	if cfg.Endpoint == "" {
		return nil, nil
	}
	// Parse endpoint to determine if it already includes a scheme.
	u, err := url.Parse(cfg.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("invalid s3 endpoint %q: %w", cfg.Endpoint, err)
	}
	useSSL := cfg.UseSSL
	if u.Scheme == "https" {
		useSSL = true
	} else if u.Scheme == "http" {
		useSSL = false
	}
	// Host:port without scheme for minio-go.
	endpoint := u.Host
	if endpoint == "" {
		endpoint = cfg.Endpoint
	}

	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}

	mc, err := minio.New(endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure:       useSSL,
		Region:       region,
		BucketLookup: minio.BucketLookupPath,
	})
	if err != nil {
		return nil, fmt.Errorf("create s3 client: %w", err)
	}

	return &Client{client: mc, bucket: cfg.Bucket}, nil
}

// Upload uploads a local file to S3 at the given key.
func (c *Client) Upload(ctx context.Context, key, localPath string) error {
	f, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("open local file %s: %w", localPath, err)
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat local file %s: %w", localPath, err)
	}
	_, err = c.client.PutObject(ctx, c.bucket, key, f, stat.Size(), minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	})
	if err != nil {
		return fmt.Errorf("upload %s to s3: %w", key, err)
	}
	return nil
}

// Download fetches an S3 object and writes it to a local file.
func (c *Client) Download(ctx context.Context, key, localPath string) error {
	obj, err := c.client.GetObject(ctx, c.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("get s3 object %s: %w", key, err)
	}
	defer obj.Close()

	f, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("create local file %s: %w", localPath, err)
	}
	defer f.Close()

	if _, err := io.Copy(f, obj); err != nil {
		return fmt.Errorf("download %s from s3: %w", key, err)
	}
	return nil
}

// Exists checks whether an object exists in S3.
func (c *Client) Exists(ctx context.Context, key string) bool {
	_, err := c.client.StatObject(ctx, c.bucket, key, minio.StatObjectOptions{})
	return err == nil
}

// List returns objects under the given prefix, sorted by key ascending.
func (c *Client) List(ctx context.Context, prefix string) ([]Object, error) {
	var objects []Object
	objCh := c.client.ListObjects(ctx, c.bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	})
	for obj := range objCh {
		if obj.Err != nil {
			return nil, fmt.Errorf("list s3 prefix %q: %w", prefix, obj.Err)
		}
		objects = append(objects, Object{
			Key:  obj.Key,
			Size: obj.Size,
		})
	}
	sort.Slice(objects, func(i, j int) bool {
		return objects[i].Key < objects[j].Key
	})
	return objects, nil
}

// Delete removes multiple objects from S3.
func (c *Client) Delete(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	objsCh := make(chan minio.ObjectInfo, len(keys))
	for _, k := range keys {
		objsCh <- minio.ObjectInfo{Key: k}
	}
	close(objsCh)

	errCh := c.client.RemoveObjects(ctx, c.bucket, objsCh, minio.RemoveObjectsOptions{})
	var errs []string
	for err := range errCh {
		errs = append(errs, err.Err.Error())
	}
	if len(errs) > 0 {
		return fmt.Errorf("delete s3 objects: %s", strings.Join(errs, "; "))
	}
	return nil
}

// DeletePrefix removes all objects under the given prefix.
func (c *Client) DeletePrefix(ctx context.Context, prefix string) error {
	objects, err := c.List(ctx, prefix)
	if err != nil {
		return err
	}
	var keys []string
	for _, obj := range objects {
		keys = append(keys, obj.Key)
	}
	return c.Delete(ctx, keys)
}
