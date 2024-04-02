package database

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"io"
	"log"
	"os"
	"path"
	"time"
)

// DateModifiedTag is the name of the tag on an S3 bucket for storing the modified date information received from
//
//	the MaxMind servers
const DateModifiedTag = "DateOfSourceDatabaseModification"

// S3DatabaseWriter is a databaseWriter that stores the database to a target s3 bucket and key
type S3DatabaseWriter struct {
	s3Client          *s3.Client
	s3Bucket          string
	disableEncryption bool
	verbose           bool
}

// NewS3DatabaseWriter creates a new S3DatabaseWriter, creating necessary locks and temporary files to protect from
//
//	concurrent writes
func NewS3DatabaseWriter(s3Client *s3.Client, s3Bucket string, verbose bool) (*S3DatabaseWriter, error) {
	dbWriter := &S3DatabaseWriter{
		s3Client:          s3Client,
		s3Bucket:          s3Bucket,
		disableEncryption: false,
		verbose:           verbose,
	}

	return dbWriter, nil
}

// GetHash uses the s3 bucket and key to query for the ETag (the MD5) for the S3 object
func (writer *S3DatabaseWriter) GetHash(editionID string) (string, error) {
	objectKey := writer.getObjectKey(editionID)

	response, err := writer.s3Client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(writer.s3Bucket),
		Key:    aws.String(objectKey),
	})

	if err != nil {
		var noSuchKeyErr *types.NoSuchKey
		if errors.As(err, &noSuchKeyErr) {
			return ZeroMD5, nil
		}

		return "", fmt.Errorf("failed to Get Object %s in bucket %s: %w", objectKey, writer.s3Bucket, err)
	}

	return *response.ETag, nil
}

func (writer *S3DatabaseWriter) getObjectKey(editionID string) string {
	return editionID + extension
}

func (writer *S3DatabaseWriter) Write(
	editionID string,
	reader io.ReadCloser,
	newMD5 string,
	_ time.Time,
) (err error) {
	defer func() {
		_, _ = io.Copy(io.Discard, reader) //nolint:errcheck // Best effort.
		if closeErr := reader.Close(); closeErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf("closing reader for %s: %w", editionID, closeErr),
			)
		}
	}()

	key := writer.getObjectKey(editionID)

	tempFile := path.Join("tmp", key) + tempExtension
	fw, err := newFileWriter(tempFile)
	if err != nil {
		return fmt.Errorf("setting up database writer for %s: %w", editionID, err)
	}
	defer func() {
		if closeErr := fw.close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("closing file writer: %w", closeErr))
		}
	}()

	if err = fw.write(reader); err != nil {
		return fmt.Errorf("writing to the temp file for %s: %w", editionID, err)
	}

	// make sure the hash of the temp file matches the expected hash.
	if err = fw.validateHash(newMD5); err != nil {
		return fmt.Errorf("validating hash for %s: %w", editionID, err)
	}

	s3Body, err := os.OpenFile(tempFile, os.O_RDONLY, 0o644)
	if err != nil {
		return fmt.Errorf("opening temp file to read for %s: %w", editionID, err)
	}
	defer func() {
		if closeErr := s3Body.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("closing file reader: %w", closeErr))
		}
	}()

	s3PutObject := &s3.PutObjectInput{
		Bucket:               aws.String(writer.s3Bucket),
		Key:                  aws.String(key),
		ServerSideEncryption: types.ServerSideEncryptionAes256,
		Body:                 s3Body,
	}

	if writer.disableEncryption {
		s3PutObject.ServerSideEncryption = ""
	}
	if _, err := writer.s3Client.PutObject(context.TODO(), s3PutObject); err != nil {
		return fmt.Errorf("encountered an error writing file to S3: %w", err)
	}

	if writer.verbose {
		log.Printf("Database %s successfully updated: %+v", editionID, newMD5)
	}
	return nil
}
