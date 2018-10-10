package main

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"

	"golang.org/x/net/context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	kafka "github.com/segmentio/kafka-go"
)

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}
func streamToBytes(stream io.Reader) []byte {
	buf := new(bytes.Buffer)
	buf.ReadFrom(stream)
	return buf.Bytes()
}
func main() {
	os.Setenv("AWS_ACCESS_KEY_ID", "xxxxxx")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "yyyyyy")
	os.Setenv("AWS_REGION", "us-west-2")
	bucket := "cloudtrails.logs.my"
	// Be specific or else CloudTrail-Digest files will be included
	markerStartWith := "AWSLogs/121212121212/CloudTrail/us-west-2/"

	sess, err := session.NewSession()

	if err != nil {
		fmt.Println("No credentials found")
	}
	svc := s3.New(sess)

	resp, err := svc.ListObjects(
		&s3.ListObjectsInput{
			Bucket: aws.String(bucket),
			Marker: aws.String(markerStartWith),
		})
	if err != nil {
		exitErrorf("Unable to list items in bucket %q, %v", bucket, err)
	}

	s3get := s3manager.NewDownloader(sess)
	buff := &aws.WriteAtBuffer{}
	count := 0

	for _, item := range resp.Contents {
		if *item.Size <= 0 {
			continue
		}

		// Download one object at a time, into the buffer.
		// Each object is a gz file.
		_, err := s3get.Download(buff,
			&s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(*item.Key),
			})
		if err != nil {
			exitErrorf("Unable to download item %q, %v", item, err)
		}
		// gz file gets unziped and breader has json object
		breader, _ := gzip.NewReader(bytes.NewBuffer(buff.Bytes()))
		defer breader.Close()
		//io.Copy(os.Stdout, breader)

		w := kafka.NewWriter(
			kafka.WriterConfig{
				Brokers: []string{"my-kafka-kafka:9092"},
				Topic:   "secaf-cloudtrail-logs",
			})
		w.WriteMessages(
			context.Background(),
			kafka.Message{
				Value: streamToBytes(breader),
			},
		)
		defer w.Close()

		// download three items during testing
		if count >= 3 {
			break
		}
		count++
	}

}
