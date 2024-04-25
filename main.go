package main

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"

	"github.com/minio/cli"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func mainAction(c *cli.Context) error {
	if len(os.Args) < 5 {
		fmt.Println("Usage for tool to list objects with metadata:")
		fmt.Println("list --endpoint <endpoint> --secure <secure> --access-key <accesskey> --secret-key <true/false> --bucket <bucket> --metadata <true/false>")
		return fmt.Errorf("Usage is wrong")
	}
	endpoint := c.String("endpoint")
	accessKey := c.String("access-key")
	secretKey := c.String("secret-key")

	bucketName := c.String("bucket")
	// s3Client, err := minio.New(endpoint, &minio.Options{
	// 	Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
	// 	Secure: secure,
	// })
	s3Client := gets3Client(endpoint, accessKey, secretKey)
	// if err != nil {
	// 	fmt.Println(err)
	// 	return fmt.Errorf("Error creating connections")
	// }

	opts := minio.ListObjectsOptions{
		Recursive:    true,
		WithMetadata: true,
		// WithVersions: true,
	}

	for object := range s3Client.ListObjects(context.Background(), bucketName, opts) {
		if object.Err != nil {
			fmt.Println(object.Err)
			return fmt.Errorf("Error in ListObjects")
		}

		if object.IsDeleteMarker {
			continue
		}
		hasAmzMeta := false
		for name := range object.UserMetadata {
			if strings.HasPrefix(strings.ToLower(name), "x-amz-meta") {
				hasAmzMeta = true
				break
			}

		}

		if !hasAmzMeta {
			fmt.Println(bucketName + ", " + object.Key + ", " + object.VersionID + ", false")
		}

	}
	return nil
}
func main() {
	app := cli.NewApp()
	app.CustomAppHelpTemplate = `NAME:
  {{.Name}} - {{.Usage}}

USAGE:
  {{.Name}} {{if .VisibleFlags}}[FLAGS]{{end}} [DIRS]...

List objects with metadata MSID 

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}
`

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Usage: "bucket name",
			Name:  "bucket",
		},
		cli.StringFlag{
			Usage: "Skip TLS verification",
			Name:  "insecure",
		},
		cli.StringFlag{
			Usage: "access key",
			Name:  "access-key",
		},
		cli.StringFlag{
			Usage: "secret key",
			Name:  "secret-key",
		},
		cli.StringFlag{
			Usage: "endpoint",
			Name:  "endpoint",
		},
	}

	app.Action = mainAction

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func gets3Client(endpoint, accessKey, secretKey string) *minio.Client {
	u, err := url.Parse(endpoint)
	if err != nil {
		log.Fatalln(err)
	}
	secure := strings.EqualFold(u.Scheme, "https")
	transport, err := minio.DefaultTransport(secure)
	if err != nil {
		log.Fatalln(err)
	}
	if transport.TLSClientConfig != nil {
		transport.TLSClientConfig.InsecureSkipVerify = true
	}
	s3Client, err := minio.New(u.Host, &minio.Options{
		Creds:     credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure:    secure,
		Transport: transport,
	})
	if err != nil {
		log.Fatalln(err)
	}
	s3Client.SetAppInfo("traceanddelete", "v3.0")
	return s3Client
}
