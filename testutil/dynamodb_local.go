package testutil

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os/exec"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	ddbLocalLabelKey     = "godynamodb-queue.dynamodb.local"
	ddbLocalLabelValue   = "1"
	ddbLocalNameLabelKey = "godynamodb-queue.dynamodb.local.name"
	defaultDdbLocalImage = "amazon/dynamodb-local:latest"
)

// DynamoDBLocal controls a dockerized DynamoDB Local instance.
// It is intended for use in integration tests.
type DynamoDBLocal struct {
	Name        string
	Image       string
	containerID string
	cleanups    []func()
	Port        int
	InMemory    bool
	SharedDB    bool
	started     bool
}

// Ensure DynamoDBLocal implements io.Closer
var _ io.Closer = (*DynamoDBLocal)(nil)

// LocalDynamoOption allows customizing the local DynamoDB runner.
type LocalDynamoOption func(*DynamoDBLocal)

// WithPort sets the host port to map to container.
func WithPort(port int) LocalDynamoOption { return func(l *DynamoDBLocal) { l.Port = port } }

// WithImage overrides the docker image (defaults to amazon/dynamodb-local:latest).
func WithImage(img string) LocalDynamoOption { return func(l *DynamoDBLocal) { l.Image = img } }

// WithInMemory toggles the -inMemory flag.
func WithInMemory(enabled bool) LocalDynamoOption {
	return func(l *DynamoDBLocal) { l.InMemory = enabled }
}

// WithSharedDB toggles the -sharedDb flag.
func WithSharedDB(enabled bool) LocalDynamoOption {
	return func(l *DynamoDBLocal) { l.SharedDB = enabled }
}

// NewLocalDynamoDB creates a new, not-yet-started DynamoDBLocal runner.
func NewLocalDynamoDB(name string, opts ...LocalDynamoOption) *DynamoDBLocal {
	d := &DynamoDBLocal{
		Name:     name,
		Image:    defaultDdbLocalImage,
		InMemory: true,
		SharedDB: true,
	}

	for _, o := range opts {
		if o != nil {
			o(d)
		}
	}

	return d
}

// Start launches the dockerized DynamoDB Local and waits for it to accept connections.
func (d *DynamoDBLocal) Start(ctx context.Context) error {
	if d.Name == "" {
		return errors.New("dynamodb local: name is required")
	}

	if d.Port <= 0 {
		p, err := pickFreePort()
		if err != nil {
			return fmt.Errorf("dynamodb local: failed to pick free port: %w", err)
		}

		d.Port = p
	}

	if d.Image == "" {
		d.Image = defaultDdbLocalImage
	}

	// Best-effort: remove any prior container with same name
	_ = runSilent("docker", "rm", "-f", d.Name) //nolint:errcheck // best-effort cleanup, failure is acceptable

	// Build docker run command
	runArgs := []string{
		"run", "-d", "--rm",
		"--name", d.Name,
		"-p", fmt.Sprintf("127.0.0.1:%d:8000", d.Port),
		"--label", fmt.Sprintf("%s=%s", ddbLocalLabelKey, ddbLocalLabelValue),
		"--label", fmt.Sprintf("%s=%s", ddbLocalNameLabelKey, d.Name),
		d.Image,
	}

	// Append command args to image
	runArgs = append(runArgs, "-jar", "DynamoDBLocal.jar")

	if d.InMemory {
		runArgs = append(runArgs, "-inMemory")
	}

	if d.SharedDB {
		runArgs = append(runArgs, "-sharedDb")
	}

	out, err := exec.CommandContext(ctx, "docker", runArgs...).CombinedOutput() //nolint:gosec // G204: docker command is necessary for test infrastructure
	if err != nil {
		return fmt.Errorf("failed to start dynamodb-local: %w: %s", err, string(out))
	}

	d.containerID = firstLine(string(out))
	d.started = true

	// Wait for port to accept connections
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	if err := waitForTCP(waitCtx, "127.0.0.1", d.Port); err != nil {
		_ = d.Close()

		return fmt.Errorf("dynamodb local did not become ready: %w", err)
	}

	return nil
}

// Stop stops the container (but leaves it around because of --rm it will remove).
func (d *DynamoDBLocal) Stop() error {
	if d.Name == "" {
		return nil
	}

	_ = runSilent("docker", "stop", d.Name) //nolint:errcheck // best-effort stop, container may already be stopped
	d.started = false

	return nil
}

// Close removes the container (force).
func (d *DynamoDBLocal) Close() error {
	if d.Name == "" {
		return nil
	}

	for _, cleanup := range d.cleanups {
		if cleanup != nil {
			cleanup()
		}
	}

	d.cleanups = nil

	_ = runSilent("docker", "rm", "-f", d.Name) //nolint:errcheck // best-effort cleanup, failure is acceptable

	d.started = false
	d.containerID = ""

	return nil
}

// EndpointURL returns the HTTP endpoint URL for this local instance.
func (d *DynamoDBLocal) EndpointURL() string {
	return fmt.Sprintf("http://127.0.0.1:%d", d.Port)
}

// AWSConfig returns an AWS SDK config for the local DynamoDB instance.
// Note: Use DynamoDBClient() instead, which properly configures the endpoint.
func (d *DynamoDBLocal) AWSConfig() aws.Config {
	if d == nil {
		return aws.Config{}
	}

	return aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("dummy", "dummy", ""),
	}
}

// DynamoDBClient returns a DynamoDB client configured to connect to the local instance.
func (d *DynamoDBLocal) DynamoDBClient(opts ...func(*dynamodb.Options)) *dynamodb.Client {
	endpoint := d.EndpointURL()

	allOpts := append([]func(*dynamodb.Options){
		func(o *dynamodb.Options) {
			o.BaseEndpoint = &endpoint
		},
	}, opts...)

	return dynamodb.NewFromConfig(d.AWSConfig(), allOpts...)
}

// CreateTable creates a DynamoDB table for the queue.
func (d *DynamoDBLocal) CreateTable(
	ctx context.Context,
	tableName string,
) error {
	client := d.DynamoDBClient()

	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("PK"),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("SK"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("PK"),
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: aws.String("SK"),
				KeyType:       types.KeyTypeRange,
			},
		},
		BillingMode: types.BillingModePayPerRequest,
		TableName:   aws.String(tableName),
	}

	_, err := client.CreateTable(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Wait for table to be active
	for range 30 {
		resp, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
			TableName: aws.String(tableName),
		})

		if err == nil && resp.Table.TableStatus == types.TableStatusActive {
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	return nil
}

// DeleteTable deletes a DynamoDB table.
func (d *DynamoDBLocal) DeleteTable(ctx context.Context, tableName string) error {
	client := d.DynamoDBClient()

	_, err := client.DeleteTable(ctx, &dynamodb.DeleteTableInput{
		TableName: aws.String(tableName),
	})

	return err
}

// WaitForReady waits for DynamoDB Local to be ready to accept requests.
// It uses the ListTables API to verify the database is operational.
func (d *DynamoDBLocal) WaitForReady(ctx context.Context, timeout time.Duration) error {
	client := d.DynamoDBClient()
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		_, err := client.ListTables(ctx, &dynamodb.ListTablesInput{Limit: aws.Int32(1)})
		if err == nil {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("dynamodb local did not become ready within %v", timeout)
}

// ListAllInstances returns the names of all containers tagged as godynamodb-queue DynamoDB local.
func ListAllInstances(ctx context.Context) ([]string, error) {
	//nolint:gosec // G204: docker command is necessary for test infrastructure
	out, err := exec.CommandContext(
		ctx,
		"docker",
		"ps",
		"-a",
		"--filter",
		fmt.Sprintf("label=%s=%s", ddbLocalLabelKey, ddbLocalLabelValue),
		"--format",
		"{{.Names}}",
	).Output()

	if err != nil {
		return nil, err
	}

	return nonEmptyLines(string(out)), nil
}

// CloseAllInstances stops and removes all tagged local instances.
func CloseAllInstances(ctx context.Context) error {
	//nolint:gosec // G204: docker command is necessary for test infrastructure
	out, err := exec.CommandContext(
		ctx,
		"docker",
		"ps",
		"-a",
		"--filter",
		fmt.Sprintf("label=%s=%s", ddbLocalLabelKey, ddbLocalLabelValue),
		"-q",
	).Output()

	if err != nil {
		return err
	}

	ids := nonEmptyLines(string(out))
	if len(ids) == 0 {
		return nil
	}

	args := append([]string{"rm", "-f"}, ids...)

	_, err = exec.Command("docker", args...).CombinedOutput() //nolint:gosec // G204: docker command is necessary for test infrastructure

	return err
}

// --- helpers ---

func pickFreePort() (int, error) {
	for range 5 {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			continue
		}

		addr, ok := ln.Addr().(*net.TCPAddr)
		if !ok {
			_ = ln.Close()
			continue
		}
		port := addr.Port

		_ = ln.Close()

		if port > 0 {
			return port, nil
		}

		time.Sleep(time.Duration(rand.Intn(50)+10) * time.Millisecond) //nolint:gosec // G404: weak rand is fine for test jitter
	}

	return 0, fmt.Errorf("could not determine free port")
}

func waitForTCP(ctx context.Context, host string, port int) error {
	addr := fmt.Sprintf("%s:%d", host, port)

	var d net.Dialer

	for {
		conn, err := d.DialContext(ctx, "tcp", addr)
		if err == nil {
			_ = conn.Close()

			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func firstLine(s string) string {
	for i := range len(s) {
		if s[i] == '\n' || s[i] == '\r' {
			return s[:i]
		}
	}

	return s
}

func nonEmptyLines(s string) []string {
	var out []string

	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			out = append(out, line)
		}
	}

	return out
}

func runSilent(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	cmd.Stdout = nil
	cmd.Stderr = nil

	return cmd.Run()
}
