package spoolctl

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/Rionlyu/spoold/internal/delivery"
)

const (
	defaultServer = "http://127.0.0.1:8080"
	maxResponse   = 2 << 20
)

var errHelp = errors.New("help requested")

type usageError struct {
	err error
}

func (e usageError) Error() string {
	return e.err.Error()
}

type createRequest struct {
	IdempotencyKey string            `json:"idempotencyKey,omitempty"`
	TargetURL      string            `json:"targetUrl"`
	Method         string            `json:"method,omitempty"`
	Headers        map[string]string `json:"headers,omitempty"`
	Body           json.RawMessage   `json:"body,omitempty"`
	BodyBase64     string            `json:"bodyBase64,omitempty"`
	MaxAttempts    int               `json:"maxAttempts,omitempty"`
}

type listResponse struct {
	Deliveries []delivery.Delivery `json:"deliveries"`
	Count      int                 `json:"count"`
}

type errorResponse struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

type apiError struct {
	StatusCode int
	Code       string
	Message    string
}

func (e apiError) Error() string {
	if e.Code == "" {
		return fmt.Sprintf("spoold returned HTTP %d: %s", e.StatusCode, e.Message)
	}
	return fmt.Sprintf("spoold returned HTTP %d (%s): %s", e.StatusCode, e.Code, e.Message)
}

type client struct {
	baseURL    *url.URL
	displayURL string
	httpClient *http.Client
}

type apiRequest struct {
	method string
	path   string
	query  url.Values
	body   any
}

// Run executes spoolctl and returns a process exit code.
func Run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		printUsage(stderr)
		return 2
	}

	var err error
	switch args[0] {
	case "send":
		err = runSend(ctx, args[1:], stdin, stdout, stderr)
	case "list":
		err = runList(ctx, args[1:], stdout, stderr)
	case "get":
		err = runDeliveryCommand(ctx, "get", http.MethodGet, args[1:], stdout, stderr)
	case "retry":
		err = runDeliveryCommand(ctx, "retry", http.MethodPost, args[1:], stdout, stderr)
	case "cancel":
		err = runDeliveryCommand(ctx, "cancel", http.MethodPost, args[1:], stdout, stderr)
	case "help", "-h", "--help":
		printUsage(stdout)
		return 0
	default:
		fmt.Fprintf(stderr, "spoolctl: unknown command %q\n\n", args[0])
		printUsage(stderr)
		return 2
	}

	switch {
	case err == nil:
		return 0
	case errors.Is(err, errHelp):
		return 0
	default:
		var usage usageError
		if errors.As(err, &usage) {
			fmt.Fprintf(stderr, "spoolctl: %s\n", usage.err)
			return 2
		}
		fmt.Fprintf(stderr, "spoolctl: %s\n", err)
		return 1
	}
}

func runSend(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	flags := newFlagSet("send", stderr)
	server := flags.String("server", serverURL(), "spoold API base URL")
	method := flags.String("method", http.MethodPost, "outbound HTTP method")
	idempotencyKey := flags.String("idempotency-key", "", "deduplicate equivalent submissions with this key")
	maxAttempts := flags.Int("max-attempts", 0, "maximum delivery attempts (server default: 8)")
	data := flags.String("data", "", "JSON request body")
	dataFile := flags.String("data-file", "", "read JSON request body from a file, or - for stdin")
	dataBinary := flags.String("data-binary", "", "read an arbitrary request body from a file, or - for stdin")
	jsonOutput := flags.Bool("json", false, "print the API response as JSON")
	headers := make(headerValues)
	flags.Var(&headers, "header", "outbound header in 'Name: value' form; repeatable")
	flags.Usage = func() {
		fmt.Fprintln(stderr, "Usage: spoolctl send [options] <target-url>")
		fmt.Fprintln(stderr)
		flags.PrintDefaults()
	}
	if err := parseFlags(flags, args); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		flags.Usage()
		return usageError{errors.New("send requires exactly one target URL")}
	}
	bodySources := 0
	for _, source := range []string{*data, *dataFile, *dataBinary} {
		if source != "" {
			bodySources++
		}
	}
	if bodySources > 1 {
		return usageError{errors.New("--data, --data-file, and --data-binary cannot be used together")}
	}

	bodyPath := *dataFile
	if *dataBinary != "" {
		bodyPath = *dataBinary
	}
	body, err := readBody(*data, bodyPath, stdin)
	if err != nil {
		return err
	}
	binaryBody := *dataBinary != ""
	if !binaryBody && len(body) > 0 && !json.Valid(body) {
		return usageError{errors.New("request body must be valid JSON")}
	}

	api, err := newClient(*server)
	if err != nil {
		return usageError{err}
	}
	var item delivery.Delivery
	payload := createRequest{
		IdempotencyKey: *idempotencyKey,
		TargetURL:      flags.Arg(0),
		Method:         *method,
		Headers:        headers,
		MaxAttempts:    *maxAttempts,
	}
	if binaryBody {
		payload.BodyBase64 = base64.StdEncoding.EncodeToString(body)
	} else {
		payload.Body = body
	}
	status, err := api.do(ctx, apiRequest{
		method: http.MethodPost,
		path:   "/v1/deliveries",
		body:   payload,
	}, &item)
	if err != nil {
		return err
	}

	if *jsonOutput {
		return writeJSON(stdout, item)
	}
	action := "existing"
	if status == http.StatusCreated {
		action = "queued"
	}
	return printDeliveries(stdout, action, []delivery.Delivery{item}, false)
}

func runList(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := newFlagSet("list", stderr)
	server := flags.String("server", serverURL(), "spoold API base URL")
	status := flags.String("status", "", "filter by delivery status")
	limit := flags.Int("limit", 100, "maximum deliveries to return (1-500)")
	jsonOutput := flags.Bool("json", false, "print the API response as JSON")
	flags.Usage = func() {
		fmt.Fprintln(stderr, "Usage: spoolctl list [options]")
		fmt.Fprintln(stderr)
		flags.PrintDefaults()
	}
	if err := parseFlags(flags, args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		flags.Usage()
		return usageError{errors.New("list does not accept positional arguments")}
	}
	if *limit < 1 || *limit > 500 {
		return usageError{errors.New("--limit must be between 1 and 500")}
	}

	api, err := newClient(*server)
	if err != nil {
		return usageError{err}
	}
	query := url.Values{"limit": {strconv.Itoa(*limit)}}
	if *status != "" {
		query.Set("status", *status)
	}
	var response listResponse
	if _, err := api.do(ctx, apiRequest{
		method: http.MethodGet,
		path:   "/v1/deliveries",
		query:  query,
	}, &response); err != nil {
		return err
	}
	if *jsonOutput {
		return writeJSON(stdout, response)
	}
	return printDeliveries(stdout, "", response.Deliveries, true)
}

func runDeliveryCommand(ctx context.Context, command, method string, args []string, stdout, stderr io.Writer) error {
	flags := newFlagSet(command, stderr)
	server := flags.String("server", serverURL(), "spoold API base URL")
	jsonOutput := flags.Bool("json", false, "print the API response as JSON")
	flags.Usage = func() {
		fmt.Fprintf(stderr, "Usage: spoolctl %s [options] <delivery-id>\n", command)
		fmt.Fprintln(stderr)
		flags.PrintDefaults()
	}
	if err := parseFlags(flags, args); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		flags.Usage()
		return usageError{fmt.Errorf("%s requires exactly one delivery ID", command)}
	}

	api, err := newClient(*server)
	if err != nil {
		return usageError{err}
	}
	path := "/v1/deliveries/" + url.PathEscape(flags.Arg(0))
	if command != "get" {
		path += "/" + command
	}
	var item delivery.Delivery
	if _, err := api.do(ctx, apiRequest{
		method: method,
		path:   path,
	}, &item); err != nil {
		return err
	}
	if *jsonOutput {
		return writeJSON(stdout, item)
	}
	if err := printDeliveries(stdout, command, []delivery.Delivery{item}, false); err != nil {
		return err
	}
	if item.LastError != "" {
		if _, err := fmt.Fprintf(stdout, "last error: %s\n", item.LastError); err != nil {
			return err
		}
	}
	return nil
}

func newFlagSet(name string, stderr io.Writer) *flag.FlagSet {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(stderr)
	return flags
}

func parseFlags(flags *flag.FlagSet, args []string) error {
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return errHelp
		}
		return usageError{err}
	}
	return nil
}

func readBody(inline, path string, stdin io.Reader) (json.RawMessage, error) {
	if inline != "" {
		return json.RawMessage(inline), nil
	}
	if path == "" {
		return nil, nil
	}

	var reader io.Reader
	var file *os.File
	if path == "-" {
		reader = stdin
	} else {
		var err error
		file, err = os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("open data file: %w", err)
		}
		defer file.Close()
		reader = file
	}
	body, err := io.ReadAll(io.LimitReader(reader, delivery.MaxBodyBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read request body: %w", err)
	}
	if len(body) > delivery.MaxBodyBytes {
		return nil, fmt.Errorf("request body exceeds %d bytes", delivery.MaxBodyBytes)
	}
	return json.RawMessage(body), nil
}

func printDeliveries(output io.Writer, action string, items []delivery.Delivery, header bool) error {
	table := tabwriter.NewWriter(output, 0, 4, 2, ' ', 0)
	if header {
		fmt.Fprintln(table, "ID\tSTATUS\tATTEMPTS\tMETHOD\tTARGET")
	}
	for _, item := range items {
		prefix := ""
		if action != "" {
			prefix = action + "\t"
		}
		fmt.Fprintf(table, "%s%s\t%s\t%d/%d\t%s\t%s\n",
			prefix,
			item.ID,
			item.Status,
			item.Attempts,
			item.MaxAttempts,
			item.Method,
			item.TargetURL,
		)
	}
	return table.Flush()
}

func writeJSON(output io.Writer, value any) error {
	encoder := json.NewEncoder(output)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(value)
}

func printUsage(output io.Writer) {
	fmt.Fprintln(output, "spoolctl submits and manages crash-safe HTTP deliveries through a local spoold.")
	fmt.Fprintln(output)
	fmt.Fprintln(output, "Usage: spoolctl <command> [options]")
	fmt.Fprintln(output)
	fmt.Fprintln(output, "Commands:")
	fmt.Fprintln(output, "  send     persist and enqueue an HTTP delivery")
	fmt.Fprintln(output, "  list     list deliveries")
	fmt.Fprintln(output, "  get      inspect one delivery")
	fmt.Fprintln(output, "  retry    start a new retry cycle for a failed delivery")
	fmt.Fprintln(output, "  cancel   cancel pending or in-flight delivery work")
	fmt.Fprintln(output)
	fmt.Fprintln(output, "Set SPOOLD_URL to override the default http://127.0.0.1:8080 API.")
}

func serverURL() string {
	if value := strings.TrimSpace(os.Getenv("SPOOLD_URL")); value != "" {
		return value
	}
	return defaultServer
}

func newClient(rawURL string) (*client, error) {
	rawURL = strings.TrimSpace(rawURL)
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("parse --server: %w", err)
	}
	if parsed.Scheme == "unix" {
		return newUnixClient(rawURL, parsed)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, errors.New("--server must use http, https, or unix")
	}
	if parsed.Host == "" {
		return nil, errors.New("--server must include a host")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, errors.New("--server must not include user information, a query, or a fragment")
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	return &client{
		baseURL:    parsed,
		displayURL: rawURL,
		httpClient: &http.Client{
			Timeout: 20 * time.Second,
		},
	}, nil
}

func (c *client) do(ctx context.Context, spec apiRequest, responseBody any) (int, error) {
	endpoint := *c.baseURL
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + spec.path
	endpoint.RawQuery = spec.query.Encode()

	var body io.Reader
	if spec.body != nil {
		encoded, err := json.Marshal(spec.body)
		if err != nil {
			return 0, fmt.Errorf("encode request: %w", err)
		}
		body = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, spec.method, endpoint.String(), body)
	if err != nil {
		return 0, fmt.Errorf("create request: %w", err)
	}
	if spec.body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	request.Header.Set("Accept", "application/json")

	response, err := c.httpClient.Do(request)
	if err != nil {
		return 0, fmt.Errorf("contact spoold at %s: %w", c.displayURL, err)
	}
	defer response.Body.Close()
	responseData, err := io.ReadAll(io.LimitReader(response.Body, maxResponse+1))
	if err != nil {
		return response.StatusCode, fmt.Errorf("read spoold response: %w", err)
	}
	if len(responseData) > maxResponse {
		return response.StatusCode, errors.New("spoold response exceeds size limit")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var remote errorResponse
		apiErr := apiError{StatusCode: response.StatusCode}
		if json.Unmarshal(responseData, &remote) == nil {
			apiErr.Code = remote.Error.Code
			apiErr.Message = strings.TrimSpace(remote.Error.Message)
		}
		if apiErr.Message == "" {
			apiErr.Message = strings.TrimSpace(string(responseData))
		}
		if apiErr.Message == "" {
			apiErr.Message = http.StatusText(response.StatusCode)
		}
		return response.StatusCode, apiErr
	}
	if responseBody == nil {
		return response.StatusCode, nil
	}
	if err := json.Unmarshal(responseData, responseBody); err != nil {
		return response.StatusCode, fmt.Errorf("decode spoold response: %w", err)
	}
	return response.StatusCode, nil
}

func newUnixClient(rawURL string, parsed *url.URL) (*client, error) {
	if parsed.Host != "" || parsed.Path == "" || !strings.HasPrefix(parsed.Path, "/") {
		return nil, errors.New("unix --server must contain an absolute socket path")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, errors.New("unix --server must not include user information, a query, or a fragment")
	}

	socketPath := parsed.Path
	dialer := &net.Dialer{Timeout: 20 * time.Second}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
		return dialer.DialContext(ctx, "unix", socketPath)
	}
	baseURL, err := url.Parse("http://spoold")
	if err != nil {
		return nil, err
	}
	return &client{
		baseURL:    baseURL,
		displayURL: rawURL,
		httpClient: &http.Client{
			Transport: transport,
			Timeout:   20 * time.Second,
		},
	}, nil
}

type headerValues map[string]string

func (h *headerValues) String() string {
	if h == nil {
		return ""
	}
	return fmt.Sprint(map[string]string(*h))
}

func (h *headerValues) Set(value string) error {
	name, headerValue, found := strings.Cut(value, ":")
	name = http.CanonicalHeaderKey(strings.TrimSpace(name))
	if !found || name == "" {
		return errors.New("header must use 'Name: value' form")
	}
	if strings.ContainsAny(headerValue, "\r\n") {
		return errors.New("header value must not contain a newline")
	}
	if *h == nil {
		*h = make(headerValues)
	}
	if _, exists := (*h)[name]; exists {
		return fmt.Errorf("header %q was provided more than once", name)
	}
	(*h)[name] = strings.TrimSpace(headerValue)
	return nil
}
