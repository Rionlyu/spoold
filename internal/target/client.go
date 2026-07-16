package target

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

var ErrBlockedAddress = errors.New("target resolves to a blocked network address")

func NewClient(allowPrivate bool, timeout time.Duration) *http.Client {
	dialer := &net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = dialer.DialContext
	if !allowPrivate {
		transport.DialContext = safeDialContext(dialer)
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   timeout,
	}
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= 5 {
			return errors.New("stopped after 5 redirects")
		}
		return ValidateURL(req.URL)
	}
	return client
}

func ValidateURL(target *url.URL) error {
	if target == nil {
		return errors.New("target URL is missing")
	}
	if target.Scheme != "http" && target.Scheme != "https" {
		return errors.New("target URL must use http or https")
	}
	if target.Hostname() == "" {
		return errors.New("target URL must include a host")
	}
	if target.User != nil {
		return errors.New("target URL must not include user information")
	}
	return nil
}

func safeDialContext(dialer *net.Dialer) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, fmt.Errorf("split target address: %w", err)
		}

		addresses, err := resolve(ctx, host)
		if err != nil {
			return nil, err
		}
		for _, ip := range addresses {
			if blocked(ip) {
				continue
			}
			return dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
		}
		return nil, fmt.Errorf("%w: %s", ErrBlockedAddress, host)
	}
}

func resolve(ctx context.Context, host string) ([]net.IP, error) {
	if ip := net.ParseIP(host); ip != nil {
		return []net.IP{ip}, nil
	}
	resolved, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("resolve target host: %w", err)
	}
	addresses := make([]net.IP, 0, len(resolved))
	for _, address := range resolved {
		addresses = append(addresses, address.IP)
	}
	return addresses, nil
}

func blocked(ip net.IP) bool {
	if ip == nil {
		return true
	}
	return ip.IsLoopback() ||
		ip.IsPrivate() ||
		ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() ||
		ip.IsUnspecified() ||
		ip.IsMulticast() ||
		isCarrierGradeNAT(ip)
}

func isCarrierGradeNAT(ip net.IP) bool {
	v4 := ip.To4()
	if v4 == nil {
		return false
	}
	value := uint32(v4[0])<<24 | uint32(v4[1])<<16 | uint32(v4[2])<<8 | uint32(v4[3])
	base := uint32(100)<<24 | uint32(64)<<16
	maskBits, _ := strconv.Atoi("10")
	mask := ^uint32(0) << (32 - maskBits)
	return value&mask == base&mask
}
