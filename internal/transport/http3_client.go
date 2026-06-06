/*
 * Copyright 2026 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/quic-go/quic-go/http3"
	"golang.org/x/net/http2"
	"google.golang.org/grpc/codes"
	istats "google.golang.org/grpc/internal/stats"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

type http3Client struct {
	addr    string
	opts    ConnectOptions
	onClose OnCloseFunc
	tr      *http3.Transport
	hclient *http.Client

	mu     sync.Mutex
	error  chan struct{}
	closed bool

	statsHandler stats.Handler

	streamsMu sync.Mutex
	streams   map[*ClientStream]*h3StreamState
}

type h3StreamState struct {
	pw *io.PipeWriter
}

// NewHTTP3Client constructs a connected ClientTransport to addr based on HTTP3
// and starts to receive messages on it. Non-nil error returns if construction
// fails.
func NewHTTP3Client(_, _ context.Context, addr resolver.Address, opts ConnectOptions, onClose OnCloseFunc) (ClientTransport, error) {
	t := &http3Client{
		addr:         addr.Addr,
		opts:         opts,
		onClose:      onClose,
		error:        make(chan struct{}),
		statsHandler: istats.NewCombinedHandler(opts.StatsHandlers...),
		streams:      make(map[*ClientStream]*h3StreamState),
	}

	t.tr = &http3.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}
	t.hclient = &http.Client{
		Transport: t.tr,
	}

	return t, nil
}

func (t *http3Client) NewStream(ctx context.Context, callHdr *CallHdr, handler stats.Handler) (*ClientStream, error) {
	s := &ClientStream{
		Stream: Stream{
			ctx:    ctx,
			method: callHdr.Method,
			buf:    recvBuffer{c: make(chan recvMsg, 1)},
		},
		ct:           t,
		done:         make(chan struct{}),
		headerChan:   make(chan struct{}),
		statsHandler: handler,
	}
	s.trReader = transportReader{
		reader: recvBufferReader{
			ctx:     s.ctx,
			ctxDone: s.ctx.Done(),
			recv:    &s.buf,
		},
		windowHandler: s,
	}
	s.readRequester = s

	pr, pw := io.Pipe()
	t.streamsMu.Lock()
	t.streams[s] = &h3StreamState{pw: pw}
	t.streamsMu.Unlock()

	// Launch the H3 request in a goroutine
	go t.doRequest(s, callHdr, pr)

	return s, nil
}

func (t *http3Client) doRequest(s *ClientStream, callHdr *CallHdr, pr *io.PipeReader) {
	url := fmt.Sprintf("https://%s%s", callHdr.Host, callHdr.Method)
	req, err := http.NewRequestWithContext(s.ctx, "POST", url, pr)
	if err != nil {
		s.buf.put(recvMsg{err: err})
		return
	}

	req.Header.Set("Content-Type", "application/grpc")
	req.Header.Set("TE", "trailers")

	resp, err := t.hclient.Do(req)
	if err != nil {
		s.buf.put(recvMsg{err: err})
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		s.buf.put(recvMsg{err: status.Errorf(codes.Unknown, "HTTP %s", resp.Status)})
		return
	}

	// Signal headers received
	s.header = metadata.MD(resp.Header)
	close(s.headerChan)
	s.headerValid = true

	// Read loop
	for {
		// Simplified buffer handling
		data := make([]byte, 1024*32)
		n, err := resp.Body.Read(data)
		if n > 0 {
			// Copy data to a safe buffer
			b := make([]byte, n)
			copy(b, data)
			s.buf.put(recvMsg{buffer: mem.NewBuffer(&b, nil)})
		}
		if err != nil {
			if err == io.EOF {
				// Handle trailers after EOF
				if len(resp.Trailer) > 0 {
					s.trailer = metadata.MD(resp.Trailer)
					if stStr := resp.Trailer.Get("Grpc-Status"); stStr != "" {
						var code uint32
						fmt.Sscanf(stStr, "%d", &code)
						msg := resp.Trailer.Get("Grpc-Message")
						s.status = status.New(codes.Code(code), msg)
					}
				}
				s.buf.put(recvMsg{err: io.EOF})
			} else {
				s.buf.put(recvMsg{err: err})
			}
			break
		}
	}

	t.streamsMu.Lock()
	delete(t.streams, s)
	t.streamsMu.Unlock()
}

func (t *http3Client) Close(err error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	t.closed = true
	t.mu.Unlock()

	if t.onClose != nil {
		t.onClose(GoAwayInfo{
			Reason: GoAwayNoReason,
			Err:    err,
		})
	}

	t.tr.Close()
	close(t.error)
}

func (t *http3Client) GracefulClose() {
	t.Close(nil)
}

func (t *http3Client) Error() <-chan struct{} {
	return t.error
}

func (t *http3Client) GoAway() <-chan struct{} {
	return make(chan struct{})
}

func (t *http3Client) GetGoAwayReason() (GoAwayReason, string) {
	return GoAwayNoReason, ""
}

func (t *http3Client) Peer() *peer.Peer {
	return &peer.Peer{}
}

// clientCallback implementation
func (t *http3Client) incrMsgRecv() {}
func (t *http3Client) closeStream(s *ClientStream, _ error, _ bool, _ http2.ErrCode, _ *status.Status, _ map[string][]string, _ bool) {
	t.streamsMu.Lock()
	if state, ok := t.streams[s]; ok {
		state.pw.Close()
	}
	t.streamsMu.Unlock()
}
func (t *http3Client) write(s *ClientStream, hdr []byte, data mem.BufferSlice, opts *WriteOptions) error {
	t.streamsMu.Lock()
	state, ok := t.streams[s]
	t.streamsMu.Unlock()

	if !ok {
		return errors.New("stream closed")
	}

	if len(hdr) > 0 {
		if _, err := state.pw.Write(hdr); err != nil {
			return err
		}
	}
	for _, b := range data {
		if _, err := state.pw.Write(b.ReadOnlyData()); err != nil {
			return err
		}
	}
	if opts.Last {
		state.pw.Close()
	}
	return nil
}
func (t *http3Client) adjustWindow(_ *ClientStream, _ uint32) {}
func (t *http3Client) updateWindow(_ *ClientStream, _ uint32) {}
