package grpc

import (
	"context"
	"errors"
	"sync"

	"github.com/coredns/coredns/pb"

	"github.com/miekg/dns"
	grpcgo "google.golang.org/grpc"
)

// ErrStreamPoolStopped is returned when the stream pool has been shut down.
var ErrStreamPoolStopped = errors.New("stream pool stopped")

// streamPool manages a fixed set of persistent bidirectional gRPC streams.
//
// Each slot holds one open QueryStream RPC. Callers acquire a slot, exchange
// exactly one DNS message, then release the slot. Because every slot is used
// exclusively by one goroutine at a time, no request-correlation bookkeeping
// is needed — the only response on the wire after a Send is the one for that
// query.
//
// The critical difference vs unary gRPC: no new HTTP/2 stream is created per
// DNS query. The per-query cost is two frame writes (DATA + DATA) rather than
// HEADERS + DATA + DATA + RST_STREAM.
type streamPool struct {
	addr     string
	dialOpts []grpcgo.DialOption
	size     int

	conn   *grpcgo.ClientConn
	client pb.DnsServiceClient

	slots chan pb.DnsService_QueryStreamClient // buffered, size == pool size
	mu    sync.Mutex
	done  chan struct{}
}

func newStreamPool(addr string, dialOpts []grpcgo.DialOption, size int) *streamPool {
	return &streamPool{
		addr:     addr,
		dialOpts: dialOpts,
		size:     size,
		done:     make(chan struct{}),
	}
}

// Start opens the single connection and pre-populates all stream slots.
func (p *streamPool) Start() error {
	conn, err := grpcgo.NewClient(p.addr, p.dialOpts...)
	if err != nil {
		return err
	}
	p.conn = conn
	p.client = pb.NewDnsServiceClient(conn)
	p.slots = make(chan pb.DnsService_QueryStreamClient, p.size)

	for range p.size {
		s, err := p.openStream()
		if err != nil {
			return err
		}
		p.slots <- s
	}
	return nil
}

func (p *streamPool) openStream() (pb.DnsService_QueryStreamClient, error) {
	return p.client.QueryStream(context.Background())
}

// Query acquires a stream slot, sends one DNS query, reads one response,
// and releases the slot. If the stream is broken it is replaced.
func (p *streamPool) Query(ctx context.Context, req *dns.Msg) (*dns.Msg, error) {
	var stream pb.DnsService_QueryStreamClient
	select {
	case stream = <-p.slots:
	case <-p.done:
		return nil, ErrStreamPoolStopped
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	resp, err := p.exchange(stream, req)
	if err != nil {
		// stream is broken — replace before returning the slot
		if s, rerr := p.openStream(); rerr == nil {
			stream = s
		}
	}

	select {
	case p.slots <- stream:
	case <-p.done:
	}

	return resp, err
}

func (p *streamPool) exchange(stream pb.DnsService_QueryStreamClient, req *dns.Msg) (*dns.Msg, error) {
	packed, err := req.Pack()
	if err != nil {
		return nil, err
	}
	if err := stream.Send(&pb.DnsPacket{Msg: packed}); err != nil {
		return nil, err
	}
	pkt, err := stream.Recv()
	if err != nil {
		return nil, err
	}
	resp := new(dns.Msg)
	if err := resp.Unpack(pkt.Msg); err != nil {
		return nil, err
	}
	return resp, nil
}

// Stop closes all streams and the underlying connection.
func (p *streamPool) Stop() {
	close(p.done)
	for range p.size {
		select {
		case s := <-p.slots:
			s.CloseSend()
		default:
		}
	}
	if p.conn != nil {
		p.conn.Close()
	}
}
