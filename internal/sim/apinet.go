package sim

import (
	"context"
	"fmt"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	"github.com/evazheng/fault-tolerant-coordinator/internal/client"
	"github.com/evazheng/fault-tolerant-coordinator/internal/server"
	schedulerpb "github.com/evazheng/fault-tolerant-coordinator/proto/schedulerpb"
)

// apiNet is the client-facing transport for the simulator: one real gRPC
// server per coordinator, over bufconn rather than TCP.
//
// WHY A REAL SERVER AND NOT A DIRECT CALL. Until now the simulator held
// *server.Service values and called their methods, which skips the whole
// client path: proto marshaling, status codes, and — the part that matters —
// client.Pool's NOT_LEADER redirect chase and its separate handling of a node
// that does not answer at all. A simulated worker that routed itself to the
// leader would be testing the simulator's routing, not the worker's, and the
// ledger it writes would prove the stand-in idempotent.
//
// bufconn gives the real stack without binding a port: no address exhaustion
// in a long run, no interference between parallel runs, and a closed listener
// looks exactly like a refused connection, which is what a killed coordinator
// should look like.
type apiNet struct {
	mu  sync.Mutex
	lis map[string]*bufconn.Listener
	srv map[string]*grpc.Server
	// gates are keyed by CLIENT (worker id), not by address: the pause fault
	// freezes a process, and a frozen process stops talking to every
	// coordinator at once.
	gates map[string]*gate
	// onReport sees every ReportResult that reaches a coordinator, and
	// whether it was accepted.
	//
	// It exists because a real worker reports for itself, so the simulator no
	// longer knows what any execution returned — and the failure-path
	// coverage counters, which exist precisely so a green run cannot claim
	// credit for paths it never ran, would silently read zero.
	onReport func(req *schedulerpb.ReportResultRequest, err error)
}

// bufSize is the listener's buffer. One request is a few hundred bytes; this
// is generous enough that the buffer itself never becomes the thing that
// blocks, since a held conn is supposed to be the only reason writes stall.
const bufSize = 1 << 20

func newAPINet(onReport func(*schedulerpb.ReportResultRequest, error)) *apiNet {
	return &apiNet{
		lis:      map[string]*bufconn.Listener{},
		srv:      map[string]*grpc.Server{},
		gates:    map[string]*gate{},
		onReport: onReport,
	}
}

// observe reports ReportResult calls to the simulator. A unary interceptor
// rather than a wrapper type: it sits where the server already is, so it
// cannot be bypassed by a call that takes another route to the service.
func (a *apiNet) observe(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
	resp, err := h(ctx, req)
	if a.onReport != nil && info.FullMethod == schedulerpb.Scheduler_ReportResult_FullMethodName {
		if rr, ok := req.(*schedulerpb.ReportResultRequest); ok {
			a.onReport(rr, err)
		}
	}
	return resp, err
}

// serve starts a gRPC server for one coordinator at addr.
func (a *apiNet) serve(addr string, svc *server.Service) {
	l := bufconn.Listen(bufSize)
	gs := grpc.NewServer(grpc.UnaryInterceptor(a.observe))
	schedulerpb.RegisterSchedulerServer(gs, svc)

	a.mu.Lock()
	a.lis[addr] = l
	a.srv[addr] = gs
	a.mu.Unlock()

	go func() {
		// Serve returns when the listener closes, which is how a node is
		// killed here. That is not an error.
		_ = gs.Serve(l)
	}()
}

// shut takes a coordinator's API offline. In-flight calls are cut, and
// subsequent dials fail — a killed node does not answer NOT_LEADER, it does
// not answer, and that distinction is the one the Pool gets wrong when it is
// not exercised.
func (a *apiNet) shut(addr string) {
	a.mu.Lock()
	gs := a.srv[addr]
	delete(a.srv, addr)
	delete(a.lis, addr)
	a.mu.Unlock()

	if gs != nil {
		gs.Stop()
	}
}

func (a *apiNet) close() {
	a.mu.Lock()
	srv := a.srv
	a.srv = map[string]*grpc.Server{}
	a.lis = map[string]*bufconn.Listener{}
	gates := a.gates
	a.mu.Unlock()

	for _, g := range gates {
		g.release()
	}
	for _, gs := range srv {
		gs.Stop()
	}
}

// dialerFor returns the dialer a given client's Pool should use. Every conn
// it opens is wrapped in that client's gate, so a hold applies to connections
// the client opens later as well — a worker chasing a redirect mid-pause must
// not escape the pause by dialing somewhere new.
func (a *apiNet) dialerFor(clientID string) client.Dialer {
	g := a.gateFor(clientID)
	return func(ctx context.Context, addr string) (net.Conn, error) {
		if err := g.wait(ctx); err != nil {
			return nil, err
		}
		a.mu.Lock()
		l := a.lis[addr]
		a.mu.Unlock()
		if l == nil {
			return nil, fmt.Errorf("connection refused: no coordinator at %s", addr)
		}
		c, err := l.DialContext(ctx)
		if err != nil {
			return nil, err
		}
		return &heldConn{Conn: c, g: g, closed: make(chan struct{})}, nil
	}
}

func (a *apiNet) gateFor(clientID string) *gate {
	a.mu.Lock()
	defer a.mu.Unlock()
	if g, ok := a.gates[clientID]; ok {
		return g
	}
	g := newGate()
	a.gates[clientID] = g
	return g
}

// hold stops a client's outbound RPCs without failing them. They sit in the
// transport and are delivered on release.
//
// HELD, NOT DROPPED, and the difference is the whole point. Dropping gives a
// partition, which the cluster already defends against. Holding gives the
// descheduled-process case: a late ReportResult for attempt N leaves the
// worker before the pause and arrives after attempt N+1 was dispatched, which
// is the delivery the (taskID, attempt) idempotency key exists to reject.
func (a *apiNet) hold(clientID string)    { a.gateFor(clientID).hold() }
func (a *apiNet) release(clientID string) { a.gateFor(clientID).release() }

// gate is a closable barrier: open by default, shut by hold, reopened by
// release. A channel rather than a condition variable so waiting can also
// select on a context.
type gate struct {
	mu   sync.Mutex
	open chan struct{}
}

func newGate() *gate {
	ch := make(chan struct{})
	close(ch)
	return &gate{open: ch}
}

func (g *gate) hold() {
	g.mu.Lock()
	defer g.mu.Unlock()
	select {
	case <-g.open:
		g.open = make(chan struct{}) // was open; shut it
	default:
	}
}

func (g *gate) release() {
	g.mu.Lock()
	defer g.mu.Unlock()
	select {
	case <-g.open:
	default:
		close(g.open)
	}
}

func (g *gate) wait(ctx context.Context) error {
	g.mu.Lock()
	ch := g.open
	g.mu.Unlock()
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// heldConn is a conn whose WRITES wait on a gate.
//
// Writes and not reads: a held process is one that is not running, so nothing
// leaves it; what has already arrived is still in its buffers. Blocking the
// write stalls gRPC's connection writer, which is a faithful model of the
// whole process being descheduled — every stream on that conn stops together,
// rather than one RPC being singled out.
type heldConn struct {
	net.Conn
	g         *gate
	closed    chan struct{}
	closeOnce sync.Once
}

func (c *heldConn) Write(b []byte) (int, error) {
	g := c.g
	g.mu.Lock()
	ch := g.open
	g.mu.Unlock()
	select {
	case <-ch:
	case <-c.closed:
		return 0, net.ErrClosed
	}
	return c.Conn.Write(b)
}

func (c *heldConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return c.Conn.Close()
}
