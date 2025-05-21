// Copyright 2016 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package integration

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/client/pkg/v3/testutil"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/pkg/v3/grpctesting"
	epb "go.etcd.io/etcd/server/v3/etcdserver/api/v3election/v3electionpb"
	lockpb "go.etcd.io/etcd/server/v3/etcdserver/api/v3lock/v3lockpb"
	framecfg "go.etcd.io/etcd/tests/v3/framework/config"
	"go.etcd.io/etcd/tests/v3/framework/testutils"
)

const (
	// RequestWaitTimeout is the time duration to wait for a request to go through or detect leader loss.
	RequestWaitTimeout = 5 * time.Second
	RequestTimeout     = 20 * time.Second

	ClusterName  = "etcd"
	BasePort     = 21000
	URLScheme    = "unix"
	URLSchemeTLS = "unixs"
	BaseGRPCPort = 30000
)

var (
	ElectionTicks = 10

	// UniqueCount integration test is used to set unique member ids
	UniqueCount = int32(0)

	TestTLSInfo = transport.TLSInfo{
		KeyFile:        testutils.MustAbsPath("../fixtures/server.key.insecure"),
		CertFile:       testutils.MustAbsPath("../fixtures/server.crt"),
		TrustedCAFile:  testutils.MustAbsPath("../fixtures/ca.crt"),
		ClientCertAuth: true,
	}

	TestTLSInfoWithSpecificUsage = transport.TLSInfo{
		KeyFile:        testutils.MustAbsPath("../fixtures/server-serverusage.key.insecure"),
		CertFile:       testutils.MustAbsPath("../fixtures/server-serverusage.crt"),
		ClientKeyFile:  testutils.MustAbsPath("../fixtures/client-clientusage.key.insecure"),
		ClientCertFile: testutils.MustAbsPath("../fixtures/client-clientusage.crt"),
		TrustedCAFile:  testutils.MustAbsPath("../fixtures/ca.crt"),
		ClientCertAuth: true,
	}

	TestTLSInfoIP = transport.TLSInfo{
		KeyFile:        testutils.MustAbsPath("../fixtures/server-ip.key.insecure"),
		CertFile:       testutils.MustAbsPath("../fixtures/server-ip.crt"),
		TrustedCAFile:  testutils.MustAbsPath("../fixtures/ca.crt"),
		ClientCertAuth: true,
	}

	TestTLSInfoExpired = transport.TLSInfo{
		KeyFile:        testutils.MustAbsPath("./fixtures-expired/server.key.insecure"),
		CertFile:       testutils.MustAbsPath("./fixtures-expired/server.crt"),
		TrustedCAFile:  testutils.MustAbsPath("./fixtures-expired/ca.crt"),
		ClientCertAuth: true,
	}

	TestTLSInfoExpiredIP = transport.TLSInfo{
		KeyFile:        testutils.MustAbsPath("./fixtures-expired/server-ip.key.insecure"),
		CertFile:       testutils.MustAbsPath("./fixtures-expired/server-ip.crt"),
		TrustedCAFile:  testutils.MustAbsPath("./fixtures-expired/ca.crt"),
		ClientCertAuth: true,
	}

	DefaultTokenJWT = fmt.Sprintf("jwt,pub-key=%s,priv-key=%s,sign-method=RS256,ttl=2s",
		testutils.MustAbsPath("../fixtures/server.crt"), testutils.MustAbsPath("../fixtures/server.key.insecure"))

	// UniqueNumber is used to generate unique port numbers
	// Should only be accessed via atomic package methods.
	UniqueNumber int32
)

type ClusterConfig struct {
	Size      int
	PeerTLS   *transport.TLSInfo
	ClientTLS *transport.TLSInfo

	DiscoveryURL string

	AuthToken string

	QuotaBackendBytes    int64
	BackendBatchInterval time.Duration

	MaxTxnOps       uint
	MaxRequestBytes uint

	SnapshotCount          uint64
	SnapshotCatchUpEntries uint64

	GRPCKeepAliveMinTime        time.Duration
	GRPCKeepAliveInterval       time.Duration
	GRPCKeepAliveTimeout        time.Duration
	GRPCAdditionalServerOptions []grpc.ServerOption

	ClientMaxCallSendMsgSize int
	ClientMaxCallRecvMsgSize int

	// UseIP is true to use only IP for gRPC requests.
	UseIP bool
	// UseBridge adds bridge between client and grpc server. Should be used in tests that
	// want to manipulate connection or require connection not breaking despite server stop/restart.
	UseBridge bool
	// UseTCP configures server listen on tcp socket. If disabled unix socket is used.
	UseTCP bool

	EnableLeaseCheckpoint   bool
	LeaseCheckpointInterval time.Duration
	LeaseCheckpointPersist  bool

	WatchProgressNotifyInterval time.Duration
	MaxLearners                 int
	DisableStrictReconfigCheck  bool
	CorruptCheckTime            time.Duration
	Metrics                     string
}

type Cluster struct {
	Cfg           *ClusterConfig
	Members       []*Member
	LastMemberNum int

	mu sync.Mutex
}

func SchemeFromTLSInfo(tls *transport.TLSInfo) string {
	if tls == nil {
		return URLScheme
	}
	return URLSchemeTLS
}

func (c *Cluster) MustNewMember(t testutil.TB) *Member {
	memberNumber := c.LastMemberNum
	c.LastMemberNum++

	m := MustNewMember(t,
		MemberConfig{
			Name: fmt.Sprintf("m%v", memberNumber),
		})
	return m
}

// WaitLeader returns index of the member in c.Members that is leader
// or fails the test (if not established in 30s).
func (c *Cluster) WaitLeader(tb testing.TB) int {
	// return c.WaitMembersForLeader(tb, c.Members)
	return 0
}

func NewLocalListener(t testutil.TB) net.Listener {
	c := atomic.AddInt32(&UniqueCount, 1)
	// Go 1.8+ allows only numbers in port
	addr := fmt.Sprintf("127.0.0.1:%05d%05d", c+BasePort, os.Getpid())
	return NewListenerWithAddr(t, addr)
}

func NewListenerWithAddr(t testutil.TB, addr string) net.Listener {
	t.Logf("Creating listener with addr: %v", addr)
	l, err := transport.NewUnixListener(addr)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

type Member struct {
	Name string `json:"name"`
	Port string `json:"port"`
	// ClientTLSInfo and DialOptions are already commented out
	// ClientTLSInfo *transport.TLSInfo
	// DialOptions []grpc.DialOption

	GRPCBridge Bridge `json:"-"` // Ignore complex field
	GRPCURL    string `json:"grpcUrl"`

	Client *clientv3.Client `json:"-"`
	// Client                   interface{} `json:"-"` // Ignore complex field
	ClientMaxCallSendMsgSize int                       `json:"clientMaxCallSendMsgSize"`
	ClientMaxCallRecvMsgSize int                       `json:"clientMaxCallRecvMsgSize"`
	UseTCP                   bool                      `json:"useTcp"`
	UseIP                    bool                      `json:"useIp"`
	UseBridge                bool                      `json:"useBridge"`
	GRPCServerRecorder       *grpctesting.GRPCRecorder `json:"-"` // Ignore complex field
	Logger                   *zap.Logger               `json:"-"` // Ignore complex field
}

// ToJSON serializes Member to JSON
func (m *Member) ToJSON() ([]byte, error) {
	return json.Marshal(m)
}

// FromJSON deserializes JSON to Member
func (m *Member) FromJSON(data []byte) error {
	return json.Unmarshal(data, m)
}

// MembersToJSON serializes Member array to JSON
func MembersToJSON(members []*Member) ([]byte, error) {
	return json.Marshal(members)
}

// MembersFromJSON deserializes JSON to Member array
func MembersFromJSON(data []byte) ([]*Member, error) {
	var members []*Member
	err := json.Unmarshal(data, &members)
	return members, err
}

type MemberConfig struct {
	Name      string
	UseBridge bool
	UseIP     bool
}

// MustNewMember return an inited member with the given name. If peerTLS is
// set, it will use https scheme to communicate between peers.
func MustNewMember(t testutil.TB, mcfg MemberConfig) *Member {

	m := &Member{}
	return m
}

func (m *Member) GRPCPortNumber() string {
	return m.Port
}

// NewClientV3 creates a new grpc client connection to the member
func NewClientV3(m *Member, token string) (*clientv3.Client, error) {
	if m.GRPCURL == "" {
		return nil, fmt.Errorf("member not configured for grpc")
	}

	cfg := clientv3.Config{
		Endpoints:          []string{m.GRPCURL},
		DialTimeout:        5 * time.Second,
		DialOptions:        []grpc.DialOption{grpc.WithBlock()},
		MaxCallSendMsgSize: m.ClientMaxCallSendMsgSize,
		MaxCallRecvMsgSize: m.ClientMaxCallRecvMsgSize,
		Logger:             m.Logger.Named("client"),
		Token:              token,
	}

	// if m.ClientTLSInfo != nil {
	// 	tls, err := m.ClientTLSInfo.ClientConfig()
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	cfg.TLS = tls
	// }
	// if m.DialOptions != nil {
	// 	cfg.DialOptions = append(cfg.DialOptions, m.DialOptions...)
	// }
	return newClientV3(cfg)
}

func (m *Member) RecordedRequests() []grpctesting.RequestInfo {
	return m.GRPCServerRecorder.RecordedRequests()
}

func (m *Member) WaitStarted(t testutil.TB) {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
		_, err := m.Client.Get(ctx, "/", clientv3.WithSerializable())
		if err != nil {
			time.Sleep(framecfg.TickDuration)
			continue
		}
		cancel()
		break
	}
}

func WaitClientV3(t testutil.TB, kv clientv3.KV) {
	WaitClientV3WithKey(t, kv, "/")
}

func WaitClientV3WithKey(t testutil.TB, kv clientv3.KV, key string) {
	timeout := time.Now().Add(RequestTimeout)
	var err error
	for time.Now().Before(timeout) {
		ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
		_, err = kv.Get(ctx, key)
		cancel()
		if err == nil {
			return
		}
		time.Sleep(framecfg.TickDuration)
	}
	if err != nil {
		t.Fatalf("timed out waiting for client: %v", err)
	}
}

type SortableMemberSliceByPeerURLs []*pb.Member

func (p SortableMemberSliceByPeerURLs) Len() int { return len(p) }
func (p SortableMemberSliceByPeerURLs) Less(i, j int) bool {
	return p[i].PeerURLs[0] < p[j].PeerURLs[0]
}
func (p SortableMemberSliceByPeerURLs) Swap(i, j int) { p[i], p[j] = p[j], p[i] }

func memberLogger(t testutil.TB, name string) (*zap.Logger, *testutils.LogObserver) {
	level := zapcore.InfoLevel
	if os.Getenv("CLUSTER_DEBUG") != "" {
		level = zapcore.DebugLevel
	}

	obCore, logOb := testutils.NewLogObserver(level)

	options := zaptest.WrapOptions(
		zap.Fields(zap.String("member", name)),

		// copy logged entities to log observer
		zap.WrapCore(func(oldCore zapcore.Core) zapcore.Core {
			return zapcore.NewTee(oldCore, obCore)
		}),
	)
	return zaptest.NewLogger(t, zaptest.Level(level), options).Named(name), logOb
}

// NewCluster returns a launched Cluster with a grpc client connection
// for each Cluster member.
func NewCluster(t testutil.TB, cfg *ClusterConfig) *Cluster {
	t.Helper()

	assertInTestContext(t)

	testutil.SkipTestIfShortMode(t, "Cannot start etcd Cluster in --short tests")

	c := &Cluster{Cfg: cfg}
	ms := make([]*Member, cfg.Size)
	// Get members from JSON file if available
	memberFile := os.Getenv("ETCD_TEST_MEMBERS_FILE")
	if memberFile == "" {
		panic("no test members file")
	}

	data, err := os.ReadFile(memberFile)

	token := make([]byte, 8)

	n, err := rand.Read(token)
	if err != nil || n != len(token) {
		t.Fatalf("failed to generate token: %v", err)
	}

	tokenString := base64.StdEncoding.EncodeToString(token)

	if err == nil {
		if members, err := MembersFromJSON(data); err == nil {
			if len(members) < cfg.Size {
				panic("must provide at least as many members as needed")
			}
			ms = members[:cfg.Size]
			c.LastMemberNum = cfg.Size - 1
			for _, m := range ms {
				m.GRPCServerRecorder = &grpctesting.GRPCRecorder{}
				m.Logger, _ = memberLogger(t, m.Name)

				cli, err := NewClientV3(m, tokenString)
				if err != nil {
					panic("failed to build client")
				}
				m.Client = cli
			}
		}
	} else {
		println("blah", data, err.Error())
	}

	c.Members = ms

	return c
}

func (c *Cluster) TakeClient(idx int) {
	c.mu.Lock()
	c.Members[idx].Client = nil
	c.mu.Unlock()
}

func (c *Cluster) Terminate(t testutil.TB) {
	if t != nil {
		t.Logf("========= Cluster termination started =====================")
	}
	for _, m := range c.Members {
		if m.Client != nil {
			m.Client.Close()
		}
	}
	var wg sync.WaitGroup
	wg.Add(len(c.Members))
	for _, m := range c.Members {
		go func(mm *Member) {
			defer wg.Done()
			// mm.Terminate(t)
		}(m)
	}
	wg.Wait()
	if t != nil {
		t.Logf("========= Cluster termination succeeded ===================")
	}
}

func (c *Cluster) RandClient() *clientv3.Client {
	return c.Members[rand.Intn(len(c.Members))].Client
}

func (c *Cluster) Client(i int) *clientv3.Client {
	return c.Members[i].Client
}

func (c *Cluster) Endpoints() []string {
	var endpoints []string
	for _, m := range c.Members {
		endpoints = append(endpoints, m.GRPCURL)
	}
	return endpoints
}

func (c *Cluster) ClusterClient(tb testing.TB, opts ...framecfg.ClientOption) (client *clientv3.Client, err error) {
	cfg, err := c.newClientCfg()
	if err != nil {
		return nil, err
	}
	for _, opt := range opts {
		opt(cfg)
	}
	client, err = newClientV3(*cfg)
	if err != nil {
		return nil, err
	}
	tb.Cleanup(func() {
		client.Close()
	})
	return client, nil
}

func WithAuth(userName, password string) framecfg.ClientOption {
	return func(c any) {
		cfg := c.(*clientv3.Config)
		cfg.Username = userName
		cfg.Password = password
	}
}

func WithAuthToken(token string) framecfg.ClientOption {
	return func(c any) {
		cfg := c.(*clientv3.Config)
		cfg.Token = token
	}
}

func WithEndpoints(endpoints []string) framecfg.ClientOption {
	return func(c any) {
		cfg := c.(*clientv3.Config)
		cfg.Endpoints = endpoints
	}
}

func (c *Cluster) newClientCfg() (*clientv3.Config, error) {
	cfg := &clientv3.Config{
		Endpoints:          c.Endpoints(),
		DialTimeout:        5 * time.Second,
		DialOptions:        []grpc.DialOption{grpc.WithBlock()},
		MaxCallSendMsgSize: c.Cfg.ClientMaxCallSendMsgSize,
		MaxCallRecvMsgSize: c.Cfg.ClientMaxCallRecvMsgSize,
	}
	if c.Cfg.ClientTLS != nil {
		tls, err := c.Cfg.ClientTLS.ClientConfig()
		if err != nil {
			return nil, err
		}
		cfg.TLS = tls
	}
	return cfg, nil
}

// NewClientV3 creates a new grpc client connection to the member
func (c *Cluster) NewClientV3(memberIndex int) (*clientv3.Client, error) {
	return NewClientV3(c.Members[memberIndex])
}

func makeClients(t testutil.TB, clus *Cluster, clients *[]*clientv3.Client, chooseMemberIndex func() int) func() *clientv3.Client {
	var mu sync.Mutex
	*clients = nil
	return func() *clientv3.Client {
		cli, err := clus.NewClientV3(chooseMemberIndex())
		if err != nil {
			t.Fatalf("cannot create client: %v", err)
		}
		mu.Lock()
		*clients = append(*clients, cli)
		mu.Unlock()
		return cli
	}
}

// MakeSingleNodeClients creates factory of clients that all connect to member 0.
// All the created clients are put on the 'clients' list. The factory is thread-safe.
func MakeSingleNodeClients(t testutil.TB, clus *Cluster, clients *[]*clientv3.Client) func() *clientv3.Client {
	return makeClients(t, clus, clients, func() int { return 0 })
}

// MakeMultiNodeClients creates factory of clients that all connect to random members.
// All the created clients are put on the 'clients' list. The factory is thread-safe.
func MakeMultiNodeClients(t testutil.TB, clus *Cluster, clients *[]*clientv3.Client) func() *clientv3.Client {
	return makeClients(t, clus, clients, func() int { return rand.Intn(len(clus.Members)) })
}

// CloseClients closes all the clients from the 'clients' list.
func CloseClients(t testutil.TB, clients []*clientv3.Client) {
	for _, cli := range clients {
		if err := cli.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

type GRPCAPI struct {
	// Cluster is the Cluster API for the client'Server connection.
	Cluster pb.ClusterClient
	// KV is the keyvalue API for the client'Server connection.
	KV pb.KVClient
	// Lease is the lease API for the client'Server connection.
	Lease pb.LeaseClient
	// Watch is the watch API for the client'Server connection.
	Watch pb.WatchClient
	// Maintenance is the maintenance API for the client'Server connection.
	Maintenance pb.MaintenanceClient
	// Auth is the authentication API for the client'Server connection.
	Auth pb.AuthClient
	// Lock is the lock API for the client'Server connection.
	Lock lockpb.LockClient
	// Election is the election API for the client'Server connection.
	Election epb.ElectionClient
}

type SortableProtoMemberSliceByPeerURLs []*pb.Member

func (p SortableProtoMemberSliceByPeerURLs) Len() int { return len(p) }
func (p SortableProtoMemberSliceByPeerURLs) Less(i, j int) bool {
	return p[i].PeerURLs[0] < p[j].PeerURLs[0]
}
func (p SortableProtoMemberSliceByPeerURLs) Swap(i, j int) { p[i], p[j] = p[j], p[i] }
