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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/etcd/tests/v3/framework/integration"
)

// TestV3LeaseRevoke ensures a key is deleted once its lease is revoked.
func TestV3LeaseRevoke(t *testing.T) {
	integration.BeforeTest(t)
	testLeaseRemoveLeasedKey(t, func(clus *integration.Cluster, leaseID int64) error {
		lc := integration.ToGRPC(clus.RandClient()).Lease
		_, err := lc.LeaseRevoke(context.TODO(), &pb.LeaseRevokeRequest{ID: leaseID})
		return err
	})
}

// TestV3LeaseGrantByID ensures leases may be created by a given id.
func TestV3LeaseGrantByID(t *testing.T) {
	integration.BeforeTest(t)
	clus := integration.NewCluster(t, &integration.ClusterConfig{Size: 3})
	defer clus.Terminate(t)

	// create fixed lease
	lresp, err := integration.ToGRPC(clus.RandClient()).Lease.LeaseGrant(
		context.TODO(),
		&pb.LeaseGrantRequest{ID: 1, TTL: 1})
	if err != nil {
		t.Errorf("could not create lease 1 (%v)", err)
	}
	if lresp.ID != 1 {
		t.Errorf("got id %v, wanted id %v", lresp.ID, 1)
	}

	// create duplicate fixed lease
	_, err = integration.ToGRPC(clus.RandClient()).Lease.LeaseGrant(
		context.TODO(),
		&pb.LeaseGrantRequest{ID: 1, TTL: 1})
	if !eqErrGRPC(err, rpctypes.ErrGRPCLeaseExist) {
		t.Error(err)
	}

	// create fresh fixed lease
	lresp, err = integration.ToGRPC(clus.RandClient()).Lease.LeaseGrant(
		context.TODO(),
		&pb.LeaseGrantRequest{ID: 2, TTL: 1})
	if err != nil {
		t.Errorf("could not create lease 2 (%v)", err)
	}
	if lresp.ID != 2 {
		t.Errorf("got id %v, wanted id %v", lresp.ID, 2)
	}
}

// TestV3LeaseExpire ensures a key is deleted once a key expires.
func TestV3LeaseExpire(t *testing.T) {
	integration.BeforeTest(t)
	testLeaseRemoveLeasedKey(t, func(clus *integration.Cluster, leaseID int64) error {
		// let lease lapse; wait for deleted key

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		wStream, err := integration.ToGRPC(clus.RandClient()).Watch.Watch(ctx)
		if err != nil {
			return err
		}

		wreq := &pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{
			CreateRequest: &pb.WatchCreateRequest{
				Key: []byte("foo"), StartRevision: 1,
			},
		}}
		if err := wStream.Send(wreq); err != nil {
			return err
		}
		if _, err := wStream.Recv(); err != nil {
			// the 'created' message
			return err
		}
		if _, err := wStream.Recv(); err != nil {
			// the 'put' message
			return err
		}

		errc := make(chan error, 1)
		go func() {
			resp, err := wStream.Recv()
			switch {
			case err != nil:
				errc <- err
			case len(resp.Events) != 1:
				fallthrough
			case resp.Events[0].Type != mvccpb.DELETE:
				errc <- fmt.Errorf("expected key delete, got %v", resp)
			default:
				errc <- nil
			}
		}()

		select {
		case <-time.After(15 * time.Second):
			return fmt.Errorf("lease expiration too slow")
		case err := <-errc:
			return err
		}
	})
}

// TestV3LeaseKeepAlive ensures keepalive keeps the lease alive.
func TestV3LeaseKeepAlive(t *testing.T) {
	integration.BeforeTest(t)
	testLeaseRemoveLeasedKey(t, func(clus *integration.Cluster, leaseID int64) error {
		lc := integration.ToGRPC(clus.RandClient()).Lease
		lreq := &pb.LeaseKeepAliveRequest{ID: leaseID}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		lac, err := lc.LeaseKeepAlive(ctx)
		if err != nil {
			return err
		}
		defer lac.CloseSend()

		// renew long enough so lease would've expired otherwise
		for i := 0; i < 3; i++ {
			if err = lac.Send(lreq); err != nil {
				return err
			}
			lresp, rxerr := lac.Recv()
			if rxerr != nil {
				return rxerr
			}
			if lresp.ID != leaseID {
				return fmt.Errorf("expected lease ID %v, got %v", leaseID, lresp.ID)
			}
			time.Sleep(time.Duration(lresp.TTL/2) * time.Second)
		}
		_, err = lc.LeaseRevoke(context.TODO(), &pb.LeaseRevokeRequest{ID: leaseID})
		return err
	})
}

// TestV3LeaseExists creates a lease on a random client and confirms it exists in the cluster.
func TestV3LeaseExists(t *testing.T) {
	integration.BeforeTest(t)
	clus := integration.NewCluster(t, &integration.ClusterConfig{Size: 3})
	defer clus.Terminate(t)

	// create lease
	ctx0, cancel0 := context.WithCancel(context.Background())
	defer cancel0()
	lresp, err := integration.ToGRPC(clus.RandClient()).Lease.LeaseGrant(
		ctx0,
		&pb.LeaseGrantRequest{TTL: 30})
	require.NoError(t, err)
	require.Empty(t, lresp.Error)

	if !leaseExist(t, clus, lresp.ID) {
		t.Error("unexpected lease not exists")
	}
}

// TestV3LeaseLeases creates leases and confirms list RPC fetches created ones.
func TestV3LeaseLeases(t *testing.T) {
	integration.BeforeTest(t)
	clus := integration.NewCluster(t, &integration.ClusterConfig{Size: 1})
	defer clus.Terminate(t)

	ctx0, cancel0 := context.WithCancel(context.Background())
	defer cancel0()

	// create leases
	var ids []int64
	for i := 0; i < 5; i++ {
		lresp, err := integration.ToGRPC(clus.RandClient()).Lease.LeaseGrant(
			ctx0,
			&pb.LeaseGrantRequest{TTL: 30})
		require.NoError(t, err)
		require.Empty(t, lresp.Error)
		ids = append(ids, lresp.ID)
	}

	lresp, err := integration.ToGRPC(clus.RandClient()).Lease.LeaseLeases(
		context.Background(),
		&pb.LeaseLeasesRequest{})
	require.NoError(t, err)
	for i := range lresp.Leases {
		if lresp.Leases[i].ID != ids[i] {
			t.Fatalf("#%d: lease ID expected %d, got %d", i, ids[i], lresp.Leases[i].ID)
		}
	}
}

// TestV3LeaseRenewStress keeps creating lease and renewing it immediately to ensure the renewal goes through.
// it was oberserved that the immediate lease renewal after granting a lease from follower resulted lease not found.
// related issue https://github.com/etcd-io/etcd/issues/6978
func TestV3LeaseRenewStress(t *testing.T) {
	testLeaseStress(t, stressLeaseRenew, false)
}

// TestV3LeaseRenewStressWithClusterClient is similar to TestV3LeaseRenewStress,
// but it uses a cluster client instead of a specific member's client.
// The related issue is https://github.com/etcd-io/etcd/issues/13675.
func TestV3LeaseRenewStressWithClusterClient(t *testing.T) {
	testLeaseStress(t, stressLeaseRenew, true)
}

// TestV3LeaseTimeToLiveStress keeps creating lease and retrieving it immediately to ensure the lease can be retrieved.
// it was oberserved that the immediate lease retrieval after granting a lease from follower resulted lease not found.
// related issue https://github.com/etcd-io/etcd/issues/6978
func TestV3LeaseTimeToLiveStress(t *testing.T) {
	testLeaseStress(t, stressLeaseTimeToLive, false)
}

// TestV3LeaseTimeToLiveStressWithClusterClient is similar to TestV3LeaseTimeToLiveStress,
// but it uses a cluster client instead of a specific member's client.
// The related issue is https://github.com/etcd-io/etcd/issues/13675.
func TestV3LeaseTimeToLiveStressWithClusterClient(t *testing.T) {
	testLeaseStress(t, stressLeaseTimeToLive, true)
}

func testLeaseStress(t *testing.T, stresser func(context.Context, pb.LeaseClient) error, useClusterClient bool) {
	integration.BeforeTest(t)
	clus := integration.NewCluster(t, &integration.ClusterConfig{Size: 3})
	defer clus.Terminate(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	errc := make(chan error)

	if useClusterClient {
		clusterClient, err := clus.ClusterClient(t)
		require.NoError(t, err)
		for i := 0; i < 300; i++ {
			go func() { errc <- stresser(ctx, integration.ToGRPC(clusterClient).Lease) }()
		}
	} else {
		for i := 0; i < 100; i++ {
			for j := 0; j < 3; j++ {
				go func(i int) { errc <- stresser(ctx, integration.ToGRPC(clus.Client(i)).Lease) }(j)
			}
		}
	}

	for i := 0; i < 300; i++ {
		err := <-errc
		require.NoError(t, err)
	}
}

func stressLeaseRenew(tctx context.Context, lc pb.LeaseClient) (reterr error) {
	defer func() {
		if tctx.Err() != nil {
			reterr = nil
		}
	}()
	lac, err := lc.LeaseKeepAlive(tctx)
	if err != nil {
		return err
	}
	for tctx.Err() == nil {
		resp, gerr := lc.LeaseGrant(tctx, &pb.LeaseGrantRequest{TTL: 60})
		if gerr != nil {
			continue
		}
		err = lac.Send(&pb.LeaseKeepAliveRequest{ID: resp.ID})
		if err != nil {
			continue
		}
		rresp, rxerr := lac.Recv()
		if rxerr != nil {
			continue
		}
		if rresp.TTL == 0 {
			return errors.New("TTL shouldn't be 0 so soon")
		}
	}
	return nil
}

func stressLeaseTimeToLive(tctx context.Context, lc pb.LeaseClient) (reterr error) {
	defer func() {
		if tctx.Err() != nil {
			reterr = nil
		}
	}()
	for tctx.Err() == nil {
		resp, gerr := lc.LeaseGrant(tctx, &pb.LeaseGrantRequest{TTL: 60})
		if gerr != nil {
			continue
		}
		_, kerr := lc.LeaseTimeToLive(tctx, &pb.LeaseTimeToLiveRequest{ID: resp.ID})
		if errors.Is(rpctypes.Error(kerr), rpctypes.ErrLeaseNotFound) {
			return kerr
		}
	}
	return nil
}

func TestV3PutOnNonExistLease(t *testing.T) {
	integration.BeforeTest(t)
	clus := integration.NewCluster(t, &integration.ClusterConfig{Size: 1})
	defer clus.Terminate(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	badLeaseID := int64(0x12345678)
	putr := &pb.PutRequest{Key: []byte("foo"), Value: []byte("bar"), Lease: badLeaseID}
	_, err := integration.ToGRPC(clus.RandClient()).KV.Put(ctx, putr)
	if !eqErrGRPC(err, rpctypes.ErrGRPCLeaseNotFound) {
		t.Errorf("err = %v, want %v", err, rpctypes.ErrGRPCLeaseNotFound)
	}
}

// TestV3GetNonExistLease ensures client retrieving nonexistent lease on a follower doesn't result node panic
// related issue https://github.com/etcd-io/etcd/issues/6537
func TestV3GetNonExistLease(t *testing.T) {
	integration.BeforeTest(t)
	clus := integration.NewCluster(t, &integration.ClusterConfig{Size: 3})
	defer clus.Terminate(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	lc := integration.ToGRPC(clus.RandClient()).Lease
	lresp, err := lc.LeaseGrant(ctx, &pb.LeaseGrantRequest{TTL: 10})
	if err != nil {
		t.Errorf("failed to create lease %v", err)
	}
	_, err = lc.LeaseRevoke(context.TODO(), &pb.LeaseRevokeRequest{ID: lresp.ID})
	require.NoError(t, err)

	leaseTTLr := &pb.LeaseTimeToLiveRequest{
		ID:   lresp.ID,
		Keys: true,
	}

	for _, m := range clus.Members {
		// quorum-read to ensure revoke completes before TimeToLive
		_, err := integration.ToGRPC(m.Client).KV.Range(ctx, &pb.RangeRequest{Key: []byte("_")})
		require.NoError(t, err)
		resp, err := integration.ToGRPC(m.Client).Lease.LeaseTimeToLive(ctx, leaseTTLr)
		if err != nil {
			t.Fatalf("expected non nil error, but go %v", err)
		}
		if resp.TTL != -1 {
			t.Fatalf("expected TTL to be -1, but got %v", resp.TTL)
		}
	}
}

// TestV3LeaseSwitch tests a key can be switched from one lease to another.
func TestV3LeaseSwitch(t *testing.T) {
	integration.BeforeTest(t)
	clus := integration.NewCluster(t, &integration.ClusterConfig{Size: 3})
	defer clus.Terminate(t)

	key := "foo"

	// create lease
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	lresp1, err1 := integration.ToGRPC(clus.RandClient()).Lease.LeaseGrant(ctx, &pb.LeaseGrantRequest{TTL: 30})
	require.NoError(t, err1)
	lresp2, err2 := integration.ToGRPC(clus.RandClient()).Lease.LeaseGrant(ctx, &pb.LeaseGrantRequest{TTL: 30})
	require.NoError(t, err2)

	// attach key on lease1 then switch it to lease2
	put1 := &pb.PutRequest{Key: []byte(key), Lease: lresp1.ID}
	_, err := integration.ToGRPC(clus.RandClient()).KV.Put(ctx, put1)
	require.NoError(t, err)
	put2 := &pb.PutRequest{Key: []byte(key), Lease: lresp2.ID}
	_, err = integration.ToGRPC(clus.RandClient()).KV.Put(ctx, put2)
	require.NoError(t, err)

	// revoke lease1 should not remove key
	_, err = integration.ToGRPC(clus.RandClient()).Lease.LeaseRevoke(ctx, &pb.LeaseRevokeRequest{ID: lresp1.ID})
	require.NoError(t, err)
	rreq := &pb.RangeRequest{Key: []byte("foo")}
	rresp, err := integration.ToGRPC(clus.RandClient()).KV.Range(context.TODO(), rreq)
	require.NoError(t, err)
	if len(rresp.Kvs) != 1 {
		t.Fatalf("unexpect removal of key")
	}

	// revoke lease2 should remove key
	_, err = integration.ToGRPC(clus.RandClient()).Lease.LeaseRevoke(ctx, &pb.LeaseRevokeRequest{ID: lresp2.ID})
	require.NoError(t, err)
	rresp, err = integration.ToGRPC(clus.RandClient()).KV.Range(context.TODO(), rreq)
	require.NoError(t, err)
	if len(rresp.Kvs) != 0 {
		t.Fatalf("lease removed but key remains")
	}
}

const fiveMinTTL int64 = 300

// acquireLeaseAndKey creates a new lease and creates an attached key.
func acquireLeaseAndKey(clus *integration.Cluster, key string) (int64, error) {
	// create lease
	lresp, err := integration.ToGRPC(clus.RandClient()).Lease.LeaseGrant(
		context.TODO(),
		&pb.LeaseGrantRequest{TTL: 1})
	if err != nil {
		return 0, err
	}
	if lresp.Error != "" {
		return 0, errors.New(lresp.Error)
	}
	// attach to key
	put := &pb.PutRequest{Key: []byte(key), Lease: lresp.ID}
	if _, err := integration.ToGRPC(clus.RandClient()).KV.Put(context.TODO(), put); err != nil {
		return 0, err
	}
	return lresp.ID, nil
}

// testLeaseRemoveLeasedKey performs some action while holding a lease with an
// attached key "foo", then confirms the key is gone.
func testLeaseRemoveLeasedKey(t *testing.T, act func(*integration.Cluster, int64) error) {
	clus := integration.NewCluster(t, &integration.ClusterConfig{Size: 3})
	defer clus.Terminate(t)

	leaseID, err := acquireLeaseAndKey(clus, "foo")
	require.NoError(t, err)

	err = act(clus, leaseID)
	require.NoError(t, err)

	// confirm no key
	rreq := &pb.RangeRequest{Key: []byte("foo")}
	rresp, err := integration.ToGRPC(clus.RandClient()).KV.Range(context.TODO(), rreq)
	require.NoError(t, err)
	if len(rresp.Kvs) != 0 {
		t.Fatalf("lease removed but key remains")
	}
}

func leaseExist(t *testing.T, clus *integration.Cluster, leaseID int64) bool {
	l := integration.ToGRPC(clus.RandClient()).Lease

	_, err := l.LeaseGrant(context.Background(), &pb.LeaseGrantRequest{ID: leaseID, TTL: 5})
	if err == nil {
		_, err = l.LeaseRevoke(context.Background(), &pb.LeaseRevokeRequest{ID: leaseID})
		if err != nil {
			t.Fatalf("failed to check lease %v", err)
		}
		return false
	}

	if eqErrGRPC(err, rpctypes.ErrGRPCLeaseExist) {
		return true
	}
	t.Fatalf("unexpecter error %v", err)

	return true
}
