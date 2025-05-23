package fr.pierrezemb.fdb.layer.etcd.grpc;

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.google.protobuf.InvalidProtocolBufferException;
import etcdserverpb.EtcdIoRpcProto;
import etcdserverpb.VertxKVGrpc;
import fr.pierrezemb.etcd.record.pb.EtcdRecord;
import fr.pierrezemb.fdb.layer.etcd.store.EtcdRecordLayer;
import fr.pierrezemb.fdb.layer.etcd.utils.ProtoUtils;
import io.vertx.core.Future;
import mvccpb.EtcdIoKvProto;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static fr.pierrezemb.fdb.layer.etcd.utils.Misc.createComparatorFromRequest;

/**
 * KVService corresponds to the KV GRPC service
 */
public class KVService extends VertxKVGrpc.KVVertxImplBase {

  private final EtcdRecordLayer recordLayer;

  public KVService(EtcdRecordLayer recordLayer) {
    this.recordLayer = recordLayer;
  }

  @Override
  public Future<EtcdIoRpcProto.RangeResponse> range(EtcdIoRpcProto.RangeRequest request) {
    String tenantId = GrpcContextKeys.TENANT_ID_KEY.get();
    if (tenantId == null) {
      return Future.failedFuture("Auth enabled and tenant not found.");
    }

    List<EtcdRecord.KeyValue> results = new ArrayList<>();
    int version = Math.toIntExact(request.getRevision());

    if (request.getRangeEnd().isEmpty()) {
      // get
      results.add(recordLayer.get(tenantId, request.getKey().toByteArray(), version));
    } else {
      // scan
      results = recordLayer.scan(tenantId, request.getKey().toByteArray(), request.getRangeEnd().toByteArray(), version);
    }

    List<EtcdIoKvProto.KeyValue> kvs = results.stream()
      .flatMap(Stream::ofNullable)
      .map(e -> EtcdIoKvProto.KeyValue.newBuilder()
        .setKey(e.getKey()).setValue(e.getValue()).build()).collect(Collectors.toList());

    if (request.getSortOrder().getNumber() > 0) {
      kvs.sort(createComparatorFromRequest(request));
    }

    // ugly hack !!!
    long revision = 0;

    if (kvs.size() > 0) {
      revision = kvs.get(0).getModRevision();
    }
    return Future.succeededFuture(EtcdIoRpcProto.RangeResponse.newBuilder()
    .setHeader(EtcdIoRpcProto.ResponseHeader.newBuilder().setRevision(revision).build())
    .addAllKvs(kvs).setCount(kvs.size()).build());
  }


  @Override
  public Future<EtcdIoRpcProto.PutResponse> put(EtcdIoRpcProto.PutRequest request) {
    EtcdRecord.KeyValue put;

    String tenantId = GrpcContextKeys.TENANT_ID_KEY.get();
    if (tenantId == null) {
      throw new RuntimeException("Auth enabled and tenant not found.");
    }

    if (request.getLease() > 0) {
      EtcdRecord.Lease lease = this.recordLayer.get(tenantId, request.getLease());
      if (null == lease) {
        return Future.failedFuture("etcdserver: requested lease not found");
      }
    }

    try {
      put = this.recordLayer.put(tenantId, ProtoUtils.from(request));
    } catch (InvalidProtocolBufferException e) {
      return Future.failedFuture(e);
    }
    return Future.succeededFuture(EtcdIoRpcProto
      .PutResponse.newBuilder()
      .setHeader(
        EtcdIoRpcProto.ResponseHeader.newBuilder().setRevision(put.getModRevision()).build()
      ).build());
  }

  @Override
  public Future<EtcdIoRpcProto.DeleteRangeResponse> deleteRange(EtcdIoRpcProto.DeleteRangeRequest request) {
    String tenantId = GrpcContextKeys.TENANT_ID_KEY.get();
    if (tenantId == null) {
      return Future.failedFuture("Tenant id not found.");
    }
    Integer count = this.recordLayer.delete(
      tenantId,
      request.getKey().toByteArray(),
      request.getRangeEnd().isEmpty() ? request.getKey().toByteArray() : request.getRangeEnd().toByteArray());

    return Future.succeededFuture(EtcdIoRpcProto.DeleteRangeResponse.newBuilder().setDeleted(count.longValue())
    // todo: FIX
    .setHeader(EtcdIoRpcProto.ResponseHeader.newBuilder().setRevision(0).build())
    .build());
  }

  @Override
  public Future<EtcdIoRpcProto.TxnResponse> txn(EtcdIoRpcProto.TxnRequest request) {
    String tenantId = GrpcContextKeys.TENANT_ID_KEY.get();


    return Future.succeededFuture(this.recordLayer.txn(tenantId, request));
  }

  @Override
  public Future<EtcdIoRpcProto.CompactionResponse> compact(EtcdIoRpcProto.CompactionRequest request) {
    String tenantId = GrpcContextKeys.TENANT_ID_KEY.get();
    if (tenantId == null) {
      return Future.failedFuture("Tenant id not found.");
    }
    this.recordLayer.compact(tenantId, request.getRevision());
    return Future.succeededFuture(EtcdIoRpcProto.CompactionResponse.newBuilder().build());
  }
}
