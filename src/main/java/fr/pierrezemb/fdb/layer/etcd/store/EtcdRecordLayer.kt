// File: KotlinEtcdRecordLayer.kt
// Package is identical to the Java class so the two sit side‑by‑side.
package fr.pierrezemb.fdb.layer.etcd.store

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext
import com.google.protobuf.ByteString
import etcdserverpb.EtcdIoRpcProto
import fr.pierrezemb.etcd.record.pb.EtcdRecord
import fr.pierrezemb.fdb.layer.etcd.grpc.GrpcContextKeys
import fr.pierrezemb.fdb.layer.etcd.utils.ProtoUtils
import io.vertx.core.Future
import mvccpb.EtcdIoKvProto
import java.util.*
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeoutException
import java.util.stream.Collectors
import java.util.stream.Stream


/**
 * Drop‑in Kotlin wrapper for [EtcdRecordLayerJava].
 *
 * Behaviour is 100 % inherited from the Java implementation; we just expose it
 * as an `open` Kotlin class so you can extend / override in Kotlin land.
 *
 * Example:
 * ```
 * class AuditingEtcdLayer(path: String) : KotlinEtcdRecordLayer(path) {
 *     override fun put(tenantID: String, record: EtcdRecord.KeyValue): EtcdRecord.KeyValue {
 *         println("AUDIT → putting ${record.key}")
 *         return super.put(tenantID, record)
 *     }
 * }
 * ```
 */
open class KotlinEtcdRecordLayer
@Throws(                         // ← annotation belongs on the constructor
  InterruptedException::class,
  ExecutionException::class,
  TimeoutException::class
)
constructor(clusterFilePath: String)
  : EtcdRecordLayerJava(clusterFilePath)

//@Throws(InterruptedException::class, ExecutionException::class, TimeoutException::class)
class EtcdRecordLayer
@Throws(
  InterruptedException::class,
  ExecutionException::class,
  TimeoutException::class
)
constructor(clusterFilePath: String)     // delegates to super‑ctor
: KotlinEtcdRecordLayer(clusterFilePath) {

  private fun predicate(comp: EtcdIoRpcProto.Compare): (EtcdRecord.KeyValue) -> Boolean {
    // A small generic helper to do the comparison based on comp.result
    fun <T : Comparable<T>> eval(a: T, b: T): Boolean = when (comp.result) {
      EtcdIoRpcProto.Compare.CompareResult.EQUAL         -> a == b
      EtcdIoRpcProto.Compare.CompareResult.GREATER       -> a > b
      EtcdIoRpcProto.Compare.CompareResult.LESS          -> a < b
      EtcdIoRpcProto.Compare.CompareResult.NOT_EQUAL     -> a != b
      EtcdIoRpcProto.Compare.CompareResult.UNRECOGNIZED  ->
        throw RuntimeException("Failed Deserializing Comparison")
    }

    // Special-case for lexicographic byte‐comparison
    fun evalBytes(a: ByteString, b: ByteString): Boolean {
      val cmp = Arrays.compareUnsigned(a.toByteArray(), b.toByteArray())
      return when (comp.result) {
        EtcdIoRpcProto.Compare.CompareResult.EQUAL     -> cmp == 0
        EtcdIoRpcProto.Compare.CompareResult.GREATER   -> cmp > 0
        EtcdIoRpcProto.Compare.CompareResult.LESS      -> cmp < 0
        EtcdIoRpcProto.Compare.CompareResult.NOT_EQUAL -> cmp != 0
        EtcdIoRpcProto.Compare.CompareResult.UNRECOGNIZED ->
          throw RuntimeException("Failed Deserializing Comparison")
      }
    }

    // Select which field of KeyValue to compare against the corresponding comp value
    return when (comp.target) {
      EtcdIoRpcProto.Compare.CompareTarget.VERSION      -> { kv -> eval(kv.version,        comp.version) }
      EtcdIoRpcProto.Compare.CompareTarget.CREATE       -> { kv -> eval(kv.createRevision, comp.createRevision) }
      EtcdIoRpcProto.Compare.CompareTarget.MOD          -> { kv -> eval(kv.modRevision,    comp.modRevision) }
      EtcdIoRpcProto.Compare.CompareTarget.VALUE        -> { kv -> evalBytes(kv.value, comp.value) }
      EtcdIoRpcProto.Compare.CompareTarget.LEASE        -> { kv -> eval(kv.lease,          comp.lease) }
      EtcdIoRpcProto.Compare.CompareTarget.UNRECOGNIZED ->
        throw RuntimeException("Failed Deserializing Comparison")
    }
  }

  fun range(request: EtcdIoRpcProto.RangeRequest): Future<EtcdIoRpcProto.RangeResponse?> {
    val tenantId = GrpcContextKeys.TENANT_ID_KEY.get()
    if (tenantId == null) {
      return Future.failedFuture<EtcdIoRpcProto.RangeResponse?>("Auth enabled and tenant not found.")
    }

    var results: MutableList<EtcdRecord.KeyValue?> = ArrayList<EtcdRecord.KeyValue?>()
    val version = Math.toIntExact(request.getRevision())

    if (request.getRangeEnd().isEmpty()) {
      // get
      results.add(get(tenantId, request.getKey().toByteArray(), version.toLong()))
    } else {
      // scan
      results = scan(
        tenantId,
        request.getKey().toByteArray(),
        request.getRangeEnd().toByteArray(),
        version.toLong()
      )
    }

    val kvs = results.stream()
      .flatMap<EtcdRecord.KeyValue?> { t: EtcdRecord.KeyValue? -> Stream.ofNullable(t) }
      .map<EtcdIoKvProto.KeyValue?> { e: EtcdRecord.KeyValue? ->
        EtcdIoKvProto.KeyValue.newBuilder()
          .setKey(e!!.getKey()).setValue(e.getValue()).build()
      }.collect(Collectors.toList())

    if (request.getSortOrder().getNumber() > 0) {
      kvs.sort(createComparatorFromRequest(request))
    }

  }

  private fun recursive_txn_op(request: List<EtcdIoRpcProto.RequestOp>, context: FDBRecordContext, tenantID: String): EtcdIoRpcProto.ResponseOp {
    request.map { r ->

      when(r.requestCase) {
        EtcdIoRpcProto.RequestOp.RequestCase.REQUEST_RANGE -> r.requestRange
        EtcdIoRpcProto.RequestOp.RequestCase.REQUEST_PUT -> put_with_context(tenantID, ProtoUtils.from(r.requestPut))
        EtcdIoRpcProto.RequestOp.RequestCase.REQUEST_DELETE_RANGE -> delete_with_context(tenantID, r.requestDeleteRange.key.toByteArray(), r.requestDeleteRange.rangeEnd.toByteArray())
        EtcdIoRpcProto.RequestOp.RequestCase.REQUEST_TXN -> recursive_txn(r.requestTxn, context, tenantID)
        EtcdIoRpcProto.RequestOp.RequestCase.REQUEST_NOT_SET ->
          throw RuntimeException("Failed Deserializing Comparison")
      }

  }
    }


  private fun recursive_txn(request: EtcdIoRpcProto.TxnRequest , context: FDBRecordContext, tenantID: String): EtcdIoRpcProto.TxnResponse {

    val fdbRecordStore = createFDBRecordStore(context, tenantID);

    // TODO: Figure out proper short-circuiting here
    val should = request.compareList.map { comp ->
      val cq = createGetQuery(comp.key.toByteArray(), 0)
      val query = fdbRecordStore.executeQuery(cq)

      query
        .map({qr -> EtcdRecord.KeyValue.newBuilder()
      .mergeFrom(qr.getRecord()).build()})
      .map({v -> predicate(comp)(v) }).asList()
    }.map { v -> v.join() }.flatten().all { v -> v }

    if (should) {
      recursive_txn_op(request.successList, context, tenantID)
    } else {
      recursive_txn_op(request.failureList, context, tenantID)
    }


    return EtcdIoRpcProto.TxnResponse.newBuilder().build();

  }

  fun txn() {

  }
}
