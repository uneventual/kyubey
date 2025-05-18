// File: KotlinEtcdRecordLayer.kt
// Package is identical to the Java class so the two sit side‑by‑side.
package fr.pierrezemb.fdb.layer.etcd.store

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext
import com.google.protobuf.ByteString
import etcdserverpb.EtcdIoRpcProto
import fr.pierrezemb.etcd.record.pb.EtcdRecord
import fr.pierrezemb.fdb.layer.etcd.utils.Misc.createComparatorFromRequest
import fr.pierrezemb.fdb.layer.etcd.utils.ProtoUtils
import mvccpb.EtcdIoKvProto
import java.util.*
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeoutException
import java.util.stream.Collectors
import java.util.stream.Stream
import etcdserverpb.EtcdIoRpcProto.*;


fun RangeResponse.toResponseOp(): ResponseOp =
  ResponseOp.newBuilder()
    .setResponseRange(this)
    .build()

fun PutResponse.toResponseOp(): ResponseOp =
  ResponseOp.newBuilder()
    .setResponsePut(this)
    .build()

fun DeleteRangeResponse.toResponseOp(): ResponseOp =
  ResponseOp.newBuilder()
    .setResponseDeleteRange(this)
    .build()

fun TxnResponse.toResponseOp(): ResponseOp =
  ResponseOp.newBuilder()
    .setResponseTxn(this)
    .build()

fun Long.toDeleteRangeResponse(): DeleteRangeResponse =
  DeleteRangeResponse.newBuilder().setDeleted(this).build()

fun EtcdRecord.KeyValue.toPutResponse(): PutResponse =
  EtcdIoRpcProto
  .PutResponse.newBuilder()
  .setHeader(
  EtcdIoRpcProto.ResponseHeader.newBuilder().setRevision(this.modRevision).build()
  ).build()

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

   fun range_with_context(tenantID: String, request: EtcdIoRpcProto.RangeRequest): (FDBRecordContext) -> EtcdIoRpcProto.RangeResponse {
     return { context ->

       var results: MutableList<EtcdRecord.KeyValue?> = ArrayList<EtcdRecord.KeyValue?>()
       val version = Math.toIntExact(request.revision)

       if (request.rangeEnd.isEmpty) {
         // get
         results.add(get_with_context(tenantID, request.key.toByteArray(), version.toLong()).apply(context))
       } else {
         // scan
         results = scan_with_context(
           tenantID,
           request.key.toByteArray(),
           request.rangeEnd.toByteArray(),
           version.toLong()
         ).apply(context)
       }

       val kvs = results.stream()
         .flatMap<EtcdRecord.KeyValue?> { t: EtcdRecord.KeyValue? -> Stream.ofNullable(t) }
         .map<EtcdIoKvProto.KeyValue?> { e: EtcdRecord.KeyValue? ->
           EtcdIoKvProto.KeyValue.newBuilder()
             .setKey(e!!.key).setValue(e.value).build()
         }.collect(Collectors.toList())

       if (request.getSortOrder().getNumber() > 0) {
         kvs.sortWith(createComparatorFromRequest(request))
       }


       EtcdIoRpcProto.RangeResponse.newBuilder().addAllKvs(kvs).setCount(kvs.size.toLong()).build()
     }
  }

  private fun recursiveTxnOp(request: List<EtcdIoRpcProto.RequestOp>, context: FDBRecordContext, tenantID: String): List<EtcdIoRpcProto.ResponseOp> {
    return request.map { r ->
      when(r.requestCase) {
        EtcdIoRpcProto.RequestOp.RequestCase.REQUEST_RANGE -> range_with_context(tenantID, r.requestRange).invoke(context).toResponseOp()
        EtcdIoRpcProto.RequestOp.RequestCase.REQUEST_PUT ->
        put_with_context(tenantID,
          ProtoUtils.from(r.requestPut)).apply(context).toPutResponse().toResponseOp()
        EtcdIoRpcProto.RequestOp.RequestCase.REQUEST_DELETE_RANGE ->
          delete_with_context(tenantID,
          r.requestDeleteRange.key.toByteArray(),
          r.requestDeleteRange.rangeEnd.toByteArray()).apply(context).toLong().toDeleteRangeResponse().toResponseOp()
        EtcdIoRpcProto.RequestOp.RequestCase.REQUEST_TXN ->
          recursiveTxn(r.requestTxn, context, tenantID).toResponseOp()
        EtcdIoRpcProto.RequestOp.RequestCase.REQUEST_NOT_SET ->
          throw RuntimeException("Failed Deserializing Comparison")
      }
    }
  }


  private fun recursiveTxn(request: EtcdIoRpcProto.TxnRequest, context: FDBRecordContext, tenantID: String): EtcdIoRpcProto.TxnResponse {

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

    val result = if (should) {
      recursiveTxnOp(request.successList, context, tenantID)
    } else {
      recursiveTxnOp(request.failureList, context, tenantID)
    }

    val build = EtcdIoRpcProto.TxnResponse.newBuilder()
      .setSucceeded(should)

    result.forEachIndexed {
      i, r -> build.addResponses(r)
    }

    return build.setHeader(
      ResponseHeader.newBuilder().setRevision(context.readVersion).build()
    ).build()


  }

  fun txn(tenantID: String, request: EtcdIoRpcProto.TxnRequest): TxnResponse {
    return db.run { context ->  recursiveTxn(request, context, tenantID) };
  }

}
