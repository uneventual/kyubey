// File: KotlinEtcdRecordLayer.kt
// Package is identical to the Java class so the two sit side‑by‑side.
package fr.pierrezemb.fdb.layer.etcd.store

import fr.pierrezemb.etcd.record.pb.EtcdRecord
import mvccpb.EtcdIoKvProto
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeoutException
import kotlin.jvm.Throws          // ← add this


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

  fun txn() {

  }
}
