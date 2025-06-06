package fr.pierrezemb.fdb.layer.etcd.service;

import fr.pierrezemb.fdb.layer.etcd.AbstractFDBContainer;
import fr.pierrezemb.fdb.layer.etcd.MainVerticle;
import fr.pierrezemb.fdb.layer.etcd.PortManager;
import fr.pierrezemb.fdb.layer.etcd.TestUtil;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.Txn;
import io.etcd.jetcd.kv.DeleteResponse;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.PutResponse;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.DeleteOption;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static fr.pierrezemb.fdb.layer.etcd.TestUtil.bytesOf;
import static fr.pierrezemb.fdb.layer.etcd.TestUtil.randomByteSequence;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Taken from https://github.com/etcd-io/jetcd/blob/jetcd-0.5.0/jetcd-core/src/test/java/io/etcd/jetcd/KVTest.java
 */
@ExtendWith(VertxExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class KVServiceTest extends AbstractFDBContainer {

  private static final ByteSequence SAMPLE_KEY = ByteSequence.from("sample_key".getBytes());
  private static final ByteSequence SAMPLE_VALUE = ByteSequence.from("sample_value".getBytes());
  private static final ByteSequence SAMPLE_KEY_2 = ByteSequence.from("sample_key2".getBytes());
  private static final ByteSequence SAMPLE_VALUE_2 = ByteSequence.from("sample_value2".getBytes());
  private static final ByteSequence SAMPLE_KEY_3 = ByteSequence.from("sample_key3".getBytes());
  public final int port = PortManager.nextFreePort();
  private KV kvClient;
  private Client client;
  private File clusterFile;

  @BeforeAll
  void deploy_verticle(Vertx vertx, VertxTestContext testContext) throws IOException, InterruptedException {

    clusterFile = container.clearAndGetClusterFile();

    DeploymentOptions options = new DeploymentOptions()
      .setConfig(new JsonObject().put("fdb-cluster-file", clusterFile.getAbsolutePath()).put("listen-port", port)
      );

    // deploy verticle
    vertx.deployVerticle(new MainVerticle(), options, testContext.succeeding(id -> {
      // create client
      client = Client.builder().endpoints("http://localhost:" + port).build();
      // uncomment this to test on real etcd
      // client = Client.builder().endpoints("http://localhost:2379").build();
      kvClient = client.getKVClient();

      testContext.completeNow();
    }));

  }

  @Test
  public void testDelete() throws Exception {

    // cleanup any tests that wrote in the (r, t) range
    kvClient.delete(ByteSequence.from(
      "r".getBytes()),
      DeleteOption.newBuilder().withRange(ByteSequence.from("t".getBytes())).build()).get();
    System.out.println("cleanup complete");

    // Put content so that we actually have something to delete
    testPut();
    ByteSequence keyToDelete = SAMPLE_KEY;

    // count keys about to delete
    CompletableFuture<GetResponse> getFeature = kvClient.get(keyToDelete);
    GetResponse resp = getFeature.get();
    assertEquals(1, resp.getKvs().size());

    // delete the keys
    CompletableFuture<DeleteResponse> deleteFuture = kvClient.delete(keyToDelete);
    DeleteResponse delResp = deleteFuture.get();
    assertEquals(resp.getKvs().size(), delResp.getDeleted());

    GetResponse responseAfterDelete = kvClient.get(keyToDelete).get();
    assertEquals(0, responseAfterDelete.getKvs().size());
  }

  @Test
  public void testPut() throws Exception {
    CompletableFuture<io.etcd.jetcd.kv.PutResponse> feature = kvClient.put(SAMPLE_KEY, SAMPLE_VALUE);
    PutResponse response = feature.get();
    assertNotNull(response.getHeader());
    assertFalse(response.hasPrevKv());
  }

  @Test
  @Disabled("This was broken in the original")
  public void testPutWithNotExistLease() throws InterruptedException {
    PutOption option = PutOption.newBuilder().withLeaseId(99999).build();
    Exception exception = Assertions.assertThrows(ExecutionException.class, () -> {
      kvClient.put(SAMPLE_KEY, SAMPLE_VALUE, option).get();
    });

    String expectedMessage = "etcdserver: requested lease not found";
    String actualMessage = exception.getMessage();

    // TODO(PZ): change the grpc error to the good one
    assertTrue(actualMessage.contains(expectedMessage));
  }

  @Test
  public void testGet() throws Exception {
    CompletableFuture<PutResponse> feature = kvClient.put(SAMPLE_KEY_2, SAMPLE_VALUE_2);
    feature.get();
    CompletableFuture<GetResponse> getFeature = kvClient.get(SAMPLE_KEY_2);
    GetResponse response = getFeature.get();
    assertEquals(1, response.getKvs().size());
    assertEquals(SAMPLE_VALUE_2.toString(UTF_8), response.getKvs().get(0).getValue().toString(UTF_8));
    assertTrue(!response.isMore());
  }

  @Test
  public void testGetWithRev() throws Exception {
    CompletableFuture<PutResponse> feature = kvClient.put(SAMPLE_KEY_3, SAMPLE_VALUE);
    PutResponse putResp = feature.get();
    kvClient.put(SAMPLE_KEY_3, SAMPLE_VALUE_2).get();
    GetOption option = GetOption.newBuilder().withRevision(putResp.getHeader().getRevision()).build();
    System.out.println(option.getRevision());
    CompletableFuture<GetResponse> getFeature = kvClient.get(SAMPLE_KEY_3, option);
    GetResponse response = getFeature.get();
    assertEquals(1, response.getKvs().size());
    assertEquals(SAMPLE_VALUE.toString(UTF_8), response.getKvs().get(0).getValue().toString(UTF_8));
  }

  @Test
  public void testGetSortedPrefix() throws Exception {
    String prefix = TestUtil.randomString();
    int numPrefix = 3;
    putKeysWithPrefix(prefix, numPrefix);

    GetOption option = GetOption.newBuilder().withSortField(GetOption.SortTarget.KEY).withSortOrder(GetOption.SortOrder.DESCEND)
      .withPrefix(bytesOf(prefix)).build();
    CompletableFuture<GetResponse> getFeature = kvClient.get(bytesOf(prefix), option);
    GetResponse response = getFeature.get();

    assertEquals(numPrefix, response.getKvs().size());
    for (int i = 0; i < numPrefix; i++) {
      assertEquals(prefix + (numPrefix - i - 1), response.getKvs().get(i).getKey().toString(UTF_8));
      assertEquals(String.valueOf(numPrefix - i - 1), response.getKvs().get(i).getValue().toString(UTF_8));
    }
  }


  @Test
  public void testGetAndDeleteWithPrefix() throws Exception {
    String prefix = TestUtil.randomString();
    ByteSequence key = bytesOf(prefix);
    int numPrefixes = 10;

    putKeysWithPrefix(prefix, numPrefixes);

    // verify get withPrefix.
    CompletableFuture<GetResponse> getFuture = kvClient.get(key, GetOption.newBuilder().withPrefix(key).build());
    GetResponse getResp = getFuture.get();
    assertEquals(numPrefixes, getResp.getCount());

    // verify del withPrefix.
    DeleteOption deleteOpt = DeleteOption.newBuilder().withPrefix(key).build();
    CompletableFuture<DeleteResponse> delFuture = kvClient.delete(key, deleteOpt);
    DeleteResponse delResp = delFuture.get();
    assertEquals(numPrefixes, delResp.getDeleted());
  }

  private void putKeysWithPrefix(String prefix, int numPrefixes) throws ExecutionException, InterruptedException {
    for (int i = 0; i < numPrefixes; i++) {
      ByteSequence key = bytesOf(prefix + i);
      ByteSequence value = bytesOf("" + i);
      kvClient.put(key, value).get();
    }
  }

  @Test
  public void testTxn() throws Exception {
    ByteSequence sampleKey = bytesOf("txn_key");
    ByteSequence sampleValue = bytesOf("xyz");
    ByteSequence cmpValue = bytesOf("abc");
    ByteSequence putValue = bytesOf("XYZ");
    ByteSequence putValueNew = bytesOf("ABC");
    // put the original txn key value pair
    kvClient.put(sampleKey, sampleValue).get();

    // construct txn operation
    Txn txn = kvClient.txn();
    Cmp cmp = new Cmp(sampleKey, Cmp.Op.GREATER, CmpTarget.value(cmpValue));
    CompletableFuture<io.etcd.jetcd.kv.TxnResponse> txnResp = txn.If(cmp)
      .Then(Op.put(sampleKey, putValue, PutOption.DEFAULT)).Else(Op.put(sampleKey, putValueNew, PutOption.DEFAULT))
      .commit();
    txnResp.get();
    // get the value
    GetResponse getResp = kvClient.get(sampleKey).get();
    assertEquals(1, getResp.getKvs().size());
    assertEquals(putValue.toString(UTF_8), getResp.getKvs().get(0).getValue().toString(UTF_8));
  }

  @Test
  public void testNestedTxn() throws Exception {
    ByteSequence foo = bytesOf("txn_foo");
    ByteSequence bar = bytesOf("txn_bar");
    ByteSequence barz = bytesOf("txn_barz");
    ByteSequence abc = bytesOf("txn_abc");
    ByteSequence oneTwoThree = bytesOf("txn_123");

    var txn = kvClient.txn();
    Cmp cmp = new Cmp(foo, Cmp.Op.EQUAL, CmpTarget.version(0));
    CompletableFuture<io.etcd.jetcd.kv.TxnResponse> txnResp = txn.If(cmp)
      .Then(Op.put(foo, bar, PutOption.DEFAULT),
        Op.txn(null, new Op[]{Op.put(abc, oneTwoThree, PutOption.DEFAULT)}, null))
      .Else(Op.put(foo, barz, PutOption.DEFAULT)).commit();
    txnResp.get();

    GetResponse getResp = kvClient.get(foo).get();
    assertEquals(1, getResp.getKvs().size());
    assertEquals(bar.toString(UTF_8), getResp.getKvs().get(0).getValue().toString(UTF_8));

    GetResponse getResp2 = kvClient.get(abc).get();
    assertEquals(1, getResp2.getKvs().size());
    assertEquals(oneTwoThree.toString(UTF_8), getResp2.getKvs().get(0).getValue().toString(UTF_8));
  }

  @Test
  public void compact() throws Exception {
    final ByteSequence key = randomByteSequence();
    final ByteSequence value = randomByteSequence();

    // Insert key twice to ensure we have at least two revisions
    final PutResponse oldResponse = kvClient.put(key, value).get();
    final PutResponse putResponse = kvClient.put(key, value).get();

    final GetResponse getBeforeCompact = kvClient.get(key, GetOption.newBuilder().withRevision(oldResponse.getHeader().getRevision()).build()).get();
    assertEquals("should not be empty", 1, getBeforeCompact.getKvs().size());

    // Compact until latest revision
    client.getKVClient().compact(putResponse.getHeader().getRevision()).get();

    final GetResponse getAfterCompact = kvClient.get(key, GetOption.newBuilder().withRevision(oldResponse.getHeader().getRevision()).build()).get();
    assertEquals("should be empty", 0, getAfterCompact.getKvs().size());
  }

}
