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
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static fr.pierrezemb.fdb.layer.etcd.TestUtil.bytesOf;
import static fr.pierrezemb.fdb.layer.etcd.TestUtil.randomByteSequence;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.*;

/**
 * Taken from https://github.com/etcd-io/jetcd/blob/jetcd-0.5.0/jetcd-core/src/test/java/io/etcd/jetcd/KVTest.java
 */
@ExtendWith(VertxExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class GoEtcdTests extends AbstractFDBContainer {

  private static final ByteSequence SAMPLE_KEY = ByteSequence.from("sample_key".getBytes());
  private static final ByteSequence SAMPLE_VALUE = ByteSequence.from("sample_value".getBytes());
  private static final ByteSequence SAMPLE_KEY_2 = ByteSequence.from("sample_key2".getBytes());
  private static final ByteSequence SAMPLE_VALUE_2 = ByteSequence.from("sample_value2".getBytes());
  private static final ByteSequence SAMPLE_KEY_3 = ByteSequence.from("sample_key3".getBytes());
  private KV kvClient;
  private Client client;
  private File clusterFile;

  @BeforeAll
  void deploy_verticle(Vertx vertx, VertxTestContext testContext) throws IOException, InterruptedException {

    clusterFile = container.clearAndGetClusterFile();



    // deploy verticle
    for (int i = 0; i < 5; i++) {

        int port = PortManager.nextFreePort();
        DeploymentOptions options = new DeploymentOptions()
        .setConfig(new JsonObject().put("fdb-cluster-file", clusterFile.getAbsolutePath()).put("listen-port", port)
        );
        vertx.deployVerticle(new MainVerticle(), options, testContext.succeeding(id -> {

            var endpoint = "http://localhost:" + port;
            // create client
            client = Client.builder().endpoints(endpoint).build();
            System.out.println("server up at " + endpoint);
            // uncomment this to test on real etcd
            // client = Client.builder().endpoints("http://localhost:2379").build();
            kvClient = client.getKVClient();

            testContext.completeNow();
      }));
    }


  }

  @Test
  public void testDelete() throws Exception {

//    // cleanup any tests that wrote in the (r, t) range
//    kvClient.delete(ByteSequence.from(
//      "r".getBytes()),
//      DeleteOption.newBuilder().withRange(ByteSequence.from("t".getBytes())).build()).get();
//    System.out.println("cleanup complete");
//
//    // Put content so that we actually have something to delete
//    ByteSequence keyToDelete = SAMPLE_KEY;
//
//    // count keys about to delete
//    CompletableFuture<GetResponse> getFeature = kvClient.get(keyToDelete);
//    GetResponse resp = getFeature.get();
//    assertEquals(1, resp.getKvs().size());
//
//    // delete the keys
//    CompletableFuture<DeleteResponse> deleteFuture = kvClient.delete(keyToDelete);
//    DeleteResponse delResp = deleteFuture.get();
//    assertEquals(resp.getKvs().size(), delResp.getDeleted());
//
//    GetResponse responseAfterDelete = kvClient.get(keyToDelete).get();
//    assertEquals(0, responseAfterDelete.getKvs().size());
//  }
    System.out.println("Program started. Will run until killed.");
    System.out.println("Press Ctrl+C or send a kill signal to terminate.");

    // Register a shutdown hook to detect when the program is being terminated
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      System.out.println("Shutdown signal received. Cleaning up...");
      System.out.println("Goodbye!");
    }));

    try {
      // Method 1: Using an object's wait() method that will never be notified
      final Object lock = new Object();
      synchronized (lock) {
        lock.wait();
      }

      // Alternative approaches:
      // Method 2: Infinite loop
      // while (true) {
      //     Thread.sleep(Long.MAX_VALUE);
      // }

      // Method 3: CountDownLatch that never counts down
      // new CountDownLatch(1).await();
    } catch (InterruptedException e) {
      System.out.println("Interrupted!");
    }

  }
}
