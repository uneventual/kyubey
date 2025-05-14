package fr.pierrezemb.fdb.layer.etcd.utils;

import etcdserverpb.EtcdIoRpcProto;
import mvccpb.EtcdIoKvProto;

import java.util.Comparator;

public class Misc {
  public static Comparator<? super EtcdIoKvProto.KeyValue> createComparatorFromRequest(EtcdIoRpcProto.RangeRequest request) {
    Comparator<EtcdIoKvProto.KeyValue> comparator;
    switch (request.getSortTarget()) {
      case KEY:
        comparator = Comparator.comparing(e -> e.getKey().toStringUtf8());
        break;
      case VERSION:
        comparator = Comparator.comparing(EtcdIoKvProto.KeyValue::getVersion);
        break;
      case CREATE:
        comparator = Comparator.comparing(EtcdIoKvProto.KeyValue::getCreateRevision);
        break;
      case MOD:
        comparator = Comparator.comparing(EtcdIoKvProto.KeyValue::getModRevision);
        break;
      case VALUE:
        comparator = Comparator.comparing(e -> e.getValue().toStringUtf8());
        break;
      default:
        throw new IllegalStateException("Unexpected value: " + request.getSortTarget());
    }

    if (request.getSortOrder().equals(EtcdIoRpcProto.RangeRequest.SortOrder.DESCEND)) {
      comparator = comparator.reversed();
    }

    return comparator;
  }


}
