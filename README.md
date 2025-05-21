# Kyubey

A datastore for the Kubernetes control plane that can scale.

- In progress now: a FoundationDB-based, Etcd-compatible durable datastore that can comfortably scale to millions of reads and writes per second without compromising durability or consistency.
  - Status: passes a minimal set of tests, supports transactions as well as all the usual Etcd stuff, currently slogging through the whole Etcd test suite.
 
- Eventually: a caching layer for the apiserver that works even when you outgrow the stock watch cache.


*This would not have been possible without the fantastic work of the contributors to [fdb-etcd](https://github.com/PierreZ/fdb-etcd).*
