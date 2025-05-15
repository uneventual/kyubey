# Kyubey

A datastore for the Kubernetes control plane that can scale.

- Now: a FoundationDB-based, Etcd-compatible durable datastore that can comfortably scale to millions of reads and writes per second without compromising durability or consistency
- To come: A KeyDB/Redis-based caching layer that lives as a sidecar to the API Server, handling queries and coalescing watches even when you outgrow the stock Kubernetes watch cache