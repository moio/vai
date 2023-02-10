# Vai - SQLite-based client-go alternative components

![Kubernetes API caching layer according to Stable Diffusion](doc/vai_logo_small.png)

This project contains alternative versions of client-go concepts - `Stores`, `Indexers`, `ThreadSafeStores`, `Reflectors` and possibly `Informers` backed by lightweight SQLite databases.

[For a gentle introduction on client-go's basic projects see this excellent guide](https://github.com/aiyengar2/k8s-docs/blob/main/docs/controllers/01_clients.md).

The aim is to allow memory-efficient and still fast enough contstructs when dealing with large Kubernetes installations with hundreds of thousands of resources and more.

Additionally, SQLite make it easier to develop custom sorting, filtering, pagination, aggregation and so on - eg. to build GUIs.

## Current status

* `sqlcache.NewStore` returns a SQLite-backed cache.Store instance that passes client-go's unit tests
* `sqlcache.NewIndexer` returns a SQLite-backed cache.Indexer instance that passes client-go's unit tests
* `sqlcache.NewThreadSafeStore` returns a SQLite-backed cache.NewThreadSafeStore instance that passes client-go's unit tests
* `sqlcache.NewVersionedIndexer` returns a SQLite-backed cache.Indexer instance that keeps track of past versions of resources
* `sqlcache.NewListOptionIndexer` returns a SQLite-backed cache.Indexer instance that can satisfy a Rancher [steve](https://github.com/rancher/steve)'s [ListOptions](https://github.com/rancher/steve/blob/53fbb87f5968222d47e55759d87e1f1b93a4533b/pkg/stores/partition/listprocessor/processor.go#L27) query object
* it is possible to set up a `Reflector` to populate a `ListOptionIndexer` from a Kubernetes API, see `examples/reflector/main.go` for an example
* it is possible to set up a `SharedIndexInformer` to populate a `ListOptionIndexer` from a Kubernetes API, see `examples/informer/main.go` for an example

Next steps:
* try to integrate in [steve](https://github.com/rancher/steve)
* add garbage collector

This project has originated in [SUSE HackWeek](https://hackweek.opensuse.org/22/projects/vai-a-kubernetes-api-caching-layer).
