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
* `sqlcache.NewVersionedStore` returns a SQLite-backed cache.Store instance that keeps track of past versions of resources

Next steps:
* Build a `Reflector` with a `VersionedStore`
* Potentially build a `VersionedIndexer`
* Add capabilities to sort/filter/paginate

This project has originated in [SUSE HackWeek](https://hackweek.opensuse.org/22/projects/vai-a-kubernetes-api-caching-layer).
