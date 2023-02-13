package main

import (
	"flag"
	"github.com/moio/vai/pkg/sqlcache"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"path/filepath"
)

func main() {
	config := readKubeconfig()
	client := getRESTClient(config)

	// create the ListWatcher
	listWatcher := cache.NewListWatchFromClient(client, "pods", "cattle-system", fields.Everything())

	// create the Indexer
	fieldFuncs := map[string]sqlcache.FieldFunc{
		"metadata.creationTimestamp": func(obj any) any {
			return obj.(*v1.Pod).CreationTimestamp.String()
		},
	}
	indexer, err := sqlcache.NewCustomListOptionIndexer(&v1.Pod{}, cache.DeletionHandlingMetaNamespaceKeyFunc, "pods.sqlite", fieldFuncs, cache.Indexers{})
	if err != nil {
		panic(err)
	}

	// connect the ListWatcher to feed the Indexer
	informer := cache.NewSharedIndexInformer(listWatcher, &v1.Pod{}, 0, cache.Indexers{})
	// HACK: this assumes the cache.SharedIndexInformer is really backed by a cache.sharedIndexInformer
	// or at least that the indexer field is named "indexer". Unfortunately it is not possible to swap the Indexer
	// implementation
	sqlcache.UnsafeSet(informer, "indexer", indexer)

	// go!
	var wg wait.Group
	stopCh := make(chan struct{})
	wg.StartWithChannel(stopCh, informer.Run)
	wg.Wait()
}

func getRESTClient(config *rest.Config) *rest.RESTClient {
	gv, _ := schema.ParseGroupVersion("api/v1")
	config.ContentConfig = rest.ContentConfig{
		GroupVersion:         &gv,
		NegotiatedSerializer: scheme.Codecs.WithoutConversion(),
	}
	c, err := rest.RESTClientFor(config)
	if err != nil {
		panic(err.Error())
	}
	return c
}

func readKubeconfig() *rest.Config {
	var kubeconfig *string
	if home := homedir.HomeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	flag.Parse()

	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		panic(err.Error())
	}
	return config
}
