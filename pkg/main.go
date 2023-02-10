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
	"time"
)

func main() {
	var kubeconfig *string
	if home := homedir.HomeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	flag.Parse()

	// use the current context in kubeconfig
	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		panic(err.Error())
	}

	// create the ListWatcher
	gv, err := schema.ParseGroupVersion("api/v1")
	config.ContentConfig = rest.ContentConfig{
		GroupVersion:         &gv,
		NegotiatedSerializer: scheme.Codecs.WithoutConversion(),
	}
	c, err := rest.RESTClientFor(config)
	if err != nil {
		panic(err.Error())
	}
	lw := cache.NewListWatchFromClient(c, "pods", "cattle-system", fields.Everything())

	fieldFuncs := map[string]sqlcache.FieldFunc{}
	loi, err := sqlcache.NewListOptionIndexer(&v1.Pod{}, "pods.sqlite", fieldFuncs)

	r := cache.NewReflector(lw, &v1.Pod{}, loi, time.Hour)

	var wg wait.Group
	stopCh := make(chan struct{})
	wg.StartWithChannel(stopCh, r.Run)
	wg.Wait()
}
