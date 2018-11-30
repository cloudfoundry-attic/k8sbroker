# k8sbroker

This is a generic K8s service broker that provisions nfs volumes for applications pushed to an Eirini deployment of Cloud Foundry.

## Deploying the k8sbroker

The k8sbroker is a golang application that can be `cf push`ed to an Eirini deployment (configured with diego staging).

To deploy the broker and enable its services in the cf marketplace:

```
$ CF_USERNAME=<username> CF_PASSWORD=<password> ./scripts/deploy.sh <path-to-kube-config.json> <cf-api> <org> <space>
```

where:
- path-to-kube-config.json: is the path to a valid kube config json file that allows connection to the kubernetes cluster 
  from within the cluster itself.  This file will be used by the k8sbroker to manage persistent volumes and persistent volume claims.
- cf-api: the cf api endpoint through which the k8sbroker will be pushed
- org: the org that the k8sbroker will be psuhed into
- space: the space that the k8sbroker will be pushed into

## Using the k8sbroker

```
$ cd ~/workspace/
$ git clone https://github.com/cloudfoundry/persi-acceptance-tests
$ cd ~/workspace/persi-acceptance-tests/assets/pora
$ cf push pora --no-start
$ cf create-service nfs Existing mynfs -c '{"server":"<server>", "share":"<share>"}'
$ cf bind-service pora mynfs
$ cf start pora
```

Refer to: [k8s-volume-release](https://github.com/cloudfoundry/k8s-volume-release) for more information.
