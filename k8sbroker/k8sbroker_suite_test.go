package k8sbroker_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestCsibroker(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "K8sBroker Suite")
}
