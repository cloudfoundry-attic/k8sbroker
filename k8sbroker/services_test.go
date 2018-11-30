package k8sbroker_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pivotal-cf/brokerapi"

	. "code.cloudfoundry.org/k8sbroker/k8sbroker"
)

var _ = Describe("Services", func() {
	var (
		services Services
	)

	BeforeEach(func() {
		var err error
		services, err = NewServicesFromConfig("../default_services.json")
		Expect(err).NotTo(HaveOccurred())
	})

	Describe("List", func() {
		It("returns the list of services", func() {
			Expect(services.List()).To(Equal([]brokerapi.Service{
				{
					ID:            "db404fc5-97fb-4806-9827-07e0e8d3bd51",
					Name:          "nfs",
					Description:   "Existing NFS volumes",
					Bindable:      true,
					PlanUpdatable: false,
					Tags:          []string{"nfs"},
					Requires:      []brokerapi.RequiredPermission{"volume_mount"},

					Plans: []brokerapi.ServicePlan{
						{
							Name:        "Existing",
							ID:          "190de554-4fc1-4008-ace9-5d3796140b48",
							Description: "A preexisting filesystem",
						},
					},
				},
			}))
		})
	})
})
