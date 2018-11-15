package k8sbroker_test

import (
	"os"
	"path/filepath"

	"code.cloudfoundry.org/csishim/csi_fake"
	"code.cloudfoundry.org/goshims/grpcshim/grpc_fake"
	"code.cloudfoundry.org/k8sbroker/k8sbroker"
	"code.cloudfoundry.org/lager/lagertest"
	"github.com/pivotal-cf/brokerapi"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ServicesRegistry", func() {
	var (
		registry     k8sbroker.ServicesRegistry
		fakeCsi      *csi_fake.FakeCsi
		fakeGrpc     *grpc_fake.FakeGrpc
		specFilepath string
		pwd          string
		initErr      error
		logger       *lagertest.TestLogger
	)

	BeforeEach(func() {
		fakeCsi = &csi_fake.FakeCsi{}
		fakeCsi.NewIdentityClientReturns(&csi_fake.FakeIdentityClient{})
		fakeCsi.NewControllerClientReturns(&csi_fake.FakeControllerClient{})

		fakeGrpc = &grpc_fake.FakeGrpc{}
		logger = lagertest.NewTestLogger("test-broker")

		var err error
		pwd, err = os.Getwd()
		Expect(err).ToNot(HaveOccurred())

		specFilepath = filepath.Join(pwd, "..", "fixtures", "service_spec.json")
	})

	JustBeforeEach(func() {
		registry, initErr = k8sbroker.NewServicesRegistry(
			fakeCsi,
			fakeGrpc,
			specFilepath,
			logger,
		)
	})

	Describe("BrokerServices", func() {
		Context("when the specfile is valid", func() {
			It("returns the service catalog as appropriate", func() {
				Expect(initErr).ToNot(HaveOccurred())

				services := registry.BrokerServices()
				Expect(services).To(HaveLen(2))

				Expect(services[0].ID).To(Equal("ServiceOne.ID"))
				Expect(services[0].Name).To(Equal("ServiceOne.Name"))
				Expect(services[0].Description).To(Equal("ServiceOne.Description"))
				Expect(services[0].Bindable).To(Equal(true))
				Expect(services[0].PlanUpdatable).To(Equal(false))
				Expect(services[0].Tags).To(ContainElement("ServiceOne.Tag1"))
				Expect(services[0].Tags).To(ContainElement("ServiceOne.Tag2"))
				Expect(services[0].Requires).To(ContainElement(brokerapi.RequiredPermission("ServiceOne.Requires")))
				Expect(services[0].Plans[0].Name).To(Equal("ServiceOne.Plans.Name"))
				Expect(services[0].Plans[0].ID).To(Equal("ServiceOne.Plans.ID"))
				Expect(services[0].Plans[0].Description).To(Equal("ServiceOne.Plans.Description"))

				Expect(services[1].ID).To(Equal("ServiceTwo.ID"))
				Expect(services[1].Name).To(Equal("ServiceTwo.Name"))
				Expect(services[1].Description).To(Equal("ServiceTwo.Description"))
				Expect(services[1].Bindable).To(Equal(false))
				Expect(services[1].PlanUpdatable).To(Equal(true))
				Expect(services[1].Tags).To(ContainElement("ServiceTwo.Tag1"))
				Expect(services[1].Tags).To(ContainElement("ServiceTwo.Tag2"))
				Expect(services[1].Requires).To(ContainElement(brokerapi.RequiredPermission("ServiceTwo.Requires")))
				Expect(services[1].Plans[0].Name).To(Equal("ServiceTwo.Plans.Name"))
				Expect(services[1].Plans[0].ID).To(Equal("ServiceTwo.Plans.ID"))
				Expect(services[1].Plans[0].Description).To(Equal("ServiceTwo.Plans.Description"))
			})
		})

		Context("when the specfile is invalid", func() {
			BeforeEach(func() {
				specFilepath = filepath.Join(pwd, "..", "fixtures", "invalid_spec.json")
			})

			It("returns an error", func() {
				Expect(initErr).To(BeAssignableToTypeOf(k8sbroker.ErrInvalidSpecFile{}))
			})
		})

		Context("when the specfile has no services", func() {
			BeforeEach(func() {
				specFilepath = filepath.Join(pwd, "..", "fixtures", "empty_spec.json")
			})

			It("returns an error", func() {
				Expect(initErr).To(Equal(k8sbroker.ErrEmptySpecFile))
			})
		})

		Context("when the specfile has invalid service", func() {
			BeforeEach(func() {
				specFilepath = filepath.Join(pwd, "..", "fixtures", "invalid_service_spec.json")
			})

			It("returns an error", func() {
				Expect(initErr).To(Equal(k8sbroker.ErrInvalidService{Index: 0}))
			})
		})
	})

	Describe("IdentityClient", func() {
		Context("when service exists", func() {
			Context("when service has connection address", func() {
				It("returns csi identity client", func() {
					_, err := registry.IdentityClient("ServiceOne.ID")
					Expect(err).NotTo(HaveOccurred())
					Expect(fakeGrpc.DialCallCount()).To(Equal(1))
					connAddr, _ := fakeGrpc.DialArgsForCall(0)
					Expect(connAddr).To(Equal("0.0.0.0:1000"))
					Expect(fakeCsi.NewIdentityClientCallCount()).To(Equal(1))
				})

				Context("when called second time", func() {
					It("returns the same identity client", func() {
						client1, err := registry.IdentityClient("ServiceOne.ID")
						Expect(err).NotTo(HaveOccurred())
						Expect(fakeGrpc.DialCallCount()).To(Equal(1))
						Expect(fakeCsi.NewIdentityClientCallCount()).To(Equal(1))

						client2, err := registry.IdentityClient("ServiceOne.ID")
						Expect(err).NotTo(HaveOccurred())
						Expect(fakeGrpc.DialCallCount()).To(Equal(1))
						Expect(fakeCsi.NewIdentityClientCallCount()).To(Equal(1))

						Expect(client2).To(Equal(client1))
					})
				})
			})

			Context("when service does not have connection address", func() {
				It("returns noop identity client", func() {
					client, err := registry.IdentityClient("ServiceTwo.ID")
					Expect(err).NotTo(HaveOccurred())
					Expect(fakeGrpc.DialCallCount()).To(Equal(0))
					Expect(fakeCsi.NewIdentityClientCallCount()).To(Equal(0))
					Expect(client).To(BeAssignableToTypeOf(new(k8sbroker.NoopIdentityClient)))
				})
			})
		})

		Context("when service does not exist", func() {
			It("returns an error", func() {
				_, err := registry.IdentityClient("non-existent-service-id")
				Expect(err).To(HaveOccurred())
				Expect(err).To(Equal(k8sbroker.ErrServiceNotFound{ID: "non-existent-service-id"}))
			})
		})
	})

	Describe("ControllerClient", func() {
		Context("when service exists", func() {
			Context("when service has connection address", func() {
				It("returns csi controller client", func() {
					_, err := registry.ControllerClient("ServiceOne.ID")
					Expect(err).NotTo(HaveOccurred())
					Expect(fakeGrpc.DialCallCount()).To(Equal(1))
					connAddr, _ := fakeGrpc.DialArgsForCall(0)
					Expect(connAddr).To(Equal("0.0.0.0:1000"))
					Expect(fakeCsi.NewControllerClientCallCount()).To(Equal(1))
				})

				Context("when called second time", func() {
					It("returns the same identity client", func() {
						client1, err := registry.ControllerClient("ServiceOne.ID")
						Expect(err).NotTo(HaveOccurred())
						Expect(fakeGrpc.DialCallCount()).To(Equal(1))
						Expect(fakeCsi.NewControllerClientCallCount()).To(Equal(1))

						client2, err := registry.ControllerClient("ServiceOne.ID")
						Expect(err).NotTo(HaveOccurred())
						Expect(fakeGrpc.DialCallCount()).To(Equal(1))
						Expect(fakeCsi.NewControllerClientCallCount()).To(Equal(1))

						Expect(client2).To(Equal(client1))
					})
				})
			})

			Context("when service does not have connection address", func() {
				It("returns noop controller client", func() {
					client, err := registry.ControllerClient("ServiceTwo.ID")
					Expect(err).NotTo(HaveOccurred())
					Expect(fakeGrpc.DialCallCount()).To(Equal(0))
					Expect(fakeCsi.NewControllerClientCallCount()).To(Equal(0))
					Expect(client).To(BeAssignableToTypeOf(new(k8sbroker.NoopControllerClient)))
				})
			})
		})

		Context("when service does not exist", func() {
			It("returns an error", func() {
				_, err := registry.ControllerClient("non-existent-service-id")
				Expect(err).To(HaveOccurred())
				Expect(err).To(Equal(k8sbroker.ErrServiceNotFound{ID: "non-existent-service-id"}))
			})
		})
	})
})
