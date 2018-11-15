package k8sbroker_test

import (
	"context"
	"encoding/json"
	"errors"

	"code.cloudfoundry.org/goshims/osshim/os_fake"
	"code.cloudfoundry.org/k8sbroker/k8sbroker"
	"code.cloudfoundry.org/k8sbroker/k8sbroker/k8sbroker_fake"
	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/lager/lagertest"
	"code.cloudfoundry.org/service-broker-store/brokerstore"
	"code.cloudfoundry.org/service-broker-store/brokerstore/brokerstorefakes"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pivotal-cf/brokerapi"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ = Describe("Broker", func() {
	var (
		broker                        *k8sbroker.Broker
		fakeOs                        *os_fake.FakeOs
		logger                        lager.Logger
		ctx                           context.Context
		fakeStore                     *brokerstorefakes.FakeStore
		fakeServicesRegistry          *k8sbroker_fake.FakeServicesRegistry
		fakeK8sClient                 *k8sbroker_fake.FakeK8sClient
		fakeK8sPersistentVolumes      *k8sbroker_fake.FakeK8sPersistentVolumes
		fakeK8sPersistentVolumeClaims *k8sbroker_fake.FakeK8sPersistentVolumeClaims
		err                           error
	)

	BeforeEach(func() {
		logger = lagertest.NewTestLogger("test-broker")
		ctx = context.TODO()
		fakeOs = &os_fake.FakeOs{}
		fakeStore = &brokerstorefakes.FakeStore{}
		fakeServicesRegistry = &k8sbroker_fake.FakeServicesRegistry{}

		fakeK8sClient = &k8sbroker_fake.FakeK8sClient{}
		fakeK8sCoreV1 := &k8sbroker_fake.FakeK8sCoreV1{}
		fakeK8sPersistentVolumes = &k8sbroker_fake.FakeK8sPersistentVolumes{}
		fakeK8sPersistentVolumeClaims = &k8sbroker_fake.FakeK8sPersistentVolumeClaims{}
		fakeK8sClient.CoreV1Returns(fakeK8sCoreV1)
		fakeK8sCoreV1.PersistentVolumesReturns(fakeK8sPersistentVolumes)
		fakeK8sCoreV1.PersistentVolumeClaimsReturns(fakeK8sPersistentVolumeClaims)

		fakeServicesRegistry.DriverNameReturns("some-driver-name", nil)
	})

	Context("when creating first time", func() {
		BeforeEach(func() {
			broker, err = k8sbroker.New(
				logger,
				fakeOs,
				nil,
				fakeStore,
				fakeK8sClient,
				"some-namespace",
				fakeServicesRegistry,
			)
			Expect(err).NotTo(HaveOccurred())
		})

		Context(".Services", func() {
			It("returns services registry broker services", func() {
				brokerServices := []brokerapi.Service{
					{ID: "some-service-1"},
					{ID: "some-service-2"},
				}
				fakeServicesRegistry.BrokerServicesReturns(brokerServices)
				Expect(broker.Services(ctx)).To(Equal(brokerServices))
			})
		})

		Context(".Provision", func() {
			var (
				instanceID       string
				provisionDetails brokerapi.ProvisionDetails
				asyncAllowed     bool

				configuration string
				err           error
			)

			BeforeEach(func() {
				instanceID = "some-instance-id"
				configuration = `
        {
           "name": "k8s-volume",
           "capacity_range":{
              "requiredBytes":"2",
              "limitBytes":"3"
           },
           "parameters":{
						 "share": "/export/some-share",
						 "server": "10.0.0.5"
           }
        }
        `
				provisionDetails = brokerapi.ProvisionDetails{PlanID: "CSI", RawParameters: json.RawMessage(configuration)}
				asyncAllowed = false
				fakeStore.RetrieveInstanceDetailsReturns(brokerstore.ServiceInstance{}, errors.New("not found"))
			})

			JustBeforeEach(func() {
				_, err = broker.Provision(ctx, instanceID, provisionDetails, asyncAllowed)
			})

			It("should not error", func() {
				Expect(err).NotTo(HaveOccurred())
			})

			It("should not delete the persistent volume", func() {
				Expect(fakeK8sPersistentVolumes.DeleteCallCount()).To(Equal(0))
			})

			It("should write state", func() {
				Expect(fakeStore.SaveCallCount()).Should(BeNumerically(">", 0))
			})

			It("should send the request to the k8s client", func() {
				expectedQuantity, err := resource.ParseQuantity("2")
				Expect(err).NotTo(HaveOccurred())
				Expect(fakeK8sPersistentVolumes.CreateCallCount()).To(Equal(1))
				requestVolume := fakeK8sPersistentVolumes.CreateArgsForCall(0)
				Expect(requestVolume.TypeMeta).To(Equal(metav1.TypeMeta{
					Kind:       "PersistentVolume",
					APIVersion: "v1",
				}))
				Expect(requestVolume.ObjectMeta).To(Equal(metav1.ObjectMeta{
					Name:   "k8s-volume",
					Labels: map[string]string{"name": "k8s-volume"},
				}))
				Expect(requestVolume.Spec.AccessModes).To(Equal([]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}))
				Expect(requestVolume.Spec.Capacity).To(Equal(v1.ResourceList{v1.ResourceStorage: expectedQuantity}))
				Expect(requestVolume.Spec.PersistentVolumeSource.CSI.Driver).To(Equal("some-driver-name"))
				Expect(requestVolume.Spec.PersistentVolumeSource.CSI.VolumeHandle).To(MatchRegexp(`[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{12}`))
				Expect(requestVolume.Spec.PersistentVolumeSource.CSI.VolumeAttributes).To(HaveKeyWithValue("server", "10.0.0.5"))
				Expect(requestVolume.Spec.PersistentVolumeSource.CSI.VolumeAttributes).To(HaveKeyWithValue("share", "/export/some-share"))
			})

			Context("when creating volume returns volume info", func() {
				var volInfo *v1.PersistentVolume

				BeforeEach(func() {
					volInfo = &v1.PersistentVolume{}
					fakeK8sPersistentVolumes.CreateReturns(volInfo, nil)
				})

				It("should save it", func() {
					Expect(fakeK8sPersistentVolumes.CreateCallCount()).To(Equal(1))

					fingerprint := k8sbroker.ServiceFingerPrint{
						Name:   "k8s-volume",
						Volume: volInfo,
					}

					expectedServiceInstance := brokerstore.ServiceInstance{
						PlanID:             "CSI",
						ServiceFingerPrint: fingerprint,
					}
					Expect(fakeStore.CreateInstanceDetailsCallCount()).To(Equal(1))
					fakeInstanceID, fakeServiceInstance := fakeStore.CreateInstanceDetailsArgsForCall(0)
					Expect(fakeInstanceID).To(Equal(instanceID))
					Expect(fakeServiceInstance).To(Equal(expectedServiceInstance))
					Expect(fakeStore.SaveCallCount()).Should(BeNumerically(">", 0))
				})
			})

			Context("when the client returns an error", func() {
				var createErr error

				BeforeEach(func() {
					createErr = errors.New("some-error")
					fakeK8sPersistentVolumes.CreateReturns(nil, createErr)
				})

				It("should error", func() {
					Expect(err).To(Equal(createErr))
				})
			})

			Context("create-service was given invalid JSON", func() {
				BeforeEach(func() {
					badJson := []byte("{this is not json")
					provisionDetails = brokerapi.ProvisionDetails{PlanID: "CSI", RawParameters: json.RawMessage(badJson)}
				})

				It("errors", func() {
					Expect(err).To(Equal(brokerapi.ErrRawParamsInvalid))
				})
			})

			Context("create-service was given valid JSON but no 'name'", func() {
				BeforeEach(func() {
					configuration := `{}`
					provisionDetails = brokerapi.ProvisionDetails{PlanID: "CSI", RawParameters: json.RawMessage(configuration)}
				})

				It("errors", func() {
					Expect(err).To(Equal(errors.New("config requires a \"name\"")))
				})
			})

			Context("create-service was given valid JSON but no 'capacity_range'", func() {
				BeforeEach(func() {
					configuration := `{"name": "some-name"}`
					provisionDetails = brokerapi.ProvisionDetails{PlanID: "CSI", RawParameters: json.RawMessage(configuration)}
				})

				It("errors", func() {
					Expect(err).To(Equal(errors.New("config requires a \"capacity_range\"")))
				})
			})

			Context("create-service was given valid JSON but no 'server' in parameters", func() {
				BeforeEach(func() {
					configuration = `
					{
						 "name": "k8s-volume",
						 "capacity_range":{
								"requiredBytes":"2",
								"limitBytes":"3"
						 },
						 "parameters":{
							 "share": "/export/some-share"
						 }
					}
					`
					provisionDetails = brokerapi.ProvisionDetails{PlanID: "CSI", RawParameters: json.RawMessage(configuration)}
				})

				It("errors", func() {
					Expect(err).To(Equal(errors.New("config requires a \"server\"")))
				})
			})

			Context("create-service was given valid JSON but no 'share' in parameters", func() {
				BeforeEach(func() {
					configuration = `
					{
						 "name": "k8s-volume",
						 "capacity_range":{
								"requiredBytes":"2",
								"limitBytes":"3"
						 },
						 "parameters":{
							 "server": "10.0.0.5"
						 }
					}
					`
					provisionDetails = brokerapi.ProvisionDetails{PlanID: "CSI", RawParameters: json.RawMessage(configuration)}
				})

				It("errors", func() {
					Expect(err).To(Equal(errors.New("config requires a \"share\"")))
				})
			})

			Context("when the service instance already exists with different details", func() {
				BeforeEach(func() {
					fakeStore.IsInstanceConflictReturns(true)
				})

				It("should error", func() {
					Expect(err).To(Equal(brokerapi.ErrInstanceAlreadyExists))
				})

				It("should delete the persistent volume", func() {
					Expect(fakeK8sPersistentVolumes.DeleteCallCount()).To(Equal(1))
					volumeName, deleteOptions := fakeK8sPersistentVolumes.DeleteArgsForCall(0)
					Expect(volumeName).To(Equal("k8s-volume"))
					Expect(deleteOptions).To(Equal(&metav1.DeleteOptions{
						TypeMeta: metav1.TypeMeta{
							Kind:       "PersistentVolume",
							APIVersion: "v1",
						},
					}))
				})
			})

			Context("when the service instance details creation fails", func() {
				BeforeEach(func() {
					fakeStore.CreateInstanceDetailsReturns(errors.New("badness"))
				})

				It("should error", func() {
					Expect(err).To(HaveOccurred())
				})

				It("should delete the persistent volume", func() {
					Expect(fakeK8sPersistentVolumes.DeleteCallCount()).To(Equal(1))
					volumeName, deleteOptions := fakeK8sPersistentVolumes.DeleteArgsForCall(0)
					Expect(volumeName).To(Equal("k8s-volume"))
					Expect(deleteOptions).To(Equal(&metav1.DeleteOptions{
						TypeMeta: metav1.TypeMeta{
							Kind:       "PersistentVolume",
							APIVersion: "v1",
						},
					}))
				})
			})

			Context("when the save fails", func() {
				BeforeEach(func() {
					fakeStore.SaveReturns(errors.New("badness"))
				})

				It("should error", func() {
					Expect(err).To(HaveOccurred())
				})
			})
		})

		Context(".Deprovision", func() {
			var (
				instanceID         string
				asyncAllowed       bool
				deprovisionDetails brokerapi.DeprovisionDetails
				err                error
			)

			BeforeEach(func() {
				instanceID = "some-instance-id"
				deprovisionDetails = brokerapi.DeprovisionDetails{PlanID: "Existing", ServiceID: "some-service-id"}
				asyncAllowed = true
			})

			JustBeforeEach(func() {
				_, err = broker.Deprovision(ctx, instanceID, deprovisionDetails, asyncAllowed)
			})

			Context("when the instance does not exist", func() {
				BeforeEach(func() {
					instanceID = "does-not-exist"
					fakeStore.RetrieveInstanceDetailsReturns(brokerstore.ServiceInstance{}, brokerapi.ErrInstanceDoesNotExist)
				})

				It("should fail", func() {
					Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
				})
			})

			Context("given an existing instance", func() {
				var (
					previousSaveCallCount int
				)

				BeforeEach(func() {
					asyncAllowed = false

					fingerprint := k8sbroker.ServiceFingerPrint{
						Name: "k8s-volume",
						Volume: &v1.PersistentVolume{
							TypeMeta: metav1.TypeMeta{
								Kind:       "PersistentVolume",
								APIVersion: "v1",
							},
							ObjectMeta: metav1.ObjectMeta{
								Name:   "k8s-volume",
								Labels: map[string]string{"name": "k8s-volume"},
							},
						},
					}

					// simulate untyped data loaded from a data file
					jsonFingerprint := &map[string]interface{}{}
					raw, err := json.Marshal(fingerprint)
					Expect(err).ToNot(HaveOccurred())
					err = json.Unmarshal(raw, jsonFingerprint)
					Expect(err).ToNot(HaveOccurred())

					fakeStore.RetrieveInstanceDetailsReturns(brokerstore.ServiceInstance{
						ServiceID:          "some-service-id",
						ServiceFingerPrint: jsonFingerprint,
					}, nil)
					previousSaveCallCount = fakeStore.SaveCallCount()
				})

				It("should succeed", func() {
					Expect(err).NotTo(HaveOccurred())
				})

				It("saves state", func() {
					Expect(fakeStore.SaveCallCount()).To(Equal(previousSaveCallCount + 1))
				})

				It("should send the request to the k8s client", func() {
					Expect(fakeK8sPersistentVolumes.DeleteCallCount()).To(Equal(1))
					volumeName, deleteOptions := fakeK8sPersistentVolumes.DeleteArgsForCall(0)
					Expect(volumeName).To(Equal("k8s-volume"))
					Expect(deleteOptions).To(Equal(&metav1.DeleteOptions{
						TypeMeta: metav1.TypeMeta{
							Kind:       "PersistentVolume",
							APIVersion: "v1",
						},
					}))
				})

				Context("when the client returns an error", func() {
					var deleteErr error

					BeforeEach(func() {
						deleteErr = errors.New("some-error")
						fakeK8sPersistentVolumes.DeleteReturns(deleteErr)
					})

					It("should error", func() {
						Expect(err).To(Equal(deleteErr))
					})
				})

				Context("when deletion of the instance fails", func() {
					var storeErr error

					BeforeEach(func() {
						storeErr = errors.New("some-error")
						fakeStore.DeleteInstanceDetailsReturns(storeErr)
					})

					It("should error", func() {
						Expect(err).To(Equal(storeErr))
					})
				})

				Context("when the save fails", func() {
					var storeErr error

					BeforeEach(func() {
						storeErr = errors.New("some-error")
						fakeStore.SaveReturns(storeErr)
					})

					It("should error", func() {
						Expect(err).To(Equal(storeErr))
					})
				})

				Context("delete-service was given no instance id", func() {
					BeforeEach(func() {
						instanceID = ""
					})

					It("errors", func() {
						Expect(err).To(Equal(errors.New("volume deletion requires instance ID")))
					})
				})
			})
		})

		Context(".Bind", func() {
			var (
				serviceID     string
				bindDetails   brokerapi.BindDetails
				rawParameters json.RawMessage
				params        map[string]interface{}
				err           error
				binding       brokerapi.Binding
			)

			BeforeEach(func() {
				serviceID = "ServiceOne.ID"
				params = make(map[string]interface{})
				params["key"] = "value"
				rawParameters, err = json.Marshal(params)

				bindDetails = brokerapi.BindDetails{
					AppGUID:       "guid",
					ServiceID:     serviceID,
					RawParameters: rawParameters,
				}
			})

			JustBeforeEach(func() {
				binding, err = broker.Bind(ctx, "some-instance-id", "binding-id", bindDetails)
			})

			Context("when service instance does not exist", func() {
				BeforeEach(func() {
					fakeStore.RetrieveInstanceDetailsReturns(brokerstore.ServiceInstance{}, errors.New("Awesome!"))
				})

				It("errors", func() {
					Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
				})
			})

			Context("when service instance contains invalid service fingerprint", func() {
				BeforeEach(func() {
					fakeStore.RetrieveInstanceDetailsReturns(brokerstore.ServiceInstance{
						ServiceID:          serviceID,
						ServiceFingerPrint: "invalid-json",
					}, nil)
				})

				It("errors", func() {
					Expect(err).To(HaveOccurred())
				})
			})

			Context("when service instance exists", func() {
				var quantity resource.Quantity

				BeforeEach(func() {
					quantity, err = resource.ParseQuantity("2")
					Expect(err).NotTo(HaveOccurred())
					fingerprint := k8sbroker.ServiceFingerPrint{
						Name: "k8s-volume",
						Volume: &v1.PersistentVolume{
							TypeMeta: metav1.TypeMeta{
								Kind:       "PersistentVolume",
								APIVersion: "v1",
							},
							ObjectMeta: metav1.ObjectMeta{
								Name:   "k8s-volume",
								Labels: map[string]string{"name": "k8s-volume"},
							},
							Spec: v1.PersistentVolumeSpec{
								AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteMany},
								Capacity:    v1.ResourceList{v1.ResourceStorage: quantity},
								PersistentVolumeSource: v1.PersistentVolumeSource{
									CSI: &v1.CSIPersistentVolumeSource{
										VolumeHandle: "data-id",
									},
								},
							},
						},
					}

					// simulate untyped data loaded from a data file
					jsonFingerprint := &map[string]interface{}{}
					raw, err := json.Marshal(fingerprint)
					Expect(err).ToNot(HaveOccurred())
					err = json.Unmarshal(raw, jsonFingerprint)
					Expect(err).ToNot(HaveOccurred())
					fakeStore.RetrieveInstanceDetailsReturns(brokerstore.ServiceInstance{
						ServiceID:          serviceID,
						ServiceFingerPrint: jsonFingerprint,
					}, nil)

					fakeK8sPersistentVolumeClaims.CreateReturns(&v1.PersistentVolumeClaim{
						ObjectMeta: metav1.ObjectMeta{
							Name: "k8s-volume-claim",
						},
					}, nil)
				})

				It("should not error", func() {
					Expect(err).NotTo(HaveOccurred())
				})

				Context("when mode is not a boolean", func() {
					BeforeEach(func() {
						params["readonly"] = ""
						bindDetails.RawParameters, err = json.Marshal(params)
						Expect(err).NotTo(HaveOccurred())
					})

					It("errors", func() {
						Expect(err).To(Equal(brokerapi.ErrRawParamsInvalid))
					})
				})

				Context("when an identical binding already exists", func() {
					BeforeEach(func() {
						fakeStore.IsBindingConflictReturns(false)
					})

					It("doesn't error when binding the same details", func() {
						Expect(err).NotTo(HaveOccurred())
					})
				})

				Context("when the binding already exists with different details", func() {
					BeforeEach(func() {
						fakeStore.IsBindingConflictReturns(true)
					})

					It("errors", func() {
						Expect(err).To(Equal(brokerapi.ErrBindingAlreadyExists))
					})
				})

				Context("when it fails to create persistent volume claim", func() {
					var createErr error

					BeforeEach(func() {
						createErr = errors.New("failed-to-create")
						fakeK8sPersistentVolumeClaims.CreateReturns(nil, createErr)
					})

					It("returns an error", func() {
						Expect(err).To(Equal(createErr))
					})
				})

				It("creates a persistent volume claim", func() {
					Expect(fakeK8sPersistentVolumeClaims.CreateCallCount()).To(Equal(1), "PVC.Create not called")
					spec := fakeK8sPersistentVolumeClaims.CreateArgsForCall(0)
					Expect(spec).To(Equal(&v1.PersistentVolumeClaim{
						TypeMeta: metav1.TypeMeta{
							Kind:       "PersistentVolumeClaim",
							APIVersion: "v1",
						},
						ObjectMeta: metav1.ObjectMeta{
							Name: "k8s-volume",
						},

						Spec: v1.PersistentVolumeClaimSpec{
							AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteMany},
							Resources:   v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: quantity}},
							Selector: &metav1.LabelSelector{
								MatchExpressions: []metav1.LabelSelectorRequirement{
									{
										Key:      "name",
										Operator: metav1.LabelSelectorOpIn,
										Values:   []string{"k8s-volume"},
									},
								},
							},
						},
					}))
				})

				It("creates the binding detail", func() {
					Expect(fakeStore.CreateBindingDetailsCallCount()).To(Equal(1))
					id, details := fakeStore.CreateBindingDetailsArgsForCall(0)
					Expect(id).To(Equal("binding-id"))
					Expect(details).To(Equal(bindDetails))
				})

				It("includes empty credentials to prevent CAPI crash", func() {
					Expect(binding.Credentials).NotTo(BeNil())
				})

				It("uses the instance id in the default container path", func() {
					Expect(binding.VolumeMounts[0].ContainerDir).To(Equal("/var/vcap/data/some-instance-id"))
				})

				Context("when there is a mount path in the params", func() {
					BeforeEach(func() {
						params["mount"] = "/var/vcap/otherdir/something"
						bindDetails.RawParameters, err = json.Marshal(params)
						Expect(err).NotTo(HaveOccurred())
					})

					It("flows container path through", func() {
						Expect(binding.VolumeMounts[0].ContainerDir).To(Equal("/var/vcap/otherdir/something"))
					})
				})

				It("uses rw as its default mode", func() {
					Expect(binding.VolumeMounts[0].Mode).To(Equal("rw"))
				})

				It("fills in the driver name", func() {
					Expect(binding.VolumeMounts[0].Driver).To(Equal("csi"))
				})

				It("fills in the device type", func() {
					Expect(binding.VolumeMounts[0].DeviceType).To(Equal("shared"))
				})

				It("includes csi volume info in the service binding", func() {
					Expect(binding.VolumeMounts).To(HaveLen(1))
					Expect(binding.VolumeMounts[0].Device.VolumeId).To(Equal("some-instance-id-volume"))
					Expect(binding.VolumeMounts[0].Device.MountConfig).To(HaveKeyWithValue("name", "k8s-volume-claim"))
				})

				It("should write state", func() {
					Expect(fakeStore.SaveCallCount()).To(Equal(1))
				})

				Context("when the details are not provided", func() {
					BeforeEach(func() {
						bindDetails.RawParameters = nil
					})

					It("succeeds", func() {
						Expect(err).NotTo(HaveOccurred())
					})
				})

				Context("when the binding cannot be stored", func() {
					BeforeEach(func() {
						fakeStore.CreateBindingDetailsReturns(errors.New("badness"))
					})

					It("should error", func() {
						Expect(err).To(HaveOccurred())
					})
				})

				Context("when the save fails", func() {
					BeforeEach(func() {
						fakeStore.SaveReturns(errors.New("badness"))
					})

					It("should error", func() {
						Expect(err).To(HaveOccurred())
					})
				})
			})
		})

		Context(".Unbind", func() {
			var err error

			BeforeEach(func() {
				fingerprint := k8sbroker.ServiceFingerPrint{
					Name: "k8s-volume",
					Volume: &v1.PersistentVolume{
						TypeMeta: metav1.TypeMeta{
							Kind:       "PersistentVolume",
							APIVersion: "v1",
						},
						ObjectMeta: metav1.ObjectMeta{
							Name:   "k8s-volume",
							Labels: map[string]string{"name": "k8s-volume"},
						},
					},
				}

				// simulate untyped data loaded from a data file
				jsonFingerprint := &map[string]interface{}{}
				raw, err := json.Marshal(fingerprint)
				Expect(err).ToNot(HaveOccurred())
				err = json.Unmarshal(raw, jsonFingerprint)
				Expect(err).ToNot(HaveOccurred())

				fakeStore.RetrieveInstanceDetailsReturns(brokerstore.ServiceInstance{
					ServiceID:          "some-service-id",
					ServiceFingerPrint: jsonFingerprint,
				}, nil)
			})

			JustBeforeEach(func() {
				err = broker.Unbind(ctx, "some-instance-id", "binding-id", brokerapi.UnbindDetails{})
			})

			It("unbinds a bound service instance from an app", func() {
				Expect(err).NotTo(HaveOccurred())
			})

			It("deletes the persistent volume claim", func() {
				Expect(fakeK8sPersistentVolumeClaims.DeleteCallCount()).To(Equal(1))
				claimName, deleteOptions := fakeK8sPersistentVolumeClaims.DeleteArgsForCall(0)
				Expect(claimName).To(Equal("k8s-volume"))
				Expect(deleteOptions).To(Equal(&metav1.DeleteOptions{}))
			})

			It("should write state", func() {
				Expect(fakeStore.SaveCallCount()).To(Equal(1))
			})

			Context("when trying to unbind a instance that has not been provisioned", func() {
				BeforeEach(func() {
					fakeStore.RetrieveInstanceDetailsReturns(brokerstore.ServiceInstance{}, errors.New("Shazaam!"))
				})

				It("fails", func() {
					Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
				})
			})

			Context("when trying to unbind a binding that has not been bound", func() {
				BeforeEach(func() {
					fakeStore.RetrieveBindingDetailsReturns(brokerapi.BindDetails{}, errors.New("Hooray!"))
				})

				It("fails", func() {
					Expect(err).To(Equal(brokerapi.ErrBindingDoesNotExist))
				})
			})

			Context("when the save fails", func() {
				BeforeEach(func() {
					fakeStore.SaveReturns(errors.New("badness"))
				})

				It("should error", func() {
					Expect(err).To(HaveOccurred())
				})
			})

			Context("when deletion of the binding details fails", func() {
				BeforeEach(func() {
					fakeStore.DeleteBindingDetailsReturns(errors.New("badness"))
				})

				It("should error", func() {
					Expect(err).To(HaveOccurred())
				})
			})
		})
	})
})
