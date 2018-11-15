package k8sbroker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"path"

	"code.cloudfoundry.org/clock"
	"code.cloudfoundry.org/goshims/osshim"
	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/service-broker-store/brokerstore"
	csi "github.com/container-storage-interface/spec/lib/go/csi/v0"
	"github.com/golang/protobuf/jsonpb"
	uuid "github.com/nu7hatch/gouuid"
	"github.com/pivotal-cf/brokerapi"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
)

const (
	PermissionVolumeMount = brokerapi.RequiredPermission("volume_mount")
	DefaultContainerPath  = "/var/vcap/data"
)

var ErrEmptySpecFile = errors.New("At least one service must be provided in specfile")

type ErrInvalidService struct {
	Index int
}

func (e ErrInvalidService) Error() string {
	return fmt.Sprintf("Invalid service in specfile at index %d", e.Index)
}

type ErrInvalidSpecFile struct {
	err error
}

func (e ErrInvalidSpecFile) Error() string {
	return fmt.Sprintf("Invalid specfile %s", e.err.Error())
}

type ServiceFingerPrint struct {
	Name   string
	Volume *v1.PersistentVolume
}

type Service struct {
	DriverName string `json:"driver_name"`
	ConnAddr   string `json:"connection_address"`

	brokerapi.Service
}

type lock interface {
	Lock()
	Unlock()
}

type Broker struct {
	logger           lager.Logger
	os               osshim.Os
	mutex            lock
	clock            clock.Clock
	servicesRegistry ServicesRegistry
	store            brokerstore.Store
	client           kubernetes.Interface
	namespace        string
}

//go:generate counterfeiter -o k8sbroker_fake/fake_k8s_client.go . K8sClient
type K8sClient interface {
	kubernetes.Interface
}

//go:generate counterfeiter -o k8sbroker_fake/fake_k8s_core_v1.go . K8sCoreV1
type K8sCoreV1 interface {
	corev1.CoreV1Interface
}

//go:generate counterfeiter -o k8sbroker_fake/fake_k8s_persistent_volumes.go . K8sPersistentVolumes
type K8sPersistentVolumes interface {
	corev1.PersistentVolumeInterface
}

//go:generate counterfeiter -o k8sbroker_fake/fake_k8s_persistent_volume_claims.go . K8sPersistentVolumeClaims
type K8sPersistentVolumeClaims interface {
	corev1.PersistentVolumeClaimInterface
}

func New(
	logger lager.Logger,
	os osshim.Os,
	clock clock.Clock,
	store brokerstore.Store,
	client kubernetes.Interface,
	namespace string,
	servicesRegistry ServicesRegistry,
) (*Broker, error) {

	logger = logger.Session("new-csi-broker")
	logger.Info("start")
	defer logger.Info("end")

	theBroker := Broker{
		logger:           logger,
		os:               os,
		mutex:            &sync.Mutex{},
		clock:            clock,
		store:            store,
		client:           client,
		namespace:        namespace,
		servicesRegistry: servicesRegistry,
	}
	err := store.Restore(logger)
	if err != nil {
		return nil, err
	}

	return &theBroker, nil
}

func (b *Broker) Services(_ context.Context) []brokerapi.Service {
	logger := b.logger.Session("services")
	logger.Info("start")
	defer logger.Info("end")

	return b.servicesRegistry.BrokerServices()
}

func (b *Broker) Provision(context context.Context, instanceID string, details brokerapi.ProvisionDetails, asyncAllowed bool) (_ brokerapi.ProvisionedServiceSpec, e error) {
	logger := b.logger.Session("provision").WithData(lager.Data{"instanceID": instanceID, "details": details})
	logger.Info("start")
	defer logger.Info("end")

	var configuration csi.CreateVolumeRequest
	logger.Debug("provision-raw-parameters", lager.Data{"RawParameters": details.RawParameters})
	err := jsonpb.UnmarshalString(string(details.RawParameters), &configuration)
	if err != nil {
		logger.Error("provision-raw-parameters-decode-error", err)
		return brokerapi.ProvisionedServiceSpec{}, brokerapi.ErrRawParamsInvalid
	}

	if configuration.Name == "" {
		return brokerapi.ProvisionedServiceSpec{}, errors.New("config requires a \"name\"")
	}

	if configuration.GetCapacityRange() == nil {
		return brokerapi.ProvisionedServiceSpec{}, errors.New("config requires a \"capacity_range\"")
	}
	params := configuration.GetParameters()

	if _, ok := params["server"]; !ok {
		return brokerapi.ProvisionedServiceSpec{}, errors.New("config requires a \"server\"")
	}

	if _, ok := params["share"]; !ok {
		return brokerapi.ProvisionedServiceSpec{}, errors.New("config requires a \"share\"")
	}

	quantity, err := resource.ParseQuantity(strconv.FormatInt(configuration.GetCapacityRange().RequiredBytes, 10))
	if err != nil {
		logger.Error("failed-to-parse-quantity", err)
		return brokerapi.ProvisionedServiceSpec{}, err
	}

	volumeHandle, err := uuid.NewV4()
	if err != nil {
		logger.Error("failed-to-generate-volume-handle", err)
		return brokerapi.ProvisionedServiceSpec{}, err
	}

	driverName, err := b.servicesRegistry.DriverName(details.ServiceID)
	if err != nil {
		logger.Error("failed-to-retrieve-driver-name", err)
		return brokerapi.ProvisionedServiceSpec{}, err
	}

	volume, err := b.client.CoreV1().PersistentVolumes().Create(&v1.PersistentVolume{
		TypeMeta: metav1.TypeMeta{
			Kind:       "PersistentVolume",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:   configuration.Name,
			Labels: map[string]string{"name": configuration.Name},
		},

		Spec: v1.PersistentVolumeSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteMany},
			Capacity:    v1.ResourceList{v1.ResourceStorage: quantity},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					Driver:       driverName,
					VolumeHandle: volumeHandle.String(),
					VolumeAttributes: map[string]string{
						"server": params["server"],
						"share":  params["share"],
					},
				},
			},
		},
	})
	if err != nil {
		logger.Error("error-creating-persistent-volume", err)
		return brokerapi.ProvisionedServiceSpec{}, err
	}

	defer func() {
		if e != nil {
			err := b.deletePersistentVolume(configuration.Name)
			if err != nil {
				logger.Error("failed-to-cleanup-persistent-volume", err, lager.Data{"volume": volume})
			}
		}
	}()
	logger.Debug("created-volume", lager.Data{"volume": volume})

	b.mutex.Lock()
	defer b.mutex.Unlock()
	defer func() {
		out := b.store.Save(logger)
		if e == nil {
			e = out
		}
	}()

	fingerprint := ServiceFingerPrint{
		configuration.Name,
		volume,
	}
	instanceDetails := brokerstore.ServiceInstance{
		details.ServiceID,
		details.PlanID,
		details.OrganizationGUID,
		details.SpaceGUID,
		fingerprint,
	}

	if b.instanceConflicts(instanceDetails, instanceID) {
		return brokerapi.ProvisionedServiceSpec{}, brokerapi.ErrInstanceAlreadyExists
	}
	err = b.store.CreateInstanceDetails(instanceID, instanceDetails)
	if err != nil {
		return brokerapi.ProvisionedServiceSpec{}, fmt.Errorf("failed to store instance details %s", instanceID)
	}
	logger.Info("service-instance-created", lager.Data{"instanceDetails": instanceDetails})

	return brokerapi.ProvisionedServiceSpec{IsAsync: false}, nil
}

func (b *Broker) Deprovision(context context.Context, instanceID string, details brokerapi.DeprovisionDetails, asyncAllowed bool) (_ brokerapi.DeprovisionServiceSpec, e error) {
	logger := b.logger.Session("deprovision")
	logger.Info("start")
	defer logger.Info("end")

	var configuration csi.DeleteVolumeRequest

	if instanceID == "" {
		return brokerapi.DeprovisionServiceSpec{}, errors.New("volume deletion requires instance ID")
	}
	logger.Debug("instance-id", lager.Data{"id": instanceID})
	instanceDetails, err := b.store.RetrieveInstanceDetails(instanceID)
	if err != nil {
		return brokerapi.DeprovisionServiceSpec{}, brokerapi.ErrInstanceDoesNotExist
	}

	configuration.ControllerDeleteSecrets = map[string]string{}

	fingerprint, err := getFingerprint(instanceDetails.ServiceFingerPrint)
	if err != nil {
		return brokerapi.DeprovisionServiceSpec{}, err
	}

	err = b.deletePersistentVolume(fingerprint.Volume.Name)
	if err != nil {
		return brokerapi.DeprovisionServiceSpec{}, err
	}

	b.mutex.Lock()
	defer b.mutex.Unlock()
	defer func() {
		out := b.store.Save(logger)
		if e == nil {
			e = out
		}
	}()

	err = b.store.DeleteInstanceDetails(instanceID)
	if err != nil {
		return brokerapi.DeprovisionServiceSpec{}, err
	}

	return brokerapi.DeprovisionServiceSpec{IsAsync: false, OperationData: "deprovision"}, nil
}

func (b *Broker) Bind(context context.Context, instanceID string, bindingID string, bindDetails brokerapi.BindDetails) (_ brokerapi.Binding, e error) {
	logger := b.logger.Session("bind")
	logger.Info("start", lager.Data{"bindingID": bindingID, "details": bindDetails})
	defer logger.Info("end")

	b.mutex.Lock()
	defer b.mutex.Unlock()
	defer func() {
		out := b.store.Save(logger)
		if e == nil {
			e = out
		}
	}()

	logger.Info("starting-k8sbroker-bind")
	instanceDetails, err := b.store.RetrieveInstanceDetails(instanceID)
	if err != nil {
		return brokerapi.Binding{}, brokerapi.ErrInstanceDoesNotExist
	}
	logger.Info("retrieved-instance-details", lager.Data{"instanceDetails": instanceDetails})

	fingerprint, err := getFingerprint(instanceDetails.ServiceFingerPrint)
	if err != nil {
		return brokerapi.Binding{}, err
	}

	params := make(map[string]interface{})
	logger.Debug(fmt.Sprintf("bindDetails: %#v", bindDetails.RawParameters))

	if bindDetails.RawParameters != nil {
		err = json.Unmarshal(bindDetails.RawParameters, &params)
		if err != nil {
			return brokerapi.Binding{}, err
		}
	}

	if b.bindingConflicts(bindingID, bindDetails) {
		return brokerapi.Binding{}, brokerapi.ErrBindingAlreadyExists
	}

	cfMode, k8sMode, err := evaluateMode(params)
	if err != nil {
		logger.Error("failed-to-parse-quantity", err)
		return brokerapi.Binding{}, brokerapi.ErrRawParamsInvalid
	}

	volumeClaim, err := b.client.CoreV1().PersistentVolumeClaims(b.namespace).Create(&v1.PersistentVolumeClaim{
		TypeMeta: metav1.TypeMeta{
			Kind:       "PersistentVolumeClaim",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: fingerprint.Volume.Name,
		},

		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{k8sMode},
			Resources:   v1.ResourceRequirements{Requests: fingerprint.Volume.Spec.Capacity},
			Selector: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{
						Key:      "name",
						Operator: metav1.LabelSelectorOpIn,
						Values:   []string{fingerprint.Volume.Name},
					},
				},
			},
		},
	})
	if err != nil {
		logger.Error("error-creating-claim", err)
		return brokerapi.Binding{}, err
	}

	defer func() {
		if e != nil {
			err := b.deletePersistentVolumeClaim(fingerprint.Volume.Name)
			if err != nil {
				logger.Error("failed-to-cleanup-persistent-volume-claim", err, lager.Data{"volume-claim": volumeClaim})
			}
		}
	}()
	logger.Debug("created-volume-claim", lager.Data{"volume-claim": volumeClaim})

	err = b.store.CreateBindingDetails(bindingID, bindDetails)
	if err != nil {
		return brokerapi.Binding{}, err
	}

	volumeId := fmt.Sprintf("%s-volume", instanceID)

	ret := brokerapi.Binding{
		Credentials: struct{}{}, // if nil, cloud controller chokes on response
		VolumeMounts: []brokerapi.VolumeMount{{
			ContainerDir: evaluateContainerPath(params, instanceID),
			Mode:         cfMode,
			Driver:       "csi",
			DeviceType:   "shared",
			Device: brokerapi.SharedDevice{
				VolumeId: volumeId,
				MountConfig: map[string]interface{}{
					"name": volumeClaim.Name,
				},
			},
		}},
	}
	return ret, nil
}

func (b *Broker) Unbind(context context.Context, instanceID string, bindingID string, details brokerapi.UnbindDetails) (e error) {
	logger := b.logger.Session("unbind")
	logger.Info("start")
	defer logger.Info("end")

	b.mutex.Lock()
	defer b.mutex.Unlock()
	defer func() {
		out := b.store.Save(logger)
		if e == nil {
			e = out
		}
	}()

	var instanceDetails brokerstore.ServiceInstance
	var err error
	if instanceDetails, err = b.store.RetrieveInstanceDetails(instanceID); err != nil {
		return brokerapi.ErrInstanceDoesNotExist
	}

	if _, err := b.store.RetrieveBindingDetails(bindingID); err != nil {
		return brokerapi.ErrBindingDoesNotExist
	}

	fingerprint, err := getFingerprint(instanceDetails.ServiceFingerPrint)
	if err != nil {
		return err
	}

	err = b.deletePersistentVolumeClaim(fingerprint.Volume.Name)
	if err != nil {
		return err
	}

	if err := b.store.DeleteBindingDetails(bindingID); err != nil {
		return err
	}
	return nil
}

func (b *Broker) Update(context context.Context, instanceID string, details brokerapi.UpdateDetails, asyncAllowed bool) (brokerapi.UpdateServiceSpec, error) {
	panic("not implemented")
}

func (b *Broker) LastOperation(_ context.Context, instanceID string, operationData string) (brokerapi.LastOperation, error) {
	return brokerapi.LastOperation{}, nil
}

func (b *Broker) instanceConflicts(details brokerstore.ServiceInstance, instanceID string) bool {
	return b.store.IsInstanceConflict(instanceID, brokerstore.ServiceInstance(details))
}

func (b *Broker) bindingConflicts(bindingID string, details brokerapi.BindDetails) bool {
	return b.store.IsBindingConflict(bindingID, details)
}

func (b *Broker) deletePersistentVolume(volumeName string) error {
	return b.client.CoreV1().PersistentVolumes().Delete(volumeName, &metav1.DeleteOptions{
		TypeMeta: metav1.TypeMeta{
			Kind:       "PersistentVolume",
			APIVersion: "v1",
		},
	})
}

func (b *Broker) deletePersistentVolumeClaim(volumeClaimName string) error {
	return b.client.CoreV1().PersistentVolumeClaims(b.namespace).Delete(volumeClaimName, &metav1.DeleteOptions{})
}

func evaluateContainerPath(parameters map[string]interface{}, volId string) string {
	if containerPath, ok := parameters["mount"]; ok && containerPath != "" {
		return containerPath.(string)
	}

	return path.Join(DefaultContainerPath, volId)
}

func evaluateMode(parameters map[string]interface{}) (string, v1.PersistentVolumeAccessMode, error) {
	if ro, ok := parameters["readonly"]; ok {
		switch ro := ro.(type) {
		case bool:
			if ro {
				return "r", v1.ReadOnlyMany, nil
			}
			break
		default:
			return "", "", brokerapi.ErrRawParamsInvalid
		}
	}

	return "rw", v1.ReadWriteMany, nil
}

func getFingerprint(rawObject interface{}) (*ServiceFingerPrint, error) {
	fingerprint, ok := rawObject.(*ServiceFingerPrint)
	if ok {
		return fingerprint, nil
	}

	// casting didn't work--try marshalling and unmarshalling as the correct type
	rawJson, err := json.Marshal(rawObject)
	if err != nil {
		return nil, err
	}

	fingerprint = &ServiceFingerPrint{}
	err = json.Unmarshal(rawJson, fingerprint)
	if err != nil {
		return nil, err
	}

	return fingerprint, nil
}
