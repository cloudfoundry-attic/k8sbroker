package main

import (
	// "errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"

	"code.cloudfoundry.org/clock"
	"code.cloudfoundry.org/debugserver"
	"code.cloudfoundry.org/goshims/osshim"
	"code.cloudfoundry.org/k8sbroker/k8sbroker"
	"code.cloudfoundry.org/k8sbroker/utils"
	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/lager/lagerflags"

	"path/filepath"

	// "encoding/json"

	"code.cloudfoundry.org/service-broker-store/brokerstore"
	"github.com/go-sql-driver/mysql"
	"github.com/lib/pq"
	"github.com/pivotal-cf/brokerapi"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/grouper"
	"github.com/tedsuo/ifrit/http_server"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

var dataDir = flag.String(
	"dataDir",
	"",
	"[REQUIRED] - Broker's state will be stored here to persist across reboots",
)

var atAddress = flag.String(
	"listenAddr",
	"0.0.0.0:8999",
	"(optional) host:port to serve service broker API",
)

var servicesConfig = flag.String(
	"servicesConfig",
	"",
	"[REQUIRED] - Path to services config to register with cloud controller",
)

var dbDriver = flag.String(
	"dbDriver",
	"",
	"(optional) database driver name when using SQL to store broker state",
)

var dbHostname = flag.String(
	"dbHostname",
	"",
	"(optional) database hostname when using SQL to store broker state",
)
var dbPort = flag.String(
	"dbPort",
	"",
	"(optional) database port when using SQL to store broker state",
)

var dbName = flag.String(
	"dbName",
	"",
	"(optional) database name when using SQL to store broker state",
)

var dbCACertPath = flag.String(
	"dbCACertPath",
	"",
	"(optional) Path to CA Cert for database SSL connection",
)

var cfServiceName = flag.String(
	"cfServiceName",
	"",
	"(optional) For CF pushed apps, the service name in VCAP_SERVICES where we should find database credentials.  dbDriver must be defined if this option is set, but all other db parameters will be extracted from the service binding.",
)

var allowedOptions = flag.String(
	"allowedOptions",
	"auto_cache,uid,gid",
	"(optional) A comma separated list of parameters allowed to be set in config.",
)

var defaultOptions = flag.String(
	"defaultOptions",
	"auto_cache:true",
	"(optional) A comma separated list of defaults specified as param:value. If a parameter has a default value and is not in the allowed list, this default value becomes a fixed value that cannot be overridden",
)

var credhubURL = flag.String(
	"credhubURL",
	"",
	"(optional) CredHub server URL when using CredHub to store broker state",
)

var credhubCACertPath = flag.String(
	"credhubCACertPath",
	"",
	"(optional) Path to CA Cert for CredHub",
)

var uaaClientID = flag.String(
	"uaaClientID",
	"",
	"(optional) UAA client ID when using CredHub to store broker state",
)

var uaaClientSecret = flag.String(
	"uaaClientSecret",
	"",
	"(optional) UAA client secret when using CredHub to store broker state",
)

var uaaCACertPath = flag.String(
	"uaaCACertPath",
	"",
	"(optional) Path to CA Cert for UAA used for CredHub authorization",
)

var storeID = flag.String(
	"storeID",
	"k8sbroker",
	"(optional) Store ID used to namespace instance details and bindings (credhub only)",
)

var kubeConfig = flag.String(
	"kubeConfig",
	"",
	"[REQUIRED] Path to the kube config file",
)

var kubeNamespace = flag.String(
	"kubeNamespace",
	"opi",
	"(optional) Kubernetes namespace to create the PVCs in",
)

var (
	username   string
	password   string
	dbUsername string
	dbPassword string
)

func main() {
	parseCommandLine()
	parseEnvironment()

	checkParams()

	sink, err := lager.NewRedactingSink(
		lager.NewWriterSink(os.Stdout, lager.DEBUG),
		nil,
		nil,
	)

	if err != nil {
		panic(err)
	}

	logger, logSink := lagerflags.NewFromSink("k8sbroker", sink)
	logger.Info("starting")
	defer logger.Info("ends")

	server := createServer(logger)

	if dbgAddr := debugserver.DebugAddress(flag.CommandLine); dbgAddr != "" {
		server = utils.ProcessRunnerFor(grouper.Members{
			{"debug-server", debugserver.Runner(dbgAddr, logSink)},
			{"broker-api", server},
		})
	}

	process := ifrit.Invoke(server)
	logger.Info("started")
	utils.UntilTerminated(logger, process)
}

func parseCommandLine() {
	lagerflags.AddFlags(flag.CommandLine)
	debugserver.AddFlags(flag.CommandLine)
	flag.Parse()
}

func parseEnvironment() {
	username, _ = os.LookupEnv("USERNAME")
	password, _ = os.LookupEnv("PASSWORD")
	dbUsername, _ = os.LookupEnv("DB_USERNAME")
	dbPassword, _ = os.LookupEnv("DB_PASSWORD")
}

func checkParams() {
	if *dataDir == "" && *dbDriver == "" && *credhubURL == "" {
		fmt.Fprint(os.Stderr, "\nERROR: Either dataDir, dbDriver or credhubURL parameters must be provided.\n\n")
		flag.Usage()
		os.Exit(1)
	}

	if *servicesConfig == "" {
		fmt.Fprint(os.Stderr, "\nERROR: servicesConfig parameter must be provided.\n\n")
		flag.Usage()
		os.Exit(1)
	}
}

func getByAlias(data map[string]interface{}, keys ...string) interface{} {
	for _, key := range keys {
		value, ok := data[key]
		if ok {
			return value
		}
	}
	return nil
}

func createServer(logger lager.Logger) ifrit.Runner {
	fileName := filepath.Join(*dataDir, fmt.Sprintf("k8s-services.json"))

	var dbCACert string
	if *dbCACertPath != "" {
		b, err := ioutil.ReadFile(*dbCACertPath)
		if err != nil {
			logger.Fatal("cannot-read-db-ca-cert", err, lager.Data{"path": *dbCACertPath})
		}
		dbCACert = string(b)
	}

	var credhubCACert string
	if *credhubCACertPath != "" {
		b, err := ioutil.ReadFile(*credhubCACertPath)
		if err != nil {
			logger.Fatal("cannot-read-credhub-ca-cert", err, lager.Data{"path": *credhubCACertPath})
		}
		credhubCACert = string(b)
	}

	var uaaCACert string
	if *uaaCACertPath != "" {
		b, err := ioutil.ReadFile(*uaaCACertPath)
		if err != nil {
			logger.Fatal("cannot-read-credhub-ca-cert", err, lager.Data{"path": *uaaCACertPath})
		}
		uaaCACert = string(b)
	}

	store := brokerstore.NewStore(
		logger,
		*dbDriver,
		dbUsername,
		dbPassword,
		*dbHostname,
		*dbPort,
		*dbName,
		dbCACert,
		false,
		*credhubURL,
		credhubCACert,
		*uaaClientID,
		*uaaClientSecret,
		uaaCACert,
		fileName,
		*storeID,
	)

	services, err := k8sbroker.NewServicesFromConfig(*servicesConfig)
	if err != nil {
		logger.Fatal("loading-services-config-error", err)
	}

	logger.Info(fmt.Sprintf("Using kubeconfig %s", *kubeConfig))
	kubeConfigForClient, err := clientcmd.BuildConfigFromFlags("", *kubeConfig)
	if err != nil {
		logger.Error("failed-to-create-kube-config", err)
		os.Exit(1)
	}

	kubeClient, err := kubernetes.NewForConfig(kubeConfigForClient)
	if err != nil {
		logger.Error("failed-to-create-kube-client", err)
		os.Exit(1)
	}

	serviceBroker, err := k8sbroker.New(
		logger,
		&osshim.OsShim{},
		clock.NewClock(),
		store,
		kubeClient,
		*kubeNamespace,
		services,
	)
	if err != nil {
		logger.Fatal("creating-k8s-broker-error", err)
	}

	credentials := brokerapi.BrokerCredentials{Username: username, Password: password}
	handler := brokerapi.New(serviceBroker, logger.Session("broker-api"), credentials)

	return http_server.New(*atAddress, handler)
}

func ConvertPostgresError(err *pq.Error) string {
	return ""
}

func ConvertMySqlError(err mysql.MySQLError) string {
	return ""
}
