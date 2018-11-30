package main

import (
	"io"
	"net/http"
	"os/exec"
	"path/filepath"
	"strconv"

	"encoding/json"
	"io/ioutil"

	"fmt"

	"os"
	"time"

	"github.com/onsi/gomega/gbytes"
	"github.com/onsi/gomega/gexec"
	"github.com/pivotal-cf/brokerapi"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/ginkgomon"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type failRunner struct {
	Command           *exec.Cmd
	Name              string
	AnsiColorCode     string
	StartCheck        string
	StartCheckTimeout time.Duration
	Cleanup           func()
	session           *gexec.Session
	sessionReady      chan struct{}
}

func (r failRunner) Run(sigChan <-chan os.Signal, ready chan<- struct{}) error {
	defer GinkgoRecover()

	allOutput := gbytes.NewBuffer()

	debugWriter := gexec.NewPrefixedWriter(
		fmt.Sprintf("\x1b[32m[d]\x1b[%s[%s]\x1b[0m ", r.AnsiColorCode, r.Name),
		GinkgoWriter,
	)

	session, err := gexec.Start(
		r.Command,
		gexec.NewPrefixedWriter(
			fmt.Sprintf("\x1b[32m[o]\x1b[%s[%s]\x1b[0m ", r.AnsiColorCode, r.Name),
			io.MultiWriter(allOutput, GinkgoWriter),
		),
		gexec.NewPrefixedWriter(
			fmt.Sprintf("\x1b[91m[e]\x1b[%s[%s]\x1b[0m ", r.AnsiColorCode, r.Name),
			io.MultiWriter(allOutput, GinkgoWriter),
		),
	)

	Ω(err).ShouldNot(HaveOccurred())

	fmt.Fprintf(debugWriter, "spawned %s (pid: %d)\n", r.Command.Path, r.Command.Process.Pid)

	r.session = session
	if r.sessionReady != nil {
		close(r.sessionReady)
	}

	startCheckDuration := r.StartCheckTimeout
	if startCheckDuration == 0 {
		startCheckDuration = 5 * time.Second
	}

	var startCheckTimeout <-chan time.Time
	if r.StartCheck != "" {
		startCheckTimeout = time.After(startCheckDuration)
	}

	detectStartCheck := allOutput.Detect(r.StartCheck)

	for {
		select {
		case <-detectStartCheck: // works even with empty string
			allOutput.CancelDetects()
			startCheckTimeout = nil
			detectStartCheck = nil
			close(ready)

		case <-startCheckTimeout:
			// clean up hanging process
			session.Kill().Wait()

			// fail to start
			return fmt.Errorf(
				"did not see %s in command's output within %s. full output:\n\n%s",
				r.StartCheck,
				startCheckDuration,
				string(allOutput.Contents()),
			)

		case signal := <-sigChan:
			session.Signal(signal)

		case <-session.Exited:
			if r.Cleanup != nil {
				r.Cleanup()
			}

			Expect(string(allOutput.Contents())).To(ContainSubstring(r.StartCheck))
			Expect(session.ExitCode()).To(Not(Equal(0)), fmt.Sprintf("Expected process to exit with non-zero, got: 0"))
			return nil
		}
	}
}

var _ = Describe("k8sbroker Main", func() {
	Context("Missing required args", func() {
		var process ifrit.Process

		It("shows usage when dataDir or dbDriver are not provided", func() {
			var args []string
			volmanRunner := failRunner{
				Name:       "k8sbroker",
				Command:    exec.Command(binaryPath, args...),
				StartCheck: "Either dataDir, dbDriver or credhubURL parameters must be provided.",
			}
			process = ifrit.Invoke(volmanRunner)
		})

		It("shows usage when servicesConfig is not provided", func() {
			args := []string{"-dbDriver", "mysql"}
			volmanRunner := failRunner{
				Name:       "k8sbroker",
				Command:    exec.Command(binaryPath, args...),
				StartCheck: "servicesConfig parameter must be provided.",
			}
			process = ifrit.Invoke(volmanRunner)
		})

		AfterEach(func() {
			ginkgomon.Kill(process) // this is only if incorrect implementation leaves process running
		})
	})

	Context("Has required args", func() {
		var (
			args               []string
			listenAddr         string
			tempDir            string
			username, password string

			process ifrit.Process
		)

		BeforeEach(func() {
			listenAddr = "0.0.0.0:" + strconv.Itoa(8999+GinkgoParallelNode())
			username = "admin"
			password = "password"
			tempDir = os.TempDir()

			os.Setenv("USERNAME", username)
			os.Setenv("PASSWORD", password)

			d1 := []byte(`current-context: federal-context
apiVersion: v1
clusters:
- cluster:
    server: https://horse.org:4443
  name: horse-cluster
contexts:
- context:
    cluster: horse-cluster
    namespace: chisel-ns
    user: green-user
  name: federal-context
kind: Config
preferences:
  colors: true
users:
- name: blue-user
  user:
    token: blue-token`)

			kubeConfig := filepath.Join(tempDir, "kube-config.json")
			err := ioutil.WriteFile(kubeConfig, d1, 0644)
			Expect(err).NotTo(HaveOccurred())

			args = append(args, "-listenAddr", listenAddr)
			args = append(args, "-dataDir", tempDir)
			args = append(args, "-servicesConfig", "./default_services.json")
			args = append(args, "-kubeConfig", kubeConfig)
		})

		JustBeforeEach(func() {
			volmanRunner := ginkgomon.New(ginkgomon.Config{
				Name:       "k8sbroker",
				Command:    exec.Command(binaryPath, args...),
				StartCheck: "started",
			})
			process = ginkgomon.Invoke(volmanRunner)
		})

		AfterEach(func() {
			ginkgomon.Kill(process)
		})

		httpDoWithAuth := func(method, endpoint string, body io.ReadCloser) (*http.Response, error) {
			req, err := http.NewRequest(method, "http://"+listenAddr+endpoint, body)
			req.Header.Add("X-Broker-Api-Version", "2.14")
			Expect(err).NotTo(HaveOccurred())

			req.SetBasicAuth(username, password)
			return http.DefaultClient.Do(req)
		}

		It("should listen on the given address", func() {
			resp, err := httpDoWithAuth("GET", "/v2/catalog", nil)
			Expect(err).NotTo(HaveOccurred())

			Expect(resp.StatusCode).To(Equal(200))
		})

		It("should pass services config through to catalog", func() {
			resp, err := httpDoWithAuth("GET", "/v2/catalog", nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(200))

			bytes, err := ioutil.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())

			var catalog brokerapi.CatalogResponse
			err = json.Unmarshal(bytes, &catalog)
			Expect(err).NotTo(HaveOccurred())

			Expect(catalog.Services[0].ID).To(Equal("db404fc5-97fb-4806-9827-07e0e8d3bd51"))
			Expect(catalog.Services[0].Name).To(Equal("nfs"))
			Expect(catalog.Services[0].Plans[0].ID).To(Equal("190de554-4fc1-4008-ace9-5d3796140b48"))
			Expect(catalog.Services[0].Plans[0].Name).To(Equal("Existing"))
			Expect(catalog.Services[0].Plans[0].Description).To(Equal("A preexisting filesystem"))
		})
	})
})
