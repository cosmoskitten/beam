// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package jobopts contains shared options for job submission. These options
// are exposed to allow user code to inspect and modify them.
package jobopts

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"sync/atomic"

	"github.com/apache/beam/sdks/go/pkg/beam/log"
)

var (
	// Endpoint is the job service endpoint.
	Endpoint = flag.String("endpoint", "", "Job service endpoint (required).")

	// JobName is the name of the job.
	JobName = flag.String("job_name", "", "Job name (optional).")

	// EnvironmentType is the environmet type to run the user code.
	EnvironmentType = flag.String("go_environment_type", "DOCKER",
		"Environment Type. Possible options are DOCKER and PROCESS.")

	// EnvironmentType is the environmet type to run the user code.
	EnvironmentConfig = flag.String("go_environment_config",
		"",
		"Set environment configuration for running the user code.\n"+
			"For DOCKER: Url for the docker image.\n" +
			"For PROCESS: json of the form {\"os\": \"<OS>\", "+
			"\"arch\": \"<ARCHITECTURE>\", \"command\": \"<process to execute>\", "+
			"\"env\":{\"<Environment variables 1>\": \"<ENV_VAL>\"} }. "+
			"All fields in the json are optional except command.")

	// WorkerBinary is the location of the compiled worker binary. If not
	// specified, the binary is produced via go build.
	WorkerBinary = flag.String("worker_binary", "", "Worker binary (optional)")

	// Experiments toggle experimental features in the runner.
	Experiments = flag.String("experiments", "", "Comma-separated list of experiments (optional).")

	// Async determines whether to wait for job completion.
	Async = flag.Bool("async", false, "Do not wait for job completion.")
)

// GetEndpoint returns the endpoint, if non empty and exits otherwise. Runners
// such as Dataflow set a reasonable default. Convenience function.
func GetEndpoint() (string, error) {
	if *Endpoint == "" {
		return "", fmt.Errorf("no job service endpoint specified. Use --endpoint=<endpoint>")
	}
	return *Endpoint, nil
}

var unique int32

// GetJobName returns the specified job name or, if not present, a fresh
// autogenerated name. Convenience function.
func GetJobName() string {
	if *JobName == "" {
		id := atomic.AddInt32(&unique, 1)
		return fmt.Sprintf("go-job-%v-%v", id, time.Now().UnixNano())
	}
	return *JobName
}

// GetEnvironmentUrn returns the specified EnvironmentUrn used to run the SDK Harness,
// if not present, returns the docker environment urn "beam:env:docker:v1".
// Convenience function.
func GetEnvironmentUrn(ctx context.Context) string {
	switch env := strings.ToLower(*EnvironmentType); env {
	case "process":
		return "beam:env:process:v1"
	case "docker":
		return "beam:env:docker:v1"
	default:
		log.Infof(ctx, "No environment type specified. Using default environment: '%v'", *EnvironmentType)
		return "beam:env:docker:v1"
	}
}

// GetEnvironmentConfig returns the specified configuration for specified SDK Harness,
// if not present, the default development container for the current user.
// Convenience function.
func GetEnvironmentConfig(ctx context.Context) string {
	if *EnvironmentConfig == "" {
		*EnvironmentConfig = os.ExpandEnv("$USER-docker-apache.bintray.io/beam/go:latest")
		log.Infof(ctx, "No environment config specified. Using default config: '%v'", *EnvironmentConfig)
	}
	return *EnvironmentConfig
}

// GetExperiments returns the experiments.
func GetExperiments() []string {
	if *Experiments == "" {
		return nil
	}
	return strings.Split(*Experiments, ",")
}
