/*
 * Copyright 2018 The CovenantSQL Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package utils

import (
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"

	"github.com/CovenantSQL/CovenantSQL/utils/log"
)

// FJ is short for filepath.Join
var FJ = filepath.Join

// GetProjectSrcDir gets the src code root
func GetProjectSrcDir() string {
	_, testFile, _, _ := runtime.Caller(0)
	return FJ(filepath.Dir(testFile), "../")
}

// Build runs build.sh
func Build() (err error) {
	wd := GetProjectSrcDir()
	err = os.Chdir(wd)
	if err != nil {
		log.Errorf("change working dir failed: %s", err)
		return
	}
	cmd := exec.Command("./build.sh")
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Errorf("build failed: %s", err)
	}
	log.Debugf("build output info: %s", string(output))
	return
}

// RunCommand runs a command and capture its output to a log file,
//  if toStd is true also output to stdout and stderr
func RunCommand(bin string, args []string, processName string, workingDir string, logDir string, toStd bool) (err error) {
	cmd, err := RunCommandNB(bin, args, processName, workingDir, logDir, toStd)
	if err != nil {
		log.Errorf("start command failed: %v", err)
		return
	}
	err = cmd.Wait()
	if err != nil {
		log.Errorf("cmd %s args %s failed with %v", cmd.Path, cmd.Args, err)
		return
	}
	return
}

// RunCommandNB starts a non-blocking command
func RunCommandNB(bin string, args []string, processName string, workingDir string, logDir string, toStd bool) (cmd *exec.Cmd, err error) {
	logFD, err := os.Create(FJ(logDir, processName+".log"))
	if err != nil {
		log.Errorf("create log file failed: %s", err)
		return
	}

	err = os.Chdir(workingDir)
	if err != nil {
		log.Errorf("change working dir failed: %s", err)
		return
	}
	cmd = exec.Command(bin, args...)
	stdoutIn, _ := cmd.StdoutPipe()
	stderrIn, _ := cmd.StderrPipe()

	var stdout, stderr io.Writer
	if toStd {
		stdout = io.MultiWriter(os.Stdout, logFD)
		stderr = io.MultiWriter(os.Stderr, logFD)
	} else {
		stdout = logFD
		stderr = logFD
	}

	err = cmd.Start()
	if err != nil {
		log.Errorf("cmd.Start() failed with '%v'", err)
		return
	}

	go func() {
		_, err := io.Copy(stdout, stdoutIn)
		if err != nil {
			log.Errorf("failed to capture stdout %s", err)
			return
		}
	}()

	go func() {
		_, err := io.Copy(stderr, stderrIn)
		if err != nil {
			log.Errorf("failed to capture stderr %s", err)
			return
		}
	}()

	return
}
