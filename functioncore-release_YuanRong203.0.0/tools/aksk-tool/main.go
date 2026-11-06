/*
 * Copyright (c) 2021 Huawei Technologies Co., Ltd
 *
 * This software is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"functioncore/pkg/common/localauth"
)

type result struct {
	Authorization  string `json:"authorization"`
	XTimestampAuth string `json:"x-timestamp-auth"`
}

var defaultPort = int64(8000)

func main() {
	server := false
	port := defaultPort
	cmd := &cobra.Command{
		Use: fmt.Sprintf("%s", "auth"),
		Run: func(cmd *cobra.Command, args []string) {
			config, err := ioutil.ReadFile("./config.yaml")
			if err != nil {
				fmt.Println(err)
				return
			}
			var auth = localauth.AuthConfig{}
			if err = yaml.Unmarshal(config, &auth); err != nil {
				fmt.Println(err)
				return
			}
			if server {
				if startServer(auth, port) {
					return
				}
			}
			authorization, s := localauth.CreateAuthorization(auth.AKey, auth.SKey, "", "auth-cli", nil)
			fmt.Printf("authorization:%s\n", authorization)
			fmt.Printf("X-Timestamp-Auth:%s\n", s)
		},
	}
	cmd.Flags().BoolVarP(&server, "server", "s", false, "Enabling the HTTP Service")
	cmd.Flags().Int64VarP(&port, "port", "p", defaultPort, "HTTP port")
	cmd.AddCommand()
	if err := cmd.Execute(); err != nil {
		_ = fmt.Errorf("exec cmd error:%s\n", err)
	}
}

func startServer(auth localauth.AuthConfig, port int64) bool {
	http.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		authorization, s := localauth.CreateAuthorization(auth.AKey, auth.SKey, "", "auth-cli", nil)
		r := &result{
			Authorization:  authorization,
			XTimestampAuth: s,
		}
		result := make(map[string]string, 2)
		result["authorization"] = authorization
		result["X-Timestamp-Auth"] = s
		writer.WriteHeader(http.StatusOK)
		bytes, err := json.Marshal(r)
		if err != nil {
			fmt.Printf("%s", err)
		}
		_, err = writer.Write(bytes)
		if err != nil {
			fmt.Printf("%s", err)
			return
		}
	})
	if err := http.ListenAndServe(":"+fmt.Sprintf("%d", port), nil); err != nil {
		fmt.Printf("%s", err)
		return true
	}
	return false
}
