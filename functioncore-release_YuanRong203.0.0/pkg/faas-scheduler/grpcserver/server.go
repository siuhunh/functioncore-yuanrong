/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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

// Package grpcserver contains grpc server logic
package grpcserver

import (
	"fmt"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/grpc/pb"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/faas-scheduler/config"
)

// Server network server
type Server struct {
	grpcServer *grpc.Server
	config     Config
}

// Config network server config
type Config struct {
	Service schedulerapi.Scheduler
	IP      string
	Port    string
}

// NewServer -
func NewServer(config Config) (*Server, error) {
	grpcServer := grpc.NewServer(grpc.Creds(getCredentials()))
	service := &serverAdapter{service: config.Service}
	pb.RegisterSchedulerRPCServer(grpcServer, service)
	return &Server{grpcServer: grpcServer, config: config}, nil
}

// Start - server start listen
func (s *Server) Start() error {
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%s", s.config.IP, s.config.Port))
	if err != nil {
		log.GetLogger().Errorf("failed to listen tcp addr:%s:%s, err:%s", s.config.IP, s.config.Port, err.Error())
		return err
	}
	if err = s.grpcServer.Serve(lis); err != nil {
		log.GetLogger().Errorf("failed to start grpc Serve addr:%s:%s, err:%s",
			s.config.IP, s.config.Port, err.Error())
		return err
	}
	return nil
}

// Stop - server stop listen
func (s *Server) Stop() error {
	s.grpcServer.GracefulStop()
	return nil
}

func getCredentials() credentials.TransportCredentials {
	if config.FConfig.HTTPSConfig.HTTPSEnable {
		return credentials.NewTLS(tls.GetClientTLSConfig())
	}

	return insecure.NewCredentials()

}
