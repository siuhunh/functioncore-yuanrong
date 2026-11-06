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

// Package network network is modifying the Worker Network
package network

import (
	"bufio"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"io/ioutil"
	"math/big"
	"net"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"time"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	commontypes "functioncore/pkg/common/types"
)

const (
	ipv6Index = 1
	ipsLen    = 2
	routeLen  = 3
	// MaxRetryTimes -
	MaxRetryTimes = 4

	funcTunnelName     = "tunl_fgs_vpc"
	endpointTunnelName = "tunl_end_vpc"
	funcTunnelNameIPV6 = "tunnel0"

	multiplePatNumber = 2
	randomThreshold   = 100
	dividendNum       = 2

	apigRouteSuffix = "/32"
)

var (
	addRouteOnce        sync.Once
	nameserverLineRegex = regexp.MustCompile(`^nameserver\s+(?P<ip>[0-9.]+)\s*$`)

	gracefulShutdownCmdList = make([]*command, 0, 0)
)

type command struct {
	cmdline string
	// Indicates whether a failure can be performed, that is, whether the return value is concerned.
	// The default value is false, indicating that the failure cannot be performed.
	failurable bool
}

type cmdPair struct {
	del *command
	add *command
}

// NetWork used to modify a container network.
type NetWork struct {
	NATConfigures           []commontypes.NATConfigure
	EndpointVpcNATConfigure []commontypes.NATConfigure
	PublicVpcNATConfigure   []commontypes.NATConfigure
	CurrentNATConfigure     commontypes.NATConfigure
	RouteInfo               commontypes.RouteInfoData
	Replaceable             bool
	EndpointVpcReplaceable  bool
	PublicVpcReplaceable    bool
	IPV6Enable              bool
	VPCReservedCidr         string
	SvcCidr                 string // SvcCidr is cluster service network segment
	PingHash                string
	Ping6Hash               string
	IsSafety                bool
	SafeDNSAddr             string
	NatInitializeChan       chan struct{}
	CodeDeployChan          chan struct{}
	DNSConfig               commontypes.DNSConfig
}

// VpcGracefulShutdown -
func VpcGracefulShutdown() {
	log.GetLogger().Infof("clear the previously created vpc network resources")
	for _, cmdline := range gracefulShutdownCmdList {
		log.GetLogger().Debugf("cmd %s", cmdline.cmdline)
		if err := execCommandLine(cmdline); err != nil {
			log.GetLogger().Warnf("failed to exec cmd err: %s", err)
		}
	}
}

// NeedChange returns bool to determine whether to modify the network.
func (n *NetWork) NeedChange(configures []commontypes.NATConfigure) bool {
	if len(configures) <= 0 {
		log.GetLogger().Debugf("no network changes required")
		return false
	}
	if len(configures) == 1 {
		log.GetLogger().Debugf("network changes required and replace unable")
		return true
	}
	log.GetLogger().Info("network changes required and replaceable")
	return true
}

func execDeleteRouteCmd(delCmd *command) {
	if delCmd == nil {
		return
	}
	log.GetLogger().Debugf("exec change net work cmd: %s", delCmd.cmdline)
	err := execCommandLine(delCmd)
	if err != nil {
		log.GetLogger().Warnf("failed to delete route: %s", err.Error())
	}
}

func execAddRouteCmd(cmdList []*cmdPair) error {
	for _, cmd := range cmdList {
		if cmd.add == nil || cmd.del == nil {
			continue
		}
		// Run the add command. If the deletion fails, use the paired delete command.
		log.GetLogger().Infof("apig route add cmd: %s", cmd.add.cmdline)
		err := execCommandLine(cmd.add)
		if err != nil {
			execDeleteRouteCmd(cmd.del)
			err = execCommandLine(cmd.add)
			if err != nil {
				log.GetLogger().Infof("failed to exec change net work cmd: %s", cmd.add.cmdline)
				return fmt.Errorf("failed to exec cmd: %s", err)
			}
		}
	}
	return nil
}

func execChangeNetCmd(cmdList []*cmdPair) error {
	for index, cmd := range cmdList {
		if cmd.add == nil {
			continue
		}
		// Before adding a default route, delete the original default route.
		if index == len(cmdList)-1 && cmd.del != nil {
			execDeleteRouteCmd(cmd.del)
		}
		// Run the add command. If the deletion fails, use the paired delete command.
		log.GetLogger().Debugf("exec change net work cmd: %s", cmd.add.cmdline)
		err := execCommandLine(cmd.add)
		if err != nil {
			execDeleteRouteCmd(cmd.del)
			err = execCommandLine(cmd.add)
			if err != nil {
				return fmt.Errorf("failed to exec cmd: %s", err)
			}
		}
	}
	return nil
}

// closeTSO set tcp-segmentation-offload off
func closeTSO(tunnelName string) error {
	if tunnelName != funcTunnelName && tunnelName != endpointTunnelName {
		// nothing to do
		return nil
	}
	closeTSOCommandLine := "ethtool -K " + tunnelName + " tso off"
	_, err := exec.Command("sudo", "/bin/bash", "-c", closeTSOCommandLine).Output()
	if err != nil {
		log.GetLogger().Errorf("failed to close tso with '%s' and error %s", closeTSOCommandLine, err.Error())
		return err
	}
	log.GetLogger().Infof("succeed to close tso with exec command line '%s'", closeTSOCommandLine)
	return nil
}

func (n *NetWork) getNetWorkInfo(configNumber int, configures []commontypes.NATConfigure) (string, string, error) {
	if len(configures) < configNumber+1 {
		return "", "", fmt.Errorf("configNumber cause index outbounds")
	}
	patContainerIP := configures[configNumber].PatContainerIP
	ipv6PatContainerIP := configures[configNumber].IPV6PatContainerIP
	subMetaDigest := configures[configNumber].SubMetaDigest
	funcContainerIP := os.ExpandEnv("${POD_IP}")
	if err := n.checkPatContainerIP(patContainerIP, ipv6PatContainerIP, subMetaDigest); err != nil {
		return "", "", err
	}
	// VPC subnets to which the function is bound and subnets to which special provisioning is required .not nil
	natSubnetList := configures[configNumber].NatSubnetList
	if len(natSubnetList) == 0 {
		log.GetLogger().Errorf("failed to invalid nat subnet list(%+v).", natSubnetList)
		return "", "", fmt.Errorf("failed to invalid nat subnet list(%+v)", natSubnetList)
	}

	intfName, containerGateway, err := GetIntfnameAndGatewayByIPAddr(funcContainerIP)
	if err != nil {
		log.GetLogger().Errorf("failed to get interface and gateway of container ip '%s'", funcContainerIP)
		return "", "", fmt.Errorf("failed to get interface and gateway of container ip '%s'", funcContainerIP)
	}
	log.GetLogger().Infof("container ip: %s, interface name: %s, gateway: %s.", funcContainerIP, intfName,
		containerGateway)
	return intfName, containerGateway, nil
}

// Change Modifying the Pod Network
func (n *NetWork) Change(configNumber int, configures []commontypes.NATConfigure,
	tunnelName string, needEndpoint bool, getCodeFinishCh chan struct{}) error {
	if len(configures) < configNumber+1 {
		return fmt.Errorf("configNumber cause index outbounds")
	}
	patContainerIP := configures[configNumber].PatContainerIP
	vpcGatewayIP := configures[configNumber].PatGateway
	ipv6VpcGatewayIP := configures[configNumber].IPV6PatGateway
	var excludeSubnetList []string
	if l, ok := configures[configNumber].NatSubnetList["exclude"]; ok {
		excludeSubnetList = l
	}
	if err := n.initializePatContainer(patContainerIP, vpcGatewayIP); err != nil {
		return err
	}
	funcContainerIP := os.ExpandEnv("${POD_IP}")
	intfName, containerGateway, err := n.getNetWorkInfo(configNumber, configures)
	if err != nil {
		return fmt.Errorf("failed to get interface and gateway of container ip '%s', err: %s", funcContainerIP, err.Error())
	}
	log.GetLogger().Infof("container ip: %s, interface name: %s, gateway: %s.", funcContainerIP, intfName,
		containerGateway)
	if err = n.addRouteInfo(intfName, containerGateway); err != nil {
		return fmt.Errorf("failed to add route '%s'", err.Error())
	}
	cmdList := n.genCmdPairList(generateCommandListParam{configNumber: configNumber,
		funcContainerIP: funcContainerIP, intfName: intfName, containerGateway: containerGateway,
		excludeSubnetList: excludeSubnetList, configures: configures}, tunnelName, needEndpoint, getCodeFinishCh)
	if err = execChangeNetCmd(cmdList); err != nil {
		return err
	}
	if err = closeTSO(tunnelName); err != nil {
		return err
	}
	if err = n.checkConnectivity(vpcGatewayIP, "", false); err != nil {
		return fmt.Errorf("failed to connect to gateway IP %s, err: %s", vpcGatewayIP, err.Error())
	}
	if n.IPV6Enable {
		if err = n.checkConnectivity(ipv6VpcGatewayIP, "", true); err != nil {
			return fmt.Errorf("failed to connect to gateway IP %s, err: %s", ipv6VpcGatewayIP, err.Error())
		}
	}
	return nil
}

func (n *NetWork) initializePatContainer(patContainerIP, vpcGatewayIP string) error {
	log.GetLogger().Infof("initialize vpc proxy to pat container '%s'(tenant vpc gateway: %s).",
		patContainerIP, vpcGatewayIP)
	// Enable the iptables access policy for the PAT container.
	// Worker-init shields the network. Therefore, single-point deployment is required.
	err := execCommandLine(&command{cmdline: fmt.Sprintf("iptables -w "+
		"-I OUTPUT 1 -d %s -j ACCEPT", patContainerIP)})
	if err != nil {
		return fmt.Errorf("failed to open iptables rule for pat container ip '%s', err:%s", patContainerIP, err.Error())
	}
	return nil
}

// ChangePublicVpc Modifying the Pod public Network
func (n *NetWork) ChangePublicVpc(configNumber int, configures []commontypes.NATConfigure,
	getCodeFinishCh chan struct{}) error {
	if len(configures) < configNumber+1 {
		return fmt.Errorf("configNumber cause index outbounds")
	}
	vpcGatewayIP := configures[configNumber].PatGateway
	patContainerIP := configures[configNumber].PatContainerIP
	var excludeSubnetList []string
	if list, ok := configures[configNumber].NatSubnetList["exclude"]; ok {
		excludeSubnetList = list
	}

	log.GetLogger().Infof("initialize public vpc %s to pat container '%s'(tenant vpc gateway: %s).",
		configures[configNumber].PatPodName, patContainerIP, vpcGatewayIP)
	err := execCommandLine(&command{cmdline: fmt.Sprintf("iptables -w "+
		"-I OUTPUT 1 -d %s -j ACCEPT", patContainerIP)})
	if err != nil {
		log.GetLogger().Errorf("failed to open iptables rule for pat container ip '%s'", patContainerIP)
		return err
	}

	funcContainerIP := os.ExpandEnv("${POD_IP}")
	intfName, containerGateway, err := n.getNetWorkInfo(configNumber, configures)
	if err != nil {
		log.GetLogger().Errorf("failed to get interface and gateway of container ip '%s'", funcContainerIP)
		return fmt.Errorf("failed to get interface and gateway of container ip '%s'", funcContainerIP)
	}
	log.GetLogger().Infof("container ip: %s, interface name: %s, gateway: %s.", funcContainerIP, intfName,
		containerGateway)
	cmdList := n.genPublicCmdPairList(generateCommandListParam{configNumber: configNumber,
		funcContainerIP: funcContainerIP, intfName: intfName, containerGateway: containerGateway,
		excludeSubnetList: excludeSubnetList, configures: configures}, getCodeFinishCh)
	if err = execChangeNetCmd(cmdList); err != nil {
		return err
	}
	if err = n.checkConnectivity(patContainerIP, "", false); err != nil {
		log.GetLogger().Errorf("failed to connect to gateway IP %s", vpcGatewayIP)
		return err
	}
	return nil
}

func (n *NetWork) addRouteInfo(intfName, gateway string) error {
	var err error
	addRouteOnce.Do(func() {
		log.GetLogger().Infof("add route, ApigRouteEnable %t, intfName name: %s, gateway: %s, apigRoutes %v.",
			n.RouteInfo.ApigRouteEnable, intfName, gateway, n.RouteInfo.ApigRoutes)
		if n.RouteInfo.ApigRouteEnable {
			addRouteCmd := buildApigRouteCmd(n.RouteInfo, intfName, gateway)
			err = execAddRouteCmd(addRouteCmd)
			if err != nil {
				log.GetLogger().Errorf("failed to exec add route cmd, err: %s", err.Error())
				return
			}
			return
		}
	})
	if err != nil {
		log.GetLogger().Errorf("failed to add route, err: %s", err.Error())
		return fmt.Errorf("failed to add route '%s'", err.Error())
	}
	return nil
}

// Reset Modifying the Pod Network
func (n *NetWork) Reset(patContainerIP string) {
	err := execCommandLine(&command{cmdline: fmt.Sprintf("iptables -D OUTPUT 1")})
	if err != nil {
		log.GetLogger().Warnf("delete iptables rule for pat container ip '%s' failed.", patContainerIP)
		return
	}
	return
}

func (n *NetWork) checkPatContainerIP(patContainerIP string, ipv6PatContainerIP string, subMetaDigest string) error {
	if err := n.checkConnectivity(patContainerIP, subMetaDigest, false); err != nil {
		err = fmt.Errorf("failed to connect to pat container ip %s", patContainerIP)
		return err
	}
	if n.IPV6Enable {
		if err := n.checkConnectivity(ipv6PatContainerIP, "", true); err != nil {
			err = fmt.Errorf("failed to connect to ipv6 pat container ip %s", ipv6PatContainerIP)
			return err
		}
	}
	return nil
}

type generateCommandListParam struct {
	configNumber      int
	funcContainerIP   string
	intfName          string
	containerGateway  string
	excludeSubnetList []string
	configures        []commontypes.NATConfigure
}

func (n *NetWork) genPublicCmdPairList(param generateCommandListParam, getCodeFinishCh chan struct{}) []*cmdPair {
	if len(param.configures) < param.configNumber+1 {
		return nil
	}
	cmdList := make([]*cmdPair, 0, 0)

	addDefaultRouteCmd := &cmdPair{
		del: &command{cmdline: "ip route del default", failurable: true},
	}
	addDefaultRouteCmd.add = &command{cmdline: fmt.Sprintf("ip route add default via %s dev eth0 onlink",
		param.configures[param.configNumber].PatContainerIP)}
	if constants.GetNetworkModel() != constants.NetworkModelVPC {
		cmdList = buildDNSServerRouteCmd(cmdList, param.intfName, param.containerGateway)
	}
	cmdList = buildExcludeCmd(param.excludeSubnetList, cmdList, param.intfName, param.containerGateway)

	cmdList = append(cmdList, addDefaultRouteCmd)

	if constants.GetNetworkModel() == constants.NetworkModelVPC {
		cmdList = buildPodRouteForNetworkModelVPC(cmdList, param.intfName, param.containerGateway)
	}
	return cmdList
}

func (n *NetWork) genCmdPairList(param generateCommandListParam, tunnelName string, needEndpoint bool,
	getCodeFinishCh chan struct{}) []*cmdPair {
	if len(param.configures) < param.configNumber+1 {
		return nil
	}
	cmdList := make([]*cmdPair, 0, 0)

	cmdList = buildIPTunnelCmd(cmdList, tunnelName, param.funcContainerIP,
		param.configures[param.configNumber].PatContainerIP)
	if getCodeFinishCh != nil {
		select {
		case _, ok := <-getCodeFinishCh:
			if !ok {
				log.GetLogger().Debugf("no need to connect to cache-manager")
			}
		default:
			cmdList = buildConnectionToCacheManagerCmd(cmdList, param.containerGateway, param.intfName)
		}
	}

	cmdList = buildDNSServerRouteCmd(cmdList, param.intfName, param.containerGateway)
	cmdList = buildExcludeCmd(param.excludeSubnetList, cmdList, param.intfName, param.containerGateway)

	addDefaultRouteCmd, addDefaultIPV6RouteCmd, userDefineDNSRouteCmdList :=
		n.genRouteCmd(param, tunnelName, needEndpoint)

	cmdList = append(cmdList, addDefaultRouteCmd)

	if addDefaultIPV6RouteCmd != nil {
		cmdList = append(cmdList, addDefaultIPV6RouteCmd)
	}

	if len(userDefineDNSRouteCmdList) != 0 {
		cmdList = append(cmdList, userDefineDNSRouteCmdList...)
	}

	if constants.GetNetworkModel() == constants.NetworkModelVPC {
		cmdList = buildPodRouteForNetworkModelVPC(cmdList, param.intfName, param.containerGateway)
	}
	return cmdList
}

func (n *NetWork) genRouteCmd(param generateCommandListParam, tunnelName string,
	needEndpoint bool) (*cmdPair, *cmdPair, []*cmdPair) {
	addDefaultRouteCmd := &cmdPair{
		del: &command{cmdline: "ip route del default", failurable: true},
	}
	switch tunnelName {
	case funcTunnelName:
		if needEndpoint {
			addDefaultRouteCmd.add = &command{cmdline: fmt.Sprintf("ip route add %s via %s dev %s onlink",
				param.configures[param.configNumber].TenantCidr,
				param.configures[param.configNumber].PatContainerIP, tunnelName)}
		} else {
			addDefaultRouteCmd.add = &command{cmdline: fmt.Sprintf("ip route add default via %s dev %s onlink",
				param.configures[param.configNumber].PatContainerIP, tunnelName)}
		}
	case endpointTunnelName:
		addDefaultRouteCmd.add = &command{cmdline: fmt.Sprintf("ip route add default via %s dev %s onlink",
			param.configures[param.configNumber].PatContainerIP, tunnelName)}
	default: // nothing to do
	}
	userDefineDNSRouteCmdList := make([]*cmdPair, 0, 0)
	// add user define dns config
	if len(n.DNSConfig.DNS) != 0 {
		for _, ip := range n.DNSConfig.DNS {
			addUserDefineDNSConfigRouteCmd := &cmdPair{}
			addUserDefineDNSConfigRouteCmd.add =
				&command{cmdline: fmt.Sprintf("ip route add %s/32 via %s dev %s onlink",
					ip, param.configures[param.configNumber].PatContainerIP, tunnelName)}
			userDefineDNSRouteCmdList = append(userDefineDNSRouteCmdList, addUserDefineDNSConfigRouteCmd)
		}
	}
	if n.IPV6Enable {
		addDefaultIPV6RouteCmd := &cmdPair{}
		switch tunnelName {
		case funcTunnelName:
			addDefaultIPV6RouteCmd.add = &command{cmdline: fmt.Sprintf("ip -6 route add %s via %s dev eth0 onlink",
				param.configures[param.configNumber].IPV6TenantCidr,
				param.configures[param.configNumber].IPV6PatContainerIP)}
		default: // nothing to do
		}
		return addDefaultRouteCmd, addDefaultIPV6RouteCmd, userDefineDNSRouteCmdList
	}
	return addDefaultRouteCmd, nil, userDefineDNSRouteCmdList
}

func (n *NetWork) resetWorkerResolveConf() {
	filePath := "/etc/resolv.conf"
	// bak content
	originalContent, err := ioutil.ReadFile(filePath)
	if err != nil {
		log.GetLogger().Errorf("failed to read original resolve.conf: %s", err.Error())
		return
	}

	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		log.GetLogger().Errorf("failed to open resolv.conf: %s", err.Error())
		return
	}
	defer file.Close()

	var lines []string
	var nameServer = "nameserver"

	dnsAddr := strings.Split(strings.Trim(n.SafeDNSAddr, " "), ",")
	for _, addr := range dnsAddr {
		lines = append(lines, nameServer+" "+addr)
	}
	options := `options single-request-reopen ndots:5 timeout:2`
	lines = append(lines, options)

	file.Truncate(0)
	file.Seek(0, 0)
	writer := bufio.NewWriter(file)
	for _, line := range lines {
		_, err = fmt.Fprintln(writer, line)
		if err != nil {
			log.GetLogger().Errorf("failed to write new resolve address in resolv.conf: %s", err.Error())
			// if write new dns address error, rollback resolv.conf
			file.Truncate(0)
			file.Seek(0, 0)
			err = ioutil.WriteFile(filePath, originalContent, 0644)
			if err != nil {
				log.GetLogger().Errorf("rollback resolv.conf failed: %s", err.Error())
				return
			}
			log.GetLogger().Infof("rollback resolv.conf success")
			return
		}
	}
	writer.Flush()

	log.GetLogger().Infof("modified resolv.conf success, new address: %s", n.SafeDNSAddr)
	return
}

func buildPodRouteForNetworkModelVPC(cmdList []*cmdPair, intfName, gateway string) []*cmdPair {
	podCidr := os.Getenv("POD_CIDR")
	log.GetLogger().Infof("network model is vpc and current pod cidr is %s", podCidr)
	if podCidr == "" {
		log.GetLogger().Errorf("POD_CIDR is empty")
		return cmdList
	}
	cmdList = append(cmdList, &cmdPair{
		del: &command{cmdline: fmt.Sprintf("ip route del %s dev %s", podCidr, intfName), failurable: true},
		add: &command{cmdline: fmt.Sprintf("ip route add %s via %s dev %s", podCidr, gateway, intfName)},
	})
	return cmdList
}

func buildConnectionToCacheManagerCmd(cmdList []*cmdPair, containerGateway, intfName string) []*cmdPair {
	cacheManagerIP := os.Getenv("CACHE_MANAGER_SERVICE_HOST")
	if net.ParseIP(cacheManagerIP) == nil {
		log.GetLogger().Warnf("failed to get cacheManager IP")
		return cmdList
	}
	cmdList = append(cmdList, &cmdPair{
		add: &command{cmdline: fmt.Sprintf("ip route add %s via %s dev %s",
			cacheManagerIP, containerGateway, intfName)},
	})
	return cmdList
}

// CheckNATHealth check NAT health
func (n *NetWork) CheckNATHealth(ctx context.Context, vpcContainerIP string,
	gatewayIP string, subMetaDigest string) error {
	log.GetLogger().Infof("CheckNATHealth vpcContainerIP:%s, gatewayIP:%s, subMetaDigest:%s",
		vpcContainerIP, gatewayIP, subMetaDigest)
	if subMetaDigest == "" {
		healthChecker := NewPinger(gatewayIP, n.PingHash)
		return healthChecker.DoHealthCheck(ctx)
	}
	healthChecker := NewHTTPHealthChecker(vpcContainerIP, subMetaDigest)
	return healthChecker.DoHealthCheck(ctx)
}

func (n *NetWork) checkConnectivity(ip string, subMetaDigest string, ipv6 bool) error {
	log.GetLogger().Infof("checkConnectivity target ip:%s, subMetaDigest:%s", ip, subMetaDigest)
	start := time.Now()
	var err error
	if n.IPV6Enable && ipv6 {
		healthChecker := NewPinger(ip, n.Ping6Hash)
		err = healthChecker.Ping6Retry(1)
	} else {
		if subMetaDigest == "" {
			healthChecker := NewPinger(ip, n.PingHash)
			err = healthChecker.PingRetry(1)
		} else {
			healthChecker := NewHTTPHealthChecker(ip, subMetaDigest)
			err = healthChecker.doCheckWithRetry(1)
		}
	}
	if err != nil {
		log.GetLogger().Errorf("failed to do http health check '%s' with cost %v", ip, time.Since(start))
		return fmt.Errorf("failed to do http health check '%s'", ip)
	}
	log.GetLogger().Infof("succeed to do health check connectivity with cost %v", time.Since(start))
	return nil
}

// Derange random network config
func (n *NetWork) Derange() {
	randomValue, err := rand.Int(rand.Reader, big.NewInt(int64(randomThreshold)))
	if err != nil {
		log.GetLogger().Errorf("failed to generate random value %s", err.Error())
		return
	}
	if randomValue.Int64()%dividendNum == 0 {
		if len(n.NATConfigures) == multiplePatNumber {
			n.NATConfigures[0], n.NATConfigures[1] = n.NATConfigures[1], n.NATConfigures[0]
			log.GetLogger().Infof("derange nat conf success, now is %+v", n.NATConfigures)
		}
		if len(n.EndpointVpcNATConfigure) == multiplePatNumber {
			n.EndpointVpcNATConfigure[0], n.EndpointVpcNATConfigure[1] =
				n.EndpointVpcNATConfigure[1], n.EndpointVpcNATConfigure[0]
			log.GetLogger().Infof("derange endpoint vpc nat conf success, now is %+v", n.EndpointVpcNATConfigure)
		}
	} else {
		log.GetLogger().Infof("no need to derange network conf")
	}
}

func buildExcludeCmd(excludeSubnetList []string, cmdList []*cmdPair,
	intfName string, containerGateway string) []*cmdPair {
	// Traverse the routes that require packets to be transmitted from the container network
	// the default route will be changed in the following section
	for _, excludeSubnet := range excludeSubnetList {
		cmdList = append(cmdList, &cmdPair{
			del: &command{cmdline: fmt.Sprintf("ip route del %s dev %s", excludeSubnet, intfName), failurable: true},
			add: &command{cmdline: fmt.Sprintf("ip route add %s via %s dev %s",
				excludeSubnet, containerGateway, intfName)},
		})
	}
	return cmdList
}

func buildDNSServerRouteCmd(cmdList []*cmdPair, intfName string, containerGateway string) []*cmdPair {
	// Add a DNS server route.
	// Configure packets to be transmitted from the container network. The default route will be changed.
	nameserverList := getNameServerList()
	if len(nameserverList) > 0 {
		for _, nameserver := range nameserverList {
			cmdList = append(cmdList, &cmdPair{
				del: &command{cmdline: fmt.Sprintf("ip route del %s dev %s", nameserver, intfName), failurable: true},
				add: &command{cmdline: fmt.Sprintf("ip route add %s via %s dev %s",
					nameserver, containerGateway, intfName)},
			})
		}
	}
	return cmdList
}

func buildApigRouteCmd(apigRoutes commontypes.RouteInfoData, intfName string, containerGateway string) []*cmdPair {
	cmdList := make([]*cmdPair, 0, 0)
	if len(apigRoutes.ApigRoutes) > 0 && apigRoutes.ApigRouteEnable {
		for _, apigRoute := range apigRoutes.ApigRoutes {
			mergedRoute := apigRoute + apigRouteSuffix
			cmdList = append(cmdList, &cmdPair{
				del: &command{cmdline: fmt.Sprintf("ip route del %s dev %s", mergedRoute, intfName), failurable: true},
				add: &command{cmdline: fmt.Sprintf("ip route add %s via %s dev %s",
					mergedRoute, containerGateway, intfName)},
			})
		}
	}
	return cmdList
}

func buildIPTunnelCmd(cmdList []*cmdPair, tunnelName string,
	funcContainerIP string, patContainerIP string) []*cmdPair {
	cmdList = append(cmdList, &cmdPair{
		del: &command{cmdline: fmt.Sprintf("ip tunnel del %s", tunnelName), failurable: true},
		add: &command{cmdline: fmt.Sprintf(
			"ip tunnel add %s mode ipip local %s remote %s", tunnelName, funcContainerIP, patContainerIP)},
	})

	cmdList = append(cmdList, &cmdPair{
		add: &command{cmdline: fmt.Sprintf("ip link set %s up", tunnelName)},
	})
	return cmdList
}

func execCommandLine(command *command) error {
	startTime := time.Now()
	ret, result, err := ExecuteShellCommandLineWithCombinedOutput(command.cmdline)
	if err != nil {
		log.GetLogger().Errorf("failed to execute command line '%s' error.", command.cmdline)
		return err
	}
	if ret != 0 {
		log.GetLogger().Warnf("failed to execute command line '%s', ret=%d, result=%s.",
			command.cmdline, ret, result)
		if command.failurable {
			// if the command can fail, nil is returned.
			return nil
		}

		return fmt.Errorf("failed to execute command '%s' return non-zero value '%d', reslt: %s",
			command.cmdline, ret, result)
	}
	log.GetLogger().Infof("exeCommandOk | %s | %+v | result %s", command.cmdline, time.Since(startTime), result)
	return nil
}

// GetIntfnameAndGatewayByIPAddr -
func GetIntfnameAndGatewayByIPAddr(ip string) (string, string, error) {
	gateway := ""
	// Obtaining the NIC Name and Subnet Mask
	intfName, _, err := GetIntfNameAndNetmaskOfIp(ip)
	if err != nil {
		return intfName, gateway, err
	}

	ret, output, err := ExecuteShellCommandLineWithCombinedOutput("route -n")
	if err != nil {
		log.GetLogger().Errorf("failed to execute command line 'route -n'")
		return intfName, gateway, fmt.Errorf("failed to execute command line 'route -n' err '%s'", err.Error())
	}

	if ret != 0 {
		log.GetLogger().Errorf("failed to execute command line 'route -n' for getting return "+
			"value '%d'.", ret)
		return intfName, gateway,
			fmt.Errorf("failed to execute command line 'route -n' for getting return value '%d'", ret)
	}

	lines := strings.Split(output, "\n")
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) < routeLen {
			log.GetLogger().Errorf("failed to route table line '%s' invalid.", line)
			continue
		}

		if fields[len(fields)-1] != intfName {
			log.GetLogger().Warnf("route table line '%s' do not use interface '%s'.", line, intfName)
			continue
		}

		if constants.GetNetworkModel() == constants.NetworkModelVPC && fields[0] == "0.0.0.0" &&
			fields[1] != "0.0.0.0" {
			gateway = fields[1]
			break
		}

		if fields[0] == "0.0.0.0" || fields[1] == "0.0.0.0" {
			log.GetLogger().Warnf("route table line '%s' maybe default gateway.", line)
			continue
		}

		gateway = fields[1]
		break
	}

	if gateway == "" {
		log.GetLogger().Errorf("failed to get gateway of interface '%s'", intfName)
		return intfName, gateway, fmt.Errorf("failed to get gateway of interface '%s'", intfName)
	}

	return intfName, gateway, nil
}

func getNameServerList() []string {
	nameservers := make([]string, 0, 0)
	filePath := "/etc/resolv.conf"
	fd, err := os.Open(filePath)
	if err != nil {
		log.GetLogger().Errorf("failed to read resolve file '%s'", filePath)
		return nameservers
	}
	defer fd.Close()

	br := bufio.NewReader(fd)
	for {
		lineBytes, _, err := br.ReadLine()
		if err == io.EOF {
			break
		}

		line := string(lineBytes)
		matcheResult, _ := Match(nameserverLineRegex, line)
		if matcheResult == nil {
			continue
		}

		nameservers = append(nameservers, matcheResult.Group("ip"))
	}

	return nameservers
}

// ChangeNetworkToStreamHandler change the network between worker and streamhandler
func ChangeNetworkToStreamHandler(streamHandlerHostCidr string, streamHandlerServicePort string) error {
	err := execCommandLine(&command{cmdline: fmt.Sprintf("iptables -w "+
		"-I OUTPUT -p tcp -d %s --dport %s -j ACCEPT", streamHandlerHostCidr, streamHandlerServicePort)})
	if err != nil {
		log.GetLogger().Errorf("failed to open iptables rule for container ip '%s', port '%s'",
			streamHandlerHostCidr, streamHandlerServicePort)
		return err
	}
	return nil
}

// ChangeNetworkToDesPort change the network between worker and stream-handler
func ChangeNetworkToDesPort(port string) error {
	err := execCommandLine(&command{cmdline: fmt.Sprintf("iptables -I OUTPUT -p tcp -m tcp "+
		"--dport %s -j ACCEPT", port)})
	if err != nil {
		log.GetLogger().Errorf("failed to open iptables rule for container port '%s'", port)
		return err
	}
	return nil
}

// ChangeNetworkForSafeVpc -
func (n *NetWork) ChangeNetworkForSafeVpc() {
	if n.CodeDeployChan != nil {
		select {
		case _, ok := <-n.CodeDeployChan:
			if ok {
				log.GetLogger().Infof("goroutine code deploy finished, close channel")
				close(n.CodeDeployChan)
			}
		}
	}
	if n.NatInitializeChan != nil {
		select {
		case _, ok := <-n.NatInitializeChan:
			if ok {
				log.GetLogger().Infof("goroutine nat initialization finished, close channel")
				close(n.NatInitializeChan)
			}
		}
	}
	log.GetLogger().Infof("start to change network for safety vpc")
	// delete default dns setting
	cmdList := make([]*exec.Cmd, 0)
	// delete dns route from coredns
	for _, namespace := range getNameServerList() {
		cmdList = append(cmdList, exec.Command("ip", "route", "del", namespace))
	}
	// reset dns resolve.conf
	n.resetWorkerResolveConf()

	// set safety vpc dns resolve address with pat container ip as gateway, interface tunl_fgs_vpc
	for _, namespace := range getNameServerList() {
		cmdList = append(cmdList, exec.Command("ip", "route", "add", namespace,
			"via", n.CurrentNATConfigure.PatContainerIP, "dev", funcTunnelName, "onlink"))
	}

	// delete 100.64.0.0/10 exclude subnet
	var excludeSubnetList []string
	if l, ok := n.CurrentNATConfigure.NatSubnetList["exclude"]; ok {
		excludeSubnetList = l
	}
	for _, excludeSubnet := range excludeSubnetList {
		cmdList = append(cmdList, exec.Command("ip", "route", "del", excludeSubnet))
	}

	for _, cmd := range cmdList {
		if err := cmd.Run(); err != nil {
			log.GetLogger().Warnf("execute command: %s failed, error: %s",
				strings.Join(cmd.Args, " "), err.Error())
		}
	}
	log.GetLogger().Infof("change network for safe vpc succeed")
	return
}

// AddExcludeSubnetRouteToUploadSnapshot -
func (n *NetWork) AddExcludeSubnetRouteToUploadSnapshot() {
	if !n.IsSafety {
		return
	}
	funcContainerIP := os.ExpandEnv("${POD_IP}")
	intfName, containerGateway, err := GetIntfnameAndGatewayByIPAddr(funcContainerIP)
	if err != nil {
		log.GetLogger().Errorf("failed to get interface and gateway of container ip '%s'", funcContainerIP)
		return
	}
	var excludeSubnetList []string
	if l, ok := n.CurrentNATConfigure.NatSubnetList["exclude"]; ok {
		excludeSubnetList = l
	}
	cmdList := make([]*exec.Cmd, 0)
	for _, excludeSubnet := range excludeSubnetList {
		cmdList = append(cmdList, exec.Command("ip", "route", "add", excludeSubnet,
			"via", containerGateway, "dev", intfName))
	}
	for _, cmd := range cmdList {
		if err := cmd.Run(); err != nil {
			log.GetLogger().Warnf("execute command: %s failed, error: %s",
				strings.Join(cmd.Args, " "), err.Error())
		}
	}
	log.GetLogger().Infof("add exclude subnet route for uploading snapshot succeed")
}
