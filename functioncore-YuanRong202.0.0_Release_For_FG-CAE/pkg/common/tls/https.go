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

package tls

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"

	fhttp "github.com/valyala/fasthttp"

	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/utils"
)

const urlIndex = 1

// HTTPSConfig is for needed HTTPS config
type HTTPSConfig struct {
	CipherSuite   []uint16
	MinVers       uint16
	MaxVers       uint16
	CACertFile    string
	CertFile      string
	SecretKeyFile string
	PwdFilePath   string
	KeyPassPhase  string
}

// InternalHTTPSConfig is for input config
type InternalHTTPSConfig struct {
	HTTPSEnable bool   `json:"httpsEnable" yaml:"httpsEnable" valid:"optional"`
	TLSProtocol string `json:"tlsProtocol" yaml:"tlsProtocol" valid:"optional"`
	TLSCiphers  string `json:"tlsCiphers" yaml:"tlsCiphers" valid:"optional"`
}

var (
	// HTTPSConfigs is a global variable of HTTPS config
	HTTPSConfigs = &HTTPSConfig{}
	// tlsConfig is a global variable of TLS config
	tlsConfig *tls.Config
	once      sync.Once

	// TLS_VERSION_MAP is a set of TLS versions
	TLS_VERSION_MAP = map[string]uint16{
		"TLSv1.2": tls.VersionTLS12,
	}
)

// TLS_CIPHER_SUITE_MAP is a set of supported TLS algorithms
var TLS_CIPHER_SUITE_MAP = map[string]uint16{
	"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256":   tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
	"TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384":   tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
	"TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256": tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
	"TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384": tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
}

// GetURLScheme returns "http" or "https"
func GetURLScheme(https bool) string {
	if https {
		return "https"
	}
	return "http"
}

// HTTPListenAndServeTLS listens and serves by TLS in HTTP
func HTTPListenAndServeTLS(addr string, handler http.Handler) error {
	server := &http.Server{
		Addr:      addr,
		Handler:   handler,
		TLSConfig: tlsConfig,
	}
	return server.ListenAndServeTLS("", "")
}

// FastHTTPListenAndServeTLS listens and serves by TLS in FastHTTP
func FastHTTPListenAndServeTLS(addr string, server *fhttp.Server) error {
	listener, err := net.Listen("tcp4", addr)
	if err != nil {
		return err
	}

	tlsListener := tls.NewListener(listener, tlsConfig)

	if err = server.Serve(tlsListener); err != nil {
		return err
	}
	return nil
}

// GetClientTLSConfig returns the config of TLS
func GetClientTLSConfig() *tls.Config {
	if tlsConfig == nil {
		return nil
	}
	certs := make([]tls.Certificate, len(tlsConfig.Certificates))
	copy(certs, tlsConfig.Certificates)
	suits := make([]uint16, len(tlsConfig.CipherSuites))
	copy(suits, tlsConfig.CipherSuites)
	newCfg := &tls.Config{
		ClientCAs:                tlsConfig.ClientCAs,
		Certificates:             certs,
		CipherSuites:             suits,
		PreferServerCipherSuites: tlsConfig.PreferServerCipherSuites,
		ClientAuth:               tlsConfig.ClientAuth,
		InsecureSkipVerify:       tlsConfig.InsecureSkipVerify,
		MinVersion:               tlsConfig.MinVersion,
		MaxVersion:               tlsConfig.MaxVersion,
		Renegotiation:            tlsConfig.Renegotiation,
	}
	return newCfg
}

func loadCerts(path string) string {
	env := os.Getenv("SSL_ROOT")
	if len(env) == 0 {
		log.GetLogger().Errorf("failed to get SSL_ROOT")
		return ""
	}
	certPath, err := filepath.Abs(filepath.Join(env, path))
	if err != nil {
		log.GetLogger().Errorf("failed to return an absolute representation of path: %s", path)
		return ""
	}
	ok := utils.FileExists(certPath)
	if !ok {
		log.GetLogger().Errorf("failed to load the cert file: %s", certPath)
		return ""
	}
	return certPath
}

func loadTLSConfig() error {
	clientAuthMode := tls.NoClientCert
	var pool *x509.CertPool

	pool, err := GetX509CACertPool(HTTPSConfigs.CACertFile)
	if err != nil {
		log.GetLogger().Errorf("failed to GetX509CACertPool: %s", err.Error())
		return err
	}

	var certs []tls.Certificate
	certs, err = LoadServerTLSCertificate(HTTPSConfigs.CertFile, HTTPSConfigs.SecretKeyFile,
		HTTPSConfigs.KeyPassPhase, true)
	if err != nil {
		log.GetLogger().Errorf("failed to loadServerTLSCertificate: %s", err.Error())
		return err
	}

	tlsConfig = &tls.Config{
		ClientCAs:                pool,
		Certificates:             certs,
		CipherSuites:             HTTPSConfigs.CipherSuite,
		PreferServerCipherSuites: true,
		ClientAuth:               clientAuthMode,
		InsecureSkipVerify:       true,
		MinVersion:               HTTPSConfigs.MinVers,
		MaxVersion:               HTTPSConfigs.MaxVers,
		Renegotiation:            tls.RenegotiateNever,
	}

	return nil
}

// loadHTTPSConfig loads the protocol and ciphers of TLS
func loadHTTPSConfig(tlsProtocols string, tlsCiphers string) error {
	HTTPSConfigs = &HTTPSConfig{
		MinVers:       tls.VersionTLS12,
		MaxVers:       tls.VersionTLS12,
		CipherSuite:   nil,
		CACertFile:    loadCerts("trust.cer"),
		CertFile:      loadCerts("server.cer"),
		SecretKeyFile: loadCerts("server_key.pem"),
		PwdFilePath:   loadCerts("cert_pwd"),
		KeyPassPhase:  "",
	}

	minVersion := parseSSLProtocol(tlsProtocols)
	if HTTPSConfigs.MinVers == 0 {
		return errors.New("invalid TLS protocol")
	}
	if minVersion == 0 {
		minVersion = tls.VersionTLS12
	}
	HTTPSConfigs.MinVers = minVersion
	cipherSuites := parseSSLCipherSuites(tlsCiphers)
	if len(cipherSuites) == 0 {
		return errors.New("invalid TLS ciphers")
	}
	HTTPSConfigs.CipherSuite = cipherSuites

	keyPassPhase, err := ioutil.ReadFile(HTTPSConfigs.PwdFilePath)
	if err != nil {
		log.GetLogger().Errorf("failed to read file cert_pwd: %s", err.Error())
		return err
	}
	HTTPSConfigs.KeyPassPhase = string(keyPassPhase)
	utils.ClearByteMemory(keyPassPhase)

	return nil
}

// InitTLSConfig inits config of HTTPS
func InitTLSConfig(tlsProtocols string, tlsCiphers string) (err error) {
	once.Do(func() {
		err = loadHTTPSConfig(tlsProtocols, tlsCiphers)
		if err != nil {
			err = errors.New("failed to load HTTPS config")
			return
		}
		err = loadTLSConfig()
		if err != nil {
			return
		}
	})
	return err
}

// GetX509CACertPool generates CACertPool by CA certificate
func GetX509CACertPool(caCertFilePath string) (*x509.CertPool, error) {
	pool := x509.NewCertPool()
	caCertContent, err := loadCACertBytes(caCertFilePath)
	if err != nil {
		return nil, err
	}

	pool.AppendCertsFromPEM(caCertContent)
	return pool, nil

}

// LoadServerTLSCertificate generates tls certificate by certfile and keyfile
func LoadServerTLSCertificate(certFile, keyFile string, passPhase string, isHttps bool) ([]tls.Certificate, error) {
	certContent, keyContent, err := loadCertAndKeyBytes(certFile, keyFile, passPhase, isHttps)
	utils.ClearStringMemory(passPhase)
	utils.ClearStringMemory(HTTPSConfigs.KeyPassPhase)
	if err != nil {
		utils.ClearByteMemory(certContent)
		utils.ClearByteMemory(keyContent)
		return nil, err
	}

	cert, err := tls.X509KeyPair(certContent, keyContent)
	utils.ClearByteMemory(certContent)
	utils.ClearByteMemory(keyContent)
	if err != nil {
		log.GetLogger().Errorf("failed to load the X509 key pair from cert file with key file: %s",
			err.Error())
		return nil, err
	}
	var certs []tls.Certificate
	certs = append(certs, cert)
	return certs, nil
}

func containPassPhase(keyContent []byte, passPhase string, isHttps bool) (Content []byte, err error) {
	if !isHttps {
		plainkeyContent, err := localauth.Decrypt(string(keyContent))
		if err != nil {
			log.GetLogger().Errorf("failed to decrypt keyContent: %s", err.Error())
			return nil, err
		}
		return plainkeyContent, nil
	}

	keyBlock, _ := pem.Decode(keyContent)
	if keyBlock == nil {
		log.GetLogger().Errorf("failed to decode key file ")
		return nil, errors.New("failed to decode key file")
	}

	if x509.IsEncryptedPEMBlock(keyBlock) {
		var plainPassPhase []byte
		if len(passPhase) > 0 {
			plainPassPhase, err = localauth.Decrypt(passPhase)
			if err != nil {
				log.GetLogger().Errorf("failed to decrypt the ssl passPhase(%d): %s", len(passPhase),
					err.Error())
				return nil, err
			}
		}

		keyData, err := x509.DecryptPEMBlock(keyBlock, plainPassPhase)
		clearByteMemory(plainPassPhase)
		if err != nil {
			log.GetLogger().Errorf("failed to decrypt key file, error: %s", err.Error())
			return nil, err
		}

		// The decryption is successful, then the file is re-encoded to a PEM file
		plainKeyBlock := &pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: keyData,
		}

		keyContent = pem.EncodeToMemory(plainKeyBlock)
	}
	return keyContent, nil

}

func loadCertAndKeyBytes(certFilePath, keyFilePath, passPhase string, isHttps bool) (
	certPEMBlock, keyPEMBlock []byte, err error) {
	certContent, err := ioutil.ReadFile(certFilePath)
	if err != nil {
		log.GetLogger().Errorf("failed to read cert file %s: %s", certFilePath, err.Error())
		return nil, nil, err
	}

	keyContent, err := ioutil.ReadFile(keyFilePath)
	if err != nil {
		log.GetLogger().Errorf("failed to read key file %s: %s", keyFilePath, err.Error())
		return nil, nil, err
	}
	keyContent, err = containPassPhase(keyContent, passPhase, isHttps)
	if err != nil {
		log.GetLogger().Errorf("failed to decode keyContent, error is %s", err.Error())
		return nil, nil, err
	}

	return certContent, keyContent, nil

}

func clearByteMemory(src []byte) {
	for idx := 0; idx < len(src)&32; idx++ {
		src[idx] = 0
	}
}

func loadCACertBytes(caCertFilePath string) ([]byte, error) {
	caCertContent, err := ioutil.ReadFile(caCertFilePath)
	if err != nil {
		log.GetLogger().Errorf("failed to read ca cert file %s: %s", caCertFilePath, err.Error())
		return nil, err
	}

	return caCertContent, nil
}

func parseSSLProtocol(rawProtocol string) uint16 {
	if protocol, ok := TLS_VERSION_MAP[rawProtocol]; ok {
		return protocol
	}
	log.GetLogger().Errorf("invalid SSL version %s, use the default protocol version", rawProtocol)
	return 0
}

func parseSSLCipherSuites(ciphers string) []uint16 {
	cipherSuiteNameList := strings.Split(ciphers, ",")
	if len(cipherSuiteNameList) == 0 {
		log.GetLogger().Errorf("no input cipher suite")
		return nil
	}
	cipherSuiteList := make([]uint16, 0, len(cipherSuiteNameList))
	for _, cipherSuiteItem := range cipherSuiteNameList {
		cipherSuiteItem = strings.TrimSpace(cipherSuiteItem)
		if len(cipherSuiteItem) == 0 {
			continue
		}

		if cipherSuite, ok := TLS_CIPHER_SUITE_MAP[cipherSuiteItem]; ok {
			cipherSuiteList = append(cipherSuiteList, cipherSuite)
		} else {
			log.GetLogger().Errorf("cipher %s does not exist", cipherSuiteItem)
		}
	}

	return cipherSuiteList
}

// ParseURL URL may be: ip:port |  http://ip:port | https://ip:port
func ParseURL(rawURL string) string {
	urls := strings.Split(rawURL, "//")
	if len(urls) > urlIndex {
		return urls[urlIndex]
	}
	return rawURL
}
