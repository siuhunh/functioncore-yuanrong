package server

import (
	"net/http"
	"testing"

	"github.com/agiledragon/gomonkey/v2"

	"functioncore/pkg/common/localauth"
	"functioncore/pkg/web-terminal/config"
	"functioncore/pkg/web-terminal/options"
	mockUtils "functioncore/test/utils"
)

func TestRun(t *testing.T) {
	t.Run("success running!", func(t *testing.T) {
		// Mock the necessary functions and objects
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
				return &config.Config{LocalAuth: &localauth.AuthConfig{
					AKey: "test1",
					SKey: "test2",
				}}, nil
			}),
			gomonkey.ApplyFunc(config.WatchConfig,
				func(stopCh <-chan struct{}, callback config.ChangedCallback) error {
					return nil
				}),
			gomonkey.ApplyFunc(startServer, func(httpServer *http.Server) error {
				return nil
			}),
		})

		// Create a stop channel for the test
		stopCh := make(chan struct{})

		// Run the Run function with mocked dependencies
		err := Run(&options.WebTerminalOptions{}, stopCh)
		if err != nil {
			t.Errorf("Expected nil error, got %v", err)
		}

		// Close the stop channel to avoid goroutine leaks
		patches.ResetAll()
		t.Cleanup(func() {
			close(stopCh)
		})
	})
}
