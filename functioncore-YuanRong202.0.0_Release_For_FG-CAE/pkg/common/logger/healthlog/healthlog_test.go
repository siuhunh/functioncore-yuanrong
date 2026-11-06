package healthlog

import "testing"

func TestPrintHealthLog(t *testing.T) {
	type args struct {
		stopCh   chan struct{}
		inputLog func()
		name     string
	}
	var a args
	a.stopCh = nil
	a.inputLog = func() {
		return
	}

	tests := []struct {
		name string
		args args
	}{
		{
			name: "case1",
			args: args {
			stopCh: nil,
			inputLog: func() {
					return
				},
			},
		},
		{
			name: "case2",
			args: args {
				stopCh: make(chan struct{}),
				inputLog: func() {
					return
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.args.stopCh != nil {
				close(tt.args.stopCh)
			}
			PrintHealthLog(tt.args.stopCh, tt.args.inputLog, tt.args.name)
		})
	}
}
