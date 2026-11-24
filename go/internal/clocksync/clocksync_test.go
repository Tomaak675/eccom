package clocksync

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"eccom/internal/control"
)

func TestOffsetAndRTTMatchThePythonFormula(t *testing.T) {
	offset, rtt := OffsetAndRTT(100, 150, 160, 130)
	if offset != 40 || rtt != 20 {
		t.Fatalf("offset %d rtt %d", offset, rtt)
	}
}

func TestProbeAgainstTheControlEndpoint(t *testing.T) {
	server := control.New("127.0.0.1", 0)
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	defer server.Stop(context.Background())
	probe, err := ProbeOffset(context.Background(), "http://"+server.Addr().String()+"/")
	if err != nil {
		t.Fatal(err)
	}
	if probe.Samples != samples || probe.MinRTTNs < 0 || probe.MeasuredAtWallNs <= 0 {
		t.Fatalf("probe %+v", probe)
	}
	// same clock on both ends: the offset cannot exceed the round trip
	if probe.OffsetNs > probe.MinRTTNs || -probe.OffsetNs > probe.MinRTTNs || probe.UncertaintyNs != probe.MinRTTNs/2 {
		t.Fatalf("probe %+v", probe)
	}
}

func TestProbeFailsWithoutAnEndpoint(t *testing.T) {
	if _, err := ProbeOffset(context.Background(), "http://127.0.0.1:1"); err == nil {
		t.Fatal("expected a connection error")
	}
}

func TestCombineWidensTheBoundByDrift(t *testing.T) {
	before := Probe{OffsetNs: 10, UncertaintyNs: 5, MinRTTNs: 10, Samples: 15}
	after := Probe{OffsetNs: 20, UncertaintyNs: 3, MinRTTNs: 6, Samples: 15}
	record := Combine(before, &after)
	if record.Method != Method || record.OffsetNs != 15 || record.UncertaintyNs != 10 || *record.DriftNs != 10 || record.After == nil {
		t.Fatalf("record %+v", record)
	}
	alone := Combine(before, nil)
	if alone.OffsetNs != 10 || alone.UncertaintyNs != 5 || alone.DriftNs != nil || alone.After != nil {
		t.Fatalf("record %+v", alone)
	}
}

func TestRecordJSONHasThePythonKeys(t *testing.T) {
	data, err := json.Marshal(Combine(Probe{OffsetNs: 1, UncertaintyNs: 2, MinRTTNs: 4, Samples: 15, MeasuredAtWallNs: 9}, nil))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"method":"control-endpoint-ntp-probe","offset_ns":1,"uncertainty_ns":2,"drift_ns":null,` +
		`"before":{"offset_ns":1,"uncertainty_ns":2,"min_rtt_ns":4,"samples":15,"measured_at_wall_ns":9},"after":null}`
	if strings.TrimSpace(string(data)) != want {
		t.Fatalf("record json %s", data)
	}
}
