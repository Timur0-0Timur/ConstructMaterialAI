package httpapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type CentrifugalCompressorRequest struct {
	Tag                      string   `json:"tag"`
	ActualGasFlowRateInlet   *float64 `json:"actual_gas_flow_rate_inlet"`
	DesignGaugePressureInlet  *float64 `json:"design_gauge_pressure_inlet"`
	DesignGaugePressureOutlet *float64 `json:"design_gauge_pressure_outlet"`
	DriverPower              *float64 `json:"driver_power,omitempty"`
}

func CentrifugalCompressorHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		w.Write([]byte("Invalid request: Wrong method"))
		fmt.Println("Wrong method request")
		return
	}

	var reqBody CentrifugalCompressorRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	err := dec.Decode(&reqBody)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Invalid request: invalid json"))
		fmt.Println("invalid json", err.Error())
		return
	}

	if reqBody.Tag == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Invalid request: missing tag"))
		fmt.Println("missing tag")
		return
	}

	if reqBody.ActualGasFlowRateInlet == nil || *reqBody.ActualGasFlowRateInlet <= 0 {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Invalid request: missing or invalid actual_gas_flow_rate_inlet"))
		fmt.Println("missing/invalid actual_gas_flow_rate_inlet")
		return
	}

	if reqBody.DesignGaugePressureInlet == nil || *reqBody.DesignGaugePressureInlet <= 0 {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Invalid request: missing or invalid design_gauge_pressure_inlet"))
		fmt.Println("missing/invalid design_gauge_pressure_inlet")
		return
	}

	if reqBody.DesignGaugePressureOutlet == nil || *reqBody.DesignGaugePressureOutlet <= 0 {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Invalid request: missing or invalid design_gauge_pressure_outlet"))
		fmt.Println("missing/invalid design_gauge_pressure_outlet")
		return
	}

	payload, err := json.Marshal(reqBody)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Internal Server Error"))
		fmt.Println("Error: during marshaling json", err.Error())
		return
	}

	endpoint := "http://localhost:8000/centrifugal_compressor/estimate"
	httpReq, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Internal Server Error"))
		fmt.Println("Error: during creating request", err.Error())
		return
	}
	httpReq.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		w.WriteHeader(http.StatusBadGateway)
		w.Write([]byte("Internal Server Error"))
		fmt.Println("Error: during sending request", err.Error())
		return
	}
	defer resp.Body.Close()

	w.WriteHeader(resp.StatusCode)
	_, err = io.Copy(w, resp.Body)
	if err != nil {
		w.Write([]byte("Internal Server Error"))
		fmt.Println("Error: during sending response", err.Error())
	}
}
