package httpapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type PumpRequest struct {
	Tag       string   `json:"tag"`
	FlowRate  *float64 `json:"flow_rate"`  // обязательное
	FluidHead *float64 `json:"fluid_head"` // обязательное

	// опциональные
	RPM         *float64 `json:"rpm,omitempty"`
	SpecGravity *float64 `json:"spec_gravity,omitempty"`
	PowerKW     *float64 `json:"power_kw,omitempty"`
}

func PumpHandler(w http.ResponseWriter, r *http.Request) {
	// хэндлер, позволяющий получить данные от пользователя(json) о конкретном насосе и вернуть ему результат ml-рассчета
	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		w.Write([]byte("Invalid request: Wrong method"))
		fmt.Println("Wrong method request")
		return
	}

	var reqBody PumpRequest
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

	if reqBody.FlowRate == nil || *reqBody.FlowRate <= 0 {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Invalid request: missing or invalid flow_rate"))
		fmt.Println("missing/invalid flow_rate")
		return
	}

	if reqBody.FluidHead == nil || *reqBody.FluidHead <= 0 {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Invalid request: missing or invalid fluid_head"))
		fmt.Println("missing/invalid fluid_head")
		return
	}

	payload, err := json.Marshal(reqBody)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Internal Server Error"))
		fmt.Println("Error: during marshaling json", err.Error())
		return
	}

	endpoint := "http://127.0.0.1:8000/pump/estimate"
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
