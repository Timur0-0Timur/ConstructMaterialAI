package httpapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type ConveyorRequest struct {
	Tag            string   `json:"tag"`             //обязательное
	ConveyorLength *float64 `json:"conveyor_length"` // обязательное
	BeltWidth      *float64 `json:"belt_width"`      // обязательное

	ConveyorFlowRate     *float64 `json:"conveyor_flow_rate,omitempty"`
	DriverPowerPerSecond *float64 `json:"driver_power_per_second,omitempty"`
	ConveyorSpeed        *float64 `json:"conveyor_speed,omitempty"`
	NumberOfWalkways     *float64 `json:"number_of_walkways,omitempty"`
}

func ConveyorHandler(w http.ResponseWriter, r *http.Request) {
	// хэндлер, позволяющий получить данные от пользователя(json) о конкретном конвейере и вернуть ему результат ml-рассчета
	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		w.Write([]byte("Invalid request: Wrong method"))
		fmt.Println("Wrong method request")
		return
	}

	var reqBody ConveyorRequest
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

	if reqBody.ConveyorLength == nil || *reqBody.ConveyorLength <= 0 {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Invalid request: missing or invalid conveyor_length"))
		fmt.Println("missing/invalid conveyor_length")
		return
	}

	if reqBody.BeltWidth == nil || *reqBody.BeltWidth <= 0 {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Invalid request: missing or invalid belt_width"))
		fmt.Println("missing/invalid belt_width")
		return
	}

	payload, err := json.Marshal(reqBody)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Internal Server Error"))
		fmt.Println("Error: during marshaling json", err.Error())
		return
	}

	endpoint := "http://localhost:8000/conveyor/estimate"
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
