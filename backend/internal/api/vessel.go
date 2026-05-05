package httpapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type VesselRequest struct {
	Tag                          string   `json:"tag"`                              //обязательное
	VesselDiameter               *float64 `json:"vessel_diameter"`                  // обязательное
	VesselTangentToTangentHeight *float64 `json:"vessel_tangent_to_tangent_height"` // обязательное

	DesignGaugePressure  *float64 `json:"design_gauge_pressure,omitempty"`
	DesignTemperature    *float64 `json:"design_temperature,omitempty"`
	OperatingTemperature *float64 `json:"operating_temperature,omitempty"`
	SkirtHeight          *float64 `json:"skirt_height,omitempty"`
	VesselLegHeight      *float64 `json:"vessel_leg_height,omitempty"`
}

func VesselHandler(w http.ResponseWriter, r *http.Request) {
	// хэндлер, позволяющий получить данные от пользователя(json) о конкретном сосуде и вернуть ему результат ml-рассчета
	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		w.Write([]byte("Invalid request: Wrong method"))
		fmt.Println("Wrong method request")
		return
	}

	var reqBody VesselRequest
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

	if reqBody.VesselDiameter == nil || *reqBody.VesselDiameter <= 0 {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Invalid request: missing or invalid vessel_diameter"))
		fmt.Println("missing/invalid vessel_diameter")
		return
	}

	if reqBody.VesselTangentToTangentHeight == nil || *reqBody.VesselTangentToTangentHeight <= 0 {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Invalid request: missing or invalid vessel_tangent_to_tangent_height"))
		fmt.Println("missing/invalid vessel_tangent_to_tangent_height")
		return
	}

	payload, err := json.Marshal(reqBody)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Internal Server Error"))
		fmt.Println("Error: during marshaling json", err.Error())
		return
	}

	endpoint := "http://localhost:8000/vessel/estimate"
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
