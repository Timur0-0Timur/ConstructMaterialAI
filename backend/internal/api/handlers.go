package httpapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"time"
)

func DefaultHandler(w http.ResponseWriter, r *http.Request) {
	//чтоб проверять, что запросы на бэк вообще приходят
	_, err := w.Write([]byte("Ok"))
	if err != nil {
		fmt.Printf("Error : %s\n", err.Error())
	} else {
		fmt.Println("Default request made successfully")
	}
}

func ExcelHandler(w http.ResponseWriter, r *http.Request) {
	// проверка end to end, позволяет закидывать датасет в формате эксель
	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		w.Write([]byte("Invalid request: Wrong method"))
		fmt.Println("Wrong method request")
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 20<<20)

	err := r.ParseMultipartForm(20 << 20)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("invalid request"))
		fmt.Println("invalid multipart form", err.Error())
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("invalid request"))
		fmt.Println("missing file field", err.Error())
		return
	}

	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("invalid request: cannot read uploaded file"))
		fmt.Println("invalid file field", err.Error())
		return
	}

	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)

	part, err := mw.CreateFormFile("file", header.Filename)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Internal Server Error"))
		fmt.Println("Error: during creating multipart", err.Error())
		return
	}

	_, err = part.Write(data)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Internal Server Error"))
		fmt.Println("Error: during copying data", err.Error())
		return
	}

	err = mw.Close()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Internal Server Error"))
		fmt.Println("Error: during closing multipart writer", err.Error())
		return
	}

	endpoint := "http://192.168.1.151:8000/ingest"
	req, err := http.NewRequest(http.MethodPost, endpoint, &buf)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Internal Server Error"))
		fmt.Println("Error: during creating request", err.Error())
		return
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
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
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Internal Server Error"))
		fmt.Println("Error: during sending response", err.Error())
	}
}

type PumpRequest struct {
	Tag                  string   `json:"tag"`
	LiquidFlowRateLS     *float64 `json:"liquid_flow_rate_ls,omitempty"`
	FluidHeadM           *float64 `json:"fluid_head_m,omitempty"`
	SpeedRPM             *float64 `json:"speed_rpm,omitempty"`
	FluidSpecificGravity *float64 `json:"fluid_specific_gravity,omitempty"`
	DriverPowerKW        *float64 `json:"driver_power_kw,omitempty"`
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

	payload, err := json.Marshal(reqBody)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Internal Server Error"))
		fmt.Println("Error: during marshaling json", err.Error())
		return
	}

	endpoint := "http://192.168.1.151:8000/pump/estimate"
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
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Internal Server Error"))
		fmt.Println("Error: during sending response", err.Error())
	}
}
