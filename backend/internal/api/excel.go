package httpapi

import (
	"bytes"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"time"
)

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
		w.Write([]byte("Internal Server Error"))
		fmt.Println("Error: during sending response", err.Error())
	}
}
