package main

import (
	"constructmaterialai/internal/api"
	"fmt"
	"net/http"
)

func main() {
	http.HandleFunc("/default", httpapi.DefaultHandler)
	http.HandleFunc("/excel", httpapi.ExcelHandler)
	http.HandleFunc("/pump/estimate", httpapi.PumpHandler)
	if err := http.ListenAndServe(":8080", nil); err != nil {
		fmt.Println("Error: during sarting server", err.Error())
	}
}
