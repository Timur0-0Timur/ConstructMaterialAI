package main

import (
	"constructmaterialai/internal/api"
	"fmt"
	"net/http"
)

func main() {
	mux := http.NewServeMux()
	httpapi.RegisterRoutes(mux)

	if err := http.ListenAndServe(":8080", mux); err != nil {

		fmt.Println("Error: during starting server", err.Error())

	}
}
