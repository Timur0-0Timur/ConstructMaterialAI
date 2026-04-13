package httpapi

import "net/http"

// RegisterRoutes регистрирует все эндпоинты API.
func RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/default", DefaultHandler)
	mux.HandleFunc("/excel", ExcelHandler)

	mux.HandleFunc("/pump/estimate", PumpHandler)
	mux.HandleFunc("/drum/estimate", DrumHandler)
	mux.HandleFunc("/vessel/estimate", VesselHandler)
	mux.HandleFunc("/conveyor/estimate", ConveyorHandler)
}
