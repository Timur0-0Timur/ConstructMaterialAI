package httpapi

import (
	"constructmaterialai/internal/auth"
	"net/http"
)

// RegisterRoutes регистрирует все эндпоинты API.
func RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/default", DefaultHandler)
	mux.HandleFunc("/excel", ExcelHandler)

	mux.HandleFunc("/pump/estimate", PumpHandler)
	mux.HandleFunc("/drum/estimate", DrumHandler)
	mux.HandleFunc("/vessel/estimate", VesselHandler)
	mux.HandleFunc("/conveyor/estimate", ConveyorHandler)

	// Авторизация
	mux.HandleFunc("/api/auth/register", RegisterHandler)
	mux.HandleFunc("/api/auth/login", LoginHandler)

	// Проекты (защищенные)
	mux.Handle("/api/projects", auth.AuthMiddleware(http.HandlerFunc(ProjectsHandler)))
	mux.Handle("/api/projects/", auth.AuthMiddleware(http.HandlerFunc(ProjectByIDHandler)))
}
