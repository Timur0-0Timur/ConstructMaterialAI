package httpapi

import (
	"constructmaterialai/internal/auth"
	"constructmaterialai/internal/models"
	"constructmaterialai/internal/repository"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// DTO для API проектов

type equipmentItemDTO struct {
	EquipmentType string          `json:"equipment_type"`
	Parameters    json.RawMessage `json:"parameters"`
	WeightResult  *float64        `json:"weight_result,omitempty"`
}

type createProjectRequest struct {
	Name           string             `json:"name"`
	Description    string             `json:"description"`
	EquipmentItems []equipmentItemDTO `json:"equipment_items"`
}

type projectListItem struct {
	ID          uint      `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	ItemCount   int       `json:"item_count"`
	CreatedAt   time.Time `json:"created_at"`
}

type equipmentItemResponse struct {
	ID            uint            `json:"id"`
	EquipmentType string          `json:"equipment_type"`
	Parameters    json.RawMessage `json:"parameters"`
	WeightResult  *float64        `json:"weight_result,omitempty"`
}

type projectDetailResponse struct {
	ID             uint                    `json:"id"`
	Name           string                  `json:"name"`
	Description    string                  `json:"description"`
	EquipmentItems []equipmentItemResponse `json:"equipment_items"`
	CreatedAt      time.Time               `json:"created_at"`
}

// ProjectsHandler — маршрутизатор для /api/projects (POST и GET)
func ProjectsHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		createProjectHandler(w, r)
	case http.MethodGet:
		listProjectsHandler(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "Метод не поддерживается")
	}
}

// ProjectByIDHandler — маршрутизатор для /api/projects/{id}
func ProjectByIDHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Метод не поддерживается")
		return
	}

	idStr := strings.TrimPrefix(r.URL.Path, "/api/projects/")
	idStr = strings.TrimSuffix(idStr, "/")
	projectID, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil || projectID == 0 {
		writeError(w, http.StatusBadRequest, "Неверный ID проекта")
		return
	}

	userID, ok := auth.GetUserID(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "Требуется авторизация")
		return
	}

	project, err := repository.GetProjectByID(uint(projectID), userID)
	if err != nil {
		writeError(w, http.StatusNotFound, "Проект не найден")
		return
	}

	// Формируем ответ с оборудованием
	items := make([]equipmentItemResponse, 0, len(project.EquipmentItems))
	for _, item := range project.EquipmentItems {
		items = append(items, equipmentItemResponse{
			ID:            item.ID,
			EquipmentType: item.EquipmentType,
			Parameters:    json.RawMessage(item.Parameters),
			WeightResult:  item.WeightResult,
		})
	}

	writeJSON(w, http.StatusOK, projectDetailResponse{
		ID:             project.ID,
		Name:           project.Name,
		Description:    project.Description,
		EquipmentItems: items,
		CreatedAt:      project.CreatedAt,
	})
}

// createProjectHandler — POST /api/projects
// Сохраняет новый проект со всем оборудованием.
func createProjectHandler(w http.ResponseWriter, r *http.Request) {
	userID, ok := auth.GetUserID(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "Требуется авторизация")
		return
	}

	var req createProjectRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Неверный формат запроса")
		return
	}

	if strings.TrimSpace(req.Name) == "" {
		writeError(w, http.StatusBadRequest, "Название проекта обязательно")
		return
	}

	// Строим модель для GORM
	project := models.Project{
		UserID:      userID,
		Name:        strings.TrimSpace(req.Name),
		Description: req.Description,
	}

	for _, dto := range req.EquipmentItems {
		if strings.TrimSpace(dto.EquipmentType) == "" {
			writeError(w, http.StatusBadRequest, "equipment_type обязателен для каждой единицы")
			return
		}
		project.EquipmentItems = append(project.EquipmentItems, models.EquipmentItem{
			EquipmentType: dto.EquipmentType,
			Parameters:    models.JSONB(dto.Parameters),
			WeightResult:  dto.WeightResult,
		})
	}

	// GORM создаёт project + все equipment_items в одной транзакции
	if err := repository.CreateProject(&project); err != nil {
		writeError(w, http.StatusInternalServerError, "Ошибка сохранения проекта")
		return
	}

	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"id":      project.ID,
		"message": "Проект успешно сохранён",
	})
}

// listProjectsHandler — GET /api/projects
// Возвращает список проектов авторизованного пользователя.
func listProjectsHandler(w http.ResponseWriter, r *http.Request) {
	userID, ok := auth.GetUserID(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "Требуется авторизация")
		return
	}

	projects, err := repository.GetProjectsByUser(userID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Ошибка загрузки проектов")
		return
	}

	result := make([]projectListItem, 0, len(projects))
	for _, p := range projects {
		result = append(result, projectListItem{
			ID:          p.ID,
			Name:        p.Name,
			Description: p.Description,
			CreatedAt:   p.CreatedAt,
		})
	}

	writeJSON(w, http.StatusOK, result)
}
