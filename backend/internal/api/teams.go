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

// ─── DTO для Teams API ───────────────────────────────────────

type createTeamRequest struct {
	Name string `json:"name"`
}

type teamResponse struct {
	ID        uint      `json:"id"`
	Name      string    `json:"name"`
	OwnerID   uint      `json:"owner_id"`
	CreatedAt time.Time `json:"created_at"`
}

type addMemberRequest struct {
	Email string `json:"email"`
}

type teamMemberResponse struct {
	UserID uint   `json:"user_id"`
	Email  string `json:"email"`
	Role   string `json:"role"`
}

// ─── Маршрутизаторы ──────────────────────────────────────────

// TeamsHandler обрабатывает /api/teams (POST — создать, GET — список)
func TeamsHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		createTeamHandler(w, r)
	case http.MethodGet:
		listTeamsHandler(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "Метод не поддерживается")
	}
}

// TeamsSubHandler обрабатывает /api/teams/{id}/... подмаршруты
func TeamsSubHandler(w http.ResponseWriter, r *http.Request) {
	// Парсим путь: /api/teams/{id}/members или /api/teams/{id}/projects или /api/teams/{id}
	path := strings.TrimPrefix(r.URL.Path, "/api/teams/")
	path = strings.TrimSuffix(path, "/")

	parts := strings.SplitN(path, "/", 2)
	if len(parts) == 0 || parts[0] == "" {
		writeError(w, http.StatusBadRequest, "Неверный путь")
		return
	}

	teamID, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Неверный ID команды")
		return
	}

	if len(parts) == 1 {
		// /api/teams/{id} — DELETE для удаления команды
		if r.Method == http.MethodDelete {
			deleteTeamHandler(w, r, uint(teamID))
		} else {
			writeError(w, http.StatusMethodNotAllowed, "Метод не поддерживается")
		}
		return
	}

	subPath := parts[1]
	switch subPath {
	case "members":
		switch r.Method {
		case http.MethodPost:
			addMemberHandler(w, r, uint(teamID))
		case http.MethodGet:
			listMembersHandler(w, r, uint(teamID))
		case http.MethodDelete:
			removeMemberHandler(w, r, uint(teamID))
		default:
			writeError(w, http.StatusMethodNotAllowed, "Метод не поддерживается")
		}
	case "projects":
		if r.Method == http.MethodGet {
			listTeamProjectsHandler(w, r, uint(teamID))
		} else {
			writeError(w, http.StatusMethodNotAllowed, "Метод не поддерживается")
		}
	default:
		writeError(w, http.StatusNotFound, "Маршрут не найден")
	}
}

// ─── Handlers ────────────────────────────────────────────────

// createTeamHandler — POST /api/teams
func createTeamHandler(w http.ResponseWriter, r *http.Request) {
	userID, ok := auth.GetUserID(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "Требуется авторизация")
		return
	}

	var req createTeamRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Неверный формат запроса")
		return
	}

	name := strings.TrimSpace(req.Name)
	if name == "" {
		writeError(w, http.StatusBadRequest, "Название команды обязательно")
		return
	}

	team := models.Team{
		Name:    name,
		OwnerID: userID,
	}

	if err := repository.CreateTeam(&team); err != nil {
		writeError(w, http.StatusInternalServerError, "Ошибка создания команды")
		return
	}

	writeJSON(w, http.StatusCreated, teamResponse{
		ID:        team.ID,
		Name:      team.Name,
		OwnerID:   team.OwnerID,
		CreatedAt: team.CreatedAt,
	})
}

// listTeamsHandler — GET /api/teams
func listTeamsHandler(w http.ResponseWriter, r *http.Request) {
	userID, ok := auth.GetUserID(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "Требуется авторизация")
		return
	}

	teams, err := repository.GetTeamsByUser(userID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Ошибка загрузки команд")
		return
	}

	result := make([]teamResponse, 0, len(teams))
	for _, t := range teams {
		result = append(result, teamResponse{
			ID:        t.ID,
			Name:      t.Name,
			OwnerID:   t.OwnerID,
			CreatedAt: t.CreatedAt,
		})
	}

	writeJSON(w, http.StatusOK, result)
}

// deleteTeamHandler — DELETE /api/teams/{id}
func deleteTeamHandler(w http.ResponseWriter, r *http.Request, teamID uint) {
	userID, ok := auth.GetUserID(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "Требуется авторизация")
		return
	}

	if err := repository.DeleteTeam(teamID, userID); err != nil {
		writeError(w, http.StatusForbidden, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"message": "Команда удалена"})
}

// addMemberHandler — POST /api/teams/{id}/members
func addMemberHandler(w http.ResponseWriter, r *http.Request, teamID uint) {
	userID, ok := auth.GetUserID(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "Требуется авторизация")
		return
	}

	// Только владелец может добавлять участников
	if !repository.IsTeamOwner(teamID, userID) {
		writeError(w, http.StatusForbidden, "Только владелец может добавлять участников")
		return
	}

	var req addMemberRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Неверный формат запроса")
		return
	}

	email := strings.TrimSpace(strings.ToLower(req.Email))
	if email == "" {
		writeError(w, http.StatusBadRequest, "Email обязателен")
		return
	}

	// Ищем пользователя по email
	user, err := repository.FindUserByEmail(email)
	if err != nil {
		writeError(w, http.StatusNotFound, "Пользователь с таким email не найден")
		return
	}

	// Проверяем, не является ли уже участником
	if repository.IsTeamMember(teamID, user.ID) {
		writeError(w, http.StatusConflict, "Пользователь уже является участником команды")
		return
	}

	if err := repository.AddTeamMember(teamID, user.ID, "member"); err != nil {
		writeError(w, http.StatusInternalServerError, "Ошибка добавления участника")
		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{
		"message": "Участник добавлен",
	})
}

// listMembersHandler — GET /api/teams/{id}/members
func listMembersHandler(w http.ResponseWriter, r *http.Request, teamID uint) {
	userID, ok := auth.GetUserID(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "Требуется авторизация")
		return
	}

	// Только участники команды могут видеть список
	if !repository.IsTeamMember(teamID, userID) {
		writeError(w, http.StatusForbidden, "Нет доступа к команде")
		return
	}

	members, err := repository.GetTeamMembers(teamID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Ошибка загрузки участников")
		return
	}

	result := make([]teamMemberResponse, 0, len(members))
	for _, m := range members {
		result = append(result, teamMemberResponse{
			UserID: m.UserID,
			Email:  m.Email,
			Role:   m.Role,
		})
	}

	writeJSON(w, http.StatusOK, result)
}

// removeMemberHandler — DELETE /api/teams/{id}/members?user_id=X
func removeMemberHandler(w http.ResponseWriter, r *http.Request, teamID uint) {
	currentUserID, ok := auth.GetUserID(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "Требуется авторизация")
		return
	}

	targetUserIDStr := r.URL.Query().Get("user_id")
	targetUserID, err := strconv.ParseUint(targetUserIDStr, 10, 64)
	if err != nil || targetUserID == 0 {
		writeError(w, http.StatusBadRequest, "Неверный user_id")
		return
	}

	isOwner := repository.IsTeamOwner(teamID, currentUserID)
	isSelf := currentUserID == uint(targetUserID)

	// Владелец может удалять любого; участник может удалить только себя (выйти)
	if !isOwner && !isSelf {
		writeError(w, http.StatusForbidden, "Недостаточно прав")
		return
	}

	// Владелец не может удалить сам себя (он должен удалить команду)
	if isOwner && isSelf {
		writeError(w, http.StatusBadRequest, "Владелец не может покинуть команду. Удалите команду.")
		return
	}

	if err := repository.RemoveTeamMember(teamID, uint(targetUserID)); err != nil {
		writeError(w, http.StatusInternalServerError, "Ошибка удаления участника")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"message": "Участник удалён"})
}

// listTeamProjectsHandler — GET /api/teams/{id}/projects
func listTeamProjectsHandler(w http.ResponseWriter, r *http.Request, teamID uint) {
	userID, ok := auth.GetUserID(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "Требуется авторизация")
		return
	}

	// Только участники команды могут видеть проекты
	if !repository.IsTeamMember(teamID, userID) {
		writeError(w, http.StatusForbidden, "Нет доступа к команде")
		return
	}

	projects, err := repository.GetProjectsByTeam(teamID)
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
			TeamID:      p.TeamID,
			OwnerID:     p.UserID,
			CreatedAt:   p.CreatedAt,
		})
	}

	writeJSON(w, http.StatusOK, result)
}
