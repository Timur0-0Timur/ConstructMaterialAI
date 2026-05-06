package repository

import (
	"constructmaterialai/internal/db"
	"constructmaterialai/internal/models"
	"fmt"
	"time"

	"gorm.io/gorm"
)

// CreateProject сохраняет проект и все связанные EquipmentItems за один раз.
// GORM автоматически делает INSERT в equipment_items через ассоциацию.
func CreateProject(project *models.Project) error {
	return db.DB.Create(project).Error
}

// UpdateProject обновляет проект и его оборудование.
// Проверяет UpdatedAt для предотвращения конфликтов (Optimistic Locking).
func UpdateProject(project *models.Project, oldUpdatedAt time.Time) error {
	return db.DB.Transaction(func(tx *gorm.DB) error {
		// Проверяем версию
		var currentProject models.Project
		if err := tx.Where("id = ?", project.ID).First(&currentProject).Error; err != nil {
			return err
		}

		if currentProject.UpdatedAt.After(oldUpdatedAt) && !currentProject.UpdatedAt.Equal(oldUpdatedAt) {
			return fmt.Errorf("CONFLICT_VERSION") // Специальная ошибка для конфликта
		}

		// Удаляем старое оборудование
		if err := tx.Where("project_id = ?", project.ID).Delete(&models.EquipmentItem{}).Error; err != nil {
			return err
		}

		// Обновляем проект
		if err := tx.Save(project).Error; err != nil {
			return err
		}

		return nil
	})
}

// GetProjectsByUser возвращает проекты, доступные пользователю:
// — личные (user_id = ?) И team_id IS NULL
// — командные (team_id IN (команды пользователя))
func GetProjectsByUser(userID uint) ([]models.Project, error) {
	teamIDs, err := GetUserTeamIDs(userID)
	if err != nil {
		return nil, err
	}

	var projects []models.Project
	query := db.DB.Preload("EquipmentItems").Order("created_at DESC")

	if len(teamIDs) > 0 {
		query = query.Where(
			"(user_id = ?) OR (team_id IN (?))",
			userID, teamIDs,
		)
	} else {
		query = query.Where("user_id = ?", userID)
	}

	err = query.Find(&projects).Error
	return projects, err
}

// GetPersonalProjects возвращает только личные проекты пользователя (без командных)
func GetPersonalProjects(userID uint) ([]models.Project, error) {
	var projects []models.Project
	err := db.DB.
		Where("user_id = ? AND (team_id IS NULL OR team_id = 0)", userID).
		Order("created_at DESC").
		Find(&projects).Error
	return projects, err
}

// GetProjectsByTeam возвращает проекты конкретной команды
func GetProjectsByTeam(teamID uint) ([]models.Project, error) {
	var projects []models.Project
	err := db.DB.Preload("EquipmentItems").
		Where("team_id = ?", teamID).
		Order("created_at DESC").
		Find(&projects).Error
	return projects, err
}

// GetProjectByID возвращает конкретный проект со всем оборудованием через Preload.
// Проверяет доступ: user_id = ? ИЛИ team_id IN (команды пользователя)
func GetProjectByID(projectID, userID uint) (*models.Project, error) {
	teamIDs, err := GetUserTeamIDs(userID)
	if err != nil {
		return nil, err
	}

	var project models.Project
	query := db.DB.Preload("EquipmentItems")

	if len(teamIDs) > 0 {
		query = query.Where(
			"id = ? AND ((user_id = ?) OR (team_id IN (?)))",
			projectID, userID, teamIDs,
		)
	} else {
		query = query.Where("id = ? AND user_id = ?", projectID, userID)
	}

	if err := query.First(&project).Error; err != nil {
		return nil, err
	}
	return &project, nil
}

// MoveProjectToTeam переносит проект в команду (или обратно в личные, если teamID == nil).
// Переносить может только владелец проекта (user_id).
func MoveProjectToTeam(projectID, ownerID uint, teamID *uint) error {
	result := db.DB.Model(&models.Project{}).
		Where("id = ? AND user_id = ?", projectID, ownerID).
		Update("team_id", teamID)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("проект не найден или вы не являетесь владельцем")
	}
	return nil
}

// DeleteProject удаляет проект (только если пользователь является владельцем).
func DeleteProject(projectID, userID uint) error {
	result := db.DB.
		Where("id = ? AND user_id = ?", projectID, userID).
		Delete(&models.Project{})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("проект не найден или вы не являетесь владельцем")
	}
	return nil
}
