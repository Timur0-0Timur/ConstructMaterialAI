package repository

import (
	"constructmaterialai/internal/db"
	"constructmaterialai/internal/models"
)

// CreateProject сохраняет проект и все связанные EquipmentItems за один раз.
// GORM автоматически делает INSERT в equipment_items через ассоциацию.
func CreateProject(project *models.Project) error {
	return db.DB.Create(project).Error
}

// GetProjectsByUser возвращает список проектов пользователя (без оборудования).
func GetProjectsByUser(userID uint) ([]models.Project, error) {
	var projects []models.Project
	err := db.DB.
		Where("user_id = ?", userID).
		Order("created_at DESC").
		Find(&projects).Error
	return projects, err
}

// GetProjectByID возвращает конкретный проект со всем оборудованием через Preload.
// Проверяет принадлежность проекта пользователю (userID).
func GetProjectByID(projectID, userID uint) (*models.Project, error) {
	var project models.Project
	err := db.DB.
		Preload("EquipmentItems").
		Where("id = ? AND user_id = ?", projectID, userID).
		First(&project).Error
	if err != nil {
		return nil, err
	}
	return &project, nil
}

// DeleteProject удаляет проект (только если он принадлежит пользователю).
func DeleteProject(projectID, userID uint) error {
	return db.DB.
		Where("id = ? AND user_id = ?", projectID, userID).
		Delete(&models.Project{}).Error
}
