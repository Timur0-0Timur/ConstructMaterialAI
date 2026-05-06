package repository

import (
	"constructmaterialai/internal/db"
	"constructmaterialai/internal/models"
	"fmt"

	"gorm.io/gorm"
)

// TeamMemberInfo — участник команды с email (результат JOIN)
type TeamMemberInfo struct {
	UserID uint   `json:"user_id"`
	Email  string `json:"email"`
	Role   string `json:"role"`
}

// CreateTeam создаёт новую команду и добавляет владельца как участника с ролью owner
func CreateTeam(team *models.Team) error {
	return db.DB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(team).Error; err != nil {
			return err
		}
		// Владелец автоматически становится участником команды
		member := models.TeamMember{
			TeamID: team.ID,
			UserID: team.OwnerID,
			Role:   "owner",
		}
		return tx.Create(&member).Error
	})
}

// GetTeamsByUser возвращает команды, где пользователь является владельцем или участником
func GetTeamsByUser(userID uint) ([]models.Team, error) {
	var teams []models.Team
	err := db.DB.
		Where("id IN (SELECT team_id FROM team_members WHERE user_id = ?)", userID).
		Order("created_at DESC").
		Find(&teams).Error
	return teams, err
}

// GetTeamByID возвращает команду по ID
func GetTeamByID(teamID uint) (*models.Team, error) {
	var team models.Team
	if err := db.DB.First(&team, teamID).Error; err != nil {
		return nil, err
	}
	return &team, nil
}

// IsTeamMember проверяет, является ли пользователь участником команды
func IsTeamMember(teamID, userID uint) bool {
	var count int64
	db.DB.Model(&models.TeamMember{}).
		Where("team_id = ? AND user_id = ?", teamID, userID).
		Count(&count)
	return count > 0
}

// IsTeamOwner проверяет, является ли пользователь владельцем команды
func IsTeamOwner(teamID, userID uint) bool {
	var count int64
	db.DB.Model(&models.Team{}).
		Where("id = ? AND owner_id = ?", teamID, userID).
		Count(&count)
	return count > 0
}

// AddTeamMember добавляет пользователя в команду по его ID
func AddTeamMember(teamID, userID uint, role string) error {
	if role == "" {
		role = "member"
	}
	member := models.TeamMember{
		TeamID: teamID,
		UserID: userID,
		Role:   role,
	}
	return db.DB.Create(&member).Error
}

// FindUserByEmail находит пользователя по email
func FindUserByEmail(email string) (*models.User, error) {
	var user models.User
	if err := db.DB.Where("email = ?", email).First(&user).Error; err != nil {
		return nil, err
	}
	return &user, nil
}

// GetTeamMembers возвращает список участников команды с email (JOIN на users)
func GetTeamMembers(teamID uint) ([]TeamMemberInfo, error) {
	var members []TeamMemberInfo
	err := db.DB.
		Table("team_members").
		Select("team_members.user_id, users.email, team_members.role").
		Joins("JOIN users ON users.id = team_members.user_id").
		Where("team_members.team_id = ?", teamID).
		Scan(&members).Error
	return members, err
}

// RemoveTeamMember удаляет участника из команды
func RemoveTeamMember(teamID, userID uint) error {
	return db.DB.
		Where("team_id = ? AND user_id = ?", teamID, userID).
		Delete(&models.TeamMember{}).Error
}

// DeleteTeam удаляет команду и все связанные записи TeamMember.
// Проекты команды переводятся в личные проекты владельца (TeamID = nil).
func DeleteTeam(teamID, ownerID uint) error {
	return db.DB.Transaction(func(tx *gorm.DB) error {
		// Проверяем, что удаляющий — владелец
		var team models.Team
		if err := tx.Where("id = ? AND owner_id = ?", teamID, ownerID).First(&team).Error; err != nil {
			return fmt.Errorf("команда не найдена или вы не являетесь владельцем")
		}

		// Переводим проекты команды в личные проекты владельца
		if err := tx.Model(&models.Project{}).
			Where("team_id = ?", teamID).
			Updates(map[string]interface{}{"team_id": nil}).Error; err != nil {
			return err
		}

		// Удаляем всех участников
		if err := tx.Where("team_id = ?", teamID).Delete(&models.TeamMember{}).Error; err != nil {
			return err
		}

		// Удаляем команду
		return tx.Delete(&team).Error
	})
}

// GetUserTeamIDs возвращает ID всех команд пользователя
func GetUserTeamIDs(userID uint) ([]uint, error) {
	var teamIDs []uint
	err := db.DB.Model(&models.TeamMember{}).
		Where("user_id = ?", userID).
		Pluck("team_id", &teamIDs).Error
	return teamIDs, err
}
