package db

import (
	"constructmaterialai/internal/models"
	"fmt"
	"os"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// DB — глобальный экземпляр соединения
var DB *gorm.DB

// Connect инициализирует подключение к базе данных
func Connect() error {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return fmt.Errorf("DATABASE_URL не задана")
	}

	var err error
	DB, err = gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Warn),
	})
	if err != nil {
		return fmt.Errorf("ошибка подключения к БД: %w", err)
	}

	sqlDB, err := DB.DB()
	if err != nil {
		return fmt.Errorf("ошибка получения sql.DB: %w", err)
	}
	sqlDB.SetMaxOpenConns(10)
	sqlDB.SetMaxIdleConns(5)

	return nil
}

// Migrate выполняет автомиграцию всех таблиц
func Migrate() error {
	return DB.AutoMigrate(
		&models.User{},
		&models.Project{},
		&models.EquipmentItem{},
		&models.Team{},
		&models.TeamMember{},
	)
}
