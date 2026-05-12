package main

import (
	"fmt"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
)

// Константы
const (
	windowWidth  = 1100
	windowHeight = 750
)

var equipmentTypes = []string{
	"Насосы",
	"Конвейер",
	"Вертикальный аппарат",
	"Горизонтальная емкость",
	"Трубчатый теплообменник",
	"Тарельчатая колонна",
}

func main() {
	fmt.Println("Запуск десктопного приложения...")

	// Восстанавливаем токен авторизации из файла (если был выполнен вход ранее)
	loadAuthState()

	myApp := app.New()
	myApp.Settings().SetTheme(newModernDarkTheme())
	myWindow := myApp.NewWindow("ConstructMaterialAI: Учёт оборудования")
	myWindow.Resize(fyne.NewSize(windowWidth, windowHeight))

	showStartScreen(myWindow)

	myWindow.CenterOnScreen()
	myWindow.ShowAndRun()

	fmt.Println("Приложение закрыто.")
}
