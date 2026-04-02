package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/layout"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
)

// HTTP-КЛИЕНТ

const backendURL = "http://localhost:8080/pump/estimate"

// PumpRequest — структура данных, которую ожидает бэкенд
type PumpRequest struct {
	Tag       string   `json:"tag"`
	FlowRate  *float64 `json:"flow_rate,omitempty"`  // обязательное
	FluidHead *float64 `json:"fluid_head,omitempty"` // обязательное

	// опциональные
	RPM         *float64 `json:"rpm,omitempty"`
	SpecGravity *float64 `json:"spec_gravity,omitempty"`
	PowerKW     *float64 `json:"power_kw,omitempty"`
}

// sendToBackend — функция отправки данных о насосе на бэкенд
func sendToBackend(data PumpRequest) (string, error) {
	jsonBody, err := json.Marshal(data)
	if err != nil {
		return "", fmt.Errorf("ошибка JSON: %w", err)
	}

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Post(backendURL, "application/json", bytes.NewReader(jsonBody))
	if err != nil {
		return "", fmt.Errorf("сетевая ошибка: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("ошибка чтения ответа: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("сервер вернул ошибку %d: %s", resp.StatusCode, string(body))
	}

	return string(body), nil
}

// ИНТЕРФЕЙС (UI)

func runUI() {
	fmt.Println("Инициализация интерфейса...")
	myApp := app.New()
	myWindow := myApp.NewWindow("ConstructMaterialAI: Расчет оборудования")
	myWindow.Resize(fyne.NewSize(450, 500))

	showMainMenu(myWindow)

	myWindow.CenterOnScreen()
	myWindow.ShowAndRun()
}

func showMainMenu(myWindow fyne.Window) {
	label := widget.NewLabel("Выберите тип оборудования")
	label.Alignment = fyne.TextAlignCenter
	label.TextStyle = fyne.TextStyle{Bold: true}

	pumpBtn := widget.NewButton("Насосы", func() {
		showPumpForm(myWindow)
	})
	pumpBtn.Importance = widget.HighImportance

	vesselBtn := widget.NewButton("Vertical process vessel (в разработке)", func() {
		// Заглушка
	})
	vesselBtn.Disable()

	drumBtn := widget.NewButton("Horizontal drum (в разработке)", func() {
		// Заглушка
	})
	drumBtn.Disable()

	conveyorBtn := widget.NewButton("Belt conveyor open (в разработке)", func() {
		// Заглушка
	})
	conveyorBtn.Disable()

	content := container.NewVBox(
		widget.NewLabel(""),
		label,
		widget.NewLabel(""),
		pumpBtn,
		vesselBtn,
		drumBtn,
		conveyorBtn,
		layout.NewSpacer(),
	)

	myWindow.SetContent(container.NewPadded(content))
}

func showPumpForm(myWindow fyne.Window) {
	// Поля ввода
	tagEntry := widget.NewEntry()
	tagEntry.SetPlaceHolder("Напр.: Pump-123")

	flowEntry := widget.NewEntry()
	flowEntry.SetPlaceHolder("Число (напр. 150.5)")

	headEntry := widget.NewEntry()
	headEntry.SetPlaceHolder("Число (напр. 45.0)")

	// Опциональные поля
	rpmEntry := widget.NewEntry()
	rpmEntry.SetPlaceHolder("Число (опционально)")

	specGravityEntry := widget.NewEntry()
	specGravityEntry.SetPlaceHolder("Число (опционально)")

	powerEntry := widget.NewEntry()
	powerEntry.SetPlaceHolder("Число (опционально)")

	// для статуса
	statusLabel := widget.NewLabel("")
	statusLabel.Alignment = fyne.TextAlignCenter

	// Кнопка отправки
	var submitBtn *widget.Button
	submitBtn = widget.NewButtonWithIcon("Рассчитать", theme.ConfirmIcon(), func() {
		// Собираем текст из полей
		tagStr := tagEntry.Text
		flowStr := flowEntry.Text
		headStr := headEntry.Text

		// Валидация на пустоту
		if tagStr == "" || flowStr == "" || headStr == "" {
			statusLabel.SetText("⚠ Заполните все обязательные поля!")
			statusLabel.Refresh()
			return
		}

		// Конвертация в числа (Flow и Head)
		flow, errF := strconv.ParseFloat(flowStr, 64)
		head, errH := strconv.ParseFloat(headStr, 64)

		if errF != nil || errH != nil {
			statusLabel.SetText("⚠ Ошибка: Расход и Напор должны быть числами!")
			statusLabel.Refresh()
			return
		}

		// Конвертация опциональных полей (если заполнены)
		var rpm, specGravity, power *float64

		parseOptional := func(s string) (*float64, error) {
			if s == "" {
				return nil, nil
			}
			val, err := strconv.ParseFloat(s, 64)
			if err != nil {
				return nil, err
			}
			return &val, nil
		}

		var err error
		rpm, err = parseOptional(rpmEntry.Text)
		if err != nil {
			statusLabel.SetText("⚠ Ошибка: RPM должен быть числом!")
			statusLabel.Refresh()
			return
		}
		specGravity, err = parseOptional(specGravityEntry.Text)
		if err != nil {
			statusLabel.SetText("⚠ Ошибка: Spec Gravity должна быть числом!")
			statusLabel.Refresh()
			return
		}
		power, err = parseOptional(powerEntry.Text)
		if err != nil {
			statusLabel.SetText("⚠ Ошибка: Power должен быть числом!")
			statusLabel.Refresh()
			return
		}

		// Подготовка к отправке
		submitBtn.Disable()
		statusLabel.SetText("⏳ Выполняется расчет...")
		statusLabel.Refresh()

		go func() {
			// Формируем запрос
			data := PumpRequest{
				Tag:         tagStr,
				FlowRate:    &flow,
				FluidHead:   &head,
				RPM:         rpm,
				SpecGravity: specGravity,
				PowerKW:     power,
			}

			// Отправляем
			response, err := sendToBackend(data)

			// Обработка результата
			if err != nil {
				statusLabel.SetText(fmt.Sprintf("✗ Ошибка: %s", err.Error()))
			} else {
				statusLabel.SetText(fmt.Sprintf("✓ Результат: %s", response))
			}

			// Обновляем UI
			statusLabel.Refresh()
			submitBtn.Enable()
		}()
	})
	submitBtn.Importance = widget.HighImportance

	// Кнопка назад
	backBtn := widget.NewButtonWithIcon("Назад в меню", theme.NavigateBackIcon(), func() {
		showMainMenu(myWindow)
	})

	// Форма
	form := container.New(
		layout.NewFormLayout(),
		widget.NewLabel("Имя насоса (Tag):"), tagEntry,
		widget.NewLabel("Расход (Flow Rate):"), flowEntry,
		widget.NewLabel("Напор (Fluid Head):"), headEntry,
		widget.NewLabel("Обороты (RPM):"), rpmEntry,
		widget.NewLabel("Уд. вес (Spec Gravity):"), specGravityEntry,
		widget.NewLabel("Мощность (Power kW):"), powerEntry,
	)

	// Компоновка окна
	content := container.NewVBox(
		backBtn,
		widget.NewLabel("Расчет характеристик насоса"),
		form,
		layout.NewSpacer(),
		container.NewCenter(statusLabel),
		submitBtn,
		widget.NewLabel(""),
	)

	myWindow.SetContent(container.NewPadded(content))
}

func main() {
	fmt.Println("Запуск десктопного приложения...")
	runUI()
	fmt.Println("Приложение закрыто.")
}
