package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"image/color"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/canvas"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/layout"
	"fyne.io/fyne/v2/storage"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
)

// ─── Константы ───────────────────────────────────────────────

const (
	dataFile           = "projects.json"
	pumpBackendURL     = "http://localhost:8080/pump/estimate"
	conveyorBackendURL = "http://localhost:8080/conveyor/estimate"
	vesselBackendURL   = "http://localhost:8080/vessel/estimate"
	drumBackendURL     = "http://localhost:8080/drum/estimate"
	windowWidth        = 1100
	windowHeight       = 750
)

var equipmentTypes = []string{
	"Насосы",
	"Конвейер",
	"Вертикальный аппарат",
	"Горизонтальная емкость",
}

// ─── Модели данных ───────────────────────────────────────────

// Equipment — единица оборудования внутри проекта.
// Характеристики зависят от типа, но хранятся в общей структуре.
type Equipment struct {
	Type     string `json:"type"`
	Tag      string `json:"tag"`
	Quantity int    `json:"quantity"`

	// Характеристики насоса
	FlowRate    *float64 `json:"flow_rate,omitempty"`
	FluidHead   *float64 `json:"fluid_head,omitempty"`
	RPM         *float64 `json:"rpm,omitempty"`
	SpecGravity *float64 `json:"spec_gravity,omitempty"`
	PowerKW     *float64 `json:"power_kw,omitempty"`

	// Характеристики конвейера
	ConveyorLength *float64 `json:"conveyor_length,omitempty"`
	BeltWidth      *float64 `json:"belt_width,omitempty"`

	// Характеристики vessel/drum
	VesselDiameter               *float64 `json:"vessel_diameter,omitempty"`
	DesignTangentToTangentLength *float64 `json:"design_tangent_to_tangent_length,omitempty"`
	VesselTangentToTangentHeight *float64 `json:"vessel_tangent_to_tangent_height,omitempty"`
	DesignGaugePressure          *float64 `json:"design_gauge_pressure,omitempty"`
	DesignTemperature            *float64 `json:"design_temperature,omitempty"`
	SkirtHeight                  *float64 `json:"skirt_height,omitempty"`
	VesselLegHeight              *float64 `json:"vessel_leg_height,omitempty"`

	// Рассчитанный вес за единицу
	CalculatedWeight float64 `json:"calculated_weight"`
}

// Project — проект со списком оборудования
type Project struct {
	Name      string      `json:"name"`
	Equipment []Equipment `json:"equipment"`
}

func (p Project) TotalWeight() float64 {
	var total float64
	for _, eq := range p.Equipment {
		total += eq.CalculatedWeight * float64(eq.Quantity)
	}
	return total
}

func (p Project) EquipmentCount() int {
	var count int
	for _, eq := range p.Equipment {
		count += eq.Quantity
	}
	return count
}

// AppData — корневая структура для JSON-файла
type AppData struct {
	Projects []Project `json:"projects"`
}

// ─── Бэкенд-запросы ─────────────────────────────────────────

// PumpRequest — то, что ожидает Go-бэкенд
type PumpRequest struct {
	Tag         string   `json:"tag"`
	FlowRate    *float64 `json:"flow_rate,omitempty"`
	FluidHead   *float64 `json:"fluid_head,omitempty"`
	RPM         *float64 `json:"rpm,omitempty"`
	SpecGravity *float64 `json:"spec_gravity,omitempty"`
	PowerKW     *float64 `json:"power_kw,omitempty"`
}

type ConveyorRequest struct {
	Tag            string   `json:"tag"`
	ConveyorLength *float64 `json:"conveyor_length"`
	BeltWidth      *float64 `json:"belt_width"`
}

type VesselRequest struct {
	Tag                          string   `json:"tag"`
	VesselDiameter               *float64 `json:"vessel_diameter"`
	VesselTangentToTangentHeight *float64 `json:"vessel_tangent_to_tangent_height"`
	DesignGaugePressure          *float64 `json:"design_gauge_pressure,omitempty"`
	DesignTemperature            *float64 `json:"design_temperature,omitempty"`
	SkirtHeight                  *float64 `json:"skirt_height,omitempty"`
	VesselLegHeight              *float64 `json:"vessel_leg_height,omitempty"`
}

type DrumRequest struct {
	Tag                          string   `json:"tag"`
	VesselDiameter               *float64 `json:"vessel_diameter"`
	DesignTangentToTangentLength *float64 `json:"design_tangent_to_tangent_length"`
}

// PumpResponse — ответ от бэкенда
type PumpResponse struct {
	ModelVersion string  `json:"model_version"`
	Weight       float64 `json:"weight"`
}

type ConveyorResponse struct {
	ModelVersion string  `json:"model_version"`
	Weight       float64 `json:"weight"`
}

type VesselResponse struct {
	ModelVersion string  `json:"model_version"`
	Weight       float64 `json:"weight"`
}

type DrumResponse struct {
	ModelVersion string  `json:"model_version"`
	Weight       float64 `json:"weight"`
}

func sendPumpToBackend(data PumpRequest) (float64, error) {
	jsonBody, err := json.Marshal(data)
	if err != nil {
		return 0, fmt.Errorf("ошибка JSON: %w", err)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Post(pumpBackendURL, "application/json", bytes.NewReader(jsonBody))
	if err != nil {
		return 0, fmt.Errorf("сетевая ошибка: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("ошибка чтения ответа: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("сервер (%d): %s", resp.StatusCode, string(body))
	}

	var pumpResp PumpResponse
	if err := json.Unmarshal(body, &pumpResp); err != nil {
		return 0, fmt.Errorf("ошибка разбора ответа: %w", err)
	}

	return pumpResp.Weight, nil
}

func sendConveyorToBackend(data ConveyorRequest) (float64, error) {
	jsonBody, err := json.Marshal(data)
	if err != nil {
		return 0, fmt.Errorf("ошибка JSON: %w", err)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Post(conveyorBackendURL, "application/json", bytes.NewReader(jsonBody))
	if err != nil {
		return 0, fmt.Errorf("сетевая ошибка: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("ошибка чтения ответа: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("сервер (%d): %s", resp.StatusCode, string(body))
	}

	var conveyorResp ConveyorResponse
	if err := json.Unmarshal(body, &conveyorResp); err != nil {
		return 0, fmt.Errorf("ошибка разбора ответа: %w", err)
	}

	return conveyorResp.Weight, nil
}

func sendVesselToBackend(data VesselRequest) (float64, error) {
	jsonBody, err := json.Marshal(data)
	if err != nil {
		return 0, fmt.Errorf("ошибка JSON: %w", err)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Post(vesselBackendURL, "application/json", bytes.NewReader(jsonBody))
	if err != nil {
		return 0, fmt.Errorf("сетевая ошибка: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("ошибка чтения ответа: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("сервер (%d): %s", resp.StatusCode, string(body))
	}

	var vesselResp VesselResponse
	if err := json.Unmarshal(body, &vesselResp); err != nil {
		return 0, fmt.Errorf("ошибка разбора ответа: %w", err)
	}

	return vesselResp.Weight, nil
}

func sendDrumToBackend(data DrumRequest) (float64, error) {
	jsonBody, err := json.Marshal(data)
	if err != nil {
		return 0, fmt.Errorf("ошибка JSON: %w", err)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Post(drumBackendURL, "application/json", bytes.NewReader(jsonBody))
	if err != nil {
		return 0, fmt.Errorf("сетевая ошибка: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("ошибка чтения ответа: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("сервер (%d): %s", resp.StatusCode, string(body))
	}

	var drumResp DrumResponse
	if err := json.Unmarshal(body, &drumResp); err != nil {
		return 0, fmt.Errorf("ошибка разбора ответа: %w", err)
	}

	return drumResp.Weight, nil
}

func sendEquipmentToBackend(eq Equipment) (float64, error) {
	switch eq.Type {
	case "Насосы":
		req := PumpRequest{
			Tag:         eq.Tag,
			FlowRate:    eq.FlowRate,
			FluidHead:   eq.FluidHead,
			RPM:         eq.RPM,
			SpecGravity: eq.SpecGravity,
			PowerKW:     eq.PowerKW,
		}
		return sendPumpToBackend(req)

	case "Конвейер":
		req := ConveyorRequest{
			Tag:            eq.Tag,
			ConveyorLength: eq.ConveyorLength,
			BeltWidth:      eq.BeltWidth,
		}
		return sendConveyorToBackend(req)

	case "Вертикальный аппарат":
		req := VesselRequest{
			Tag:                          eq.Tag,
			VesselDiameter:               eq.VesselDiameter,
			VesselTangentToTangentHeight: eq.VesselTangentToTangentHeight,
			DesignGaugePressure:          eq.DesignGaugePressure,
			DesignTemperature:            eq.DesignTemperature,
			SkirtHeight:                  eq.SkirtHeight,
			VesselLegHeight:              eq.VesselLegHeight,
		}
		return sendVesselToBackend(req)

	case "Горизонтальная емкость":
		req := DrumRequest{
			Tag:                          eq.Tag,
			VesselDiameter:               eq.VesselDiameter,
			DesignTangentToTangentLength: eq.DesignTangentToTangentLength,
		}
		return sendDrumToBackend(req)

	default:
		return 0, fmt.Errorf("неизвестный тип оборудования: %s", eq.Type)
	}
}

// ─── Persistence (JSON) ──────────────────────────────────────

func loadProjects() AppData {
	data := AppData{Projects: []Project{}}

	file, err := os.ReadFile(dataFile)
	if err != nil {
		return data
	}

	if err := json.Unmarshal(file, &data); err != nil {
		fmt.Println("Ошибка чтения JSON:", err)
		return AppData{Projects: []Project{}}
	}

	return data
}

func saveProjects(data AppData) error {
	file, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("ошибка сериализации: %w", err)
	}
	return os.WriteFile(dataFile, file, 0644)
}

// ─── Хелперы для парсинга ────────────────────────────────────

func parseOptionalFloat(s string) (*float64, error) {
	s = strings.TrimSpace(s)
	// Заменяем запятую на точку для поддержки обоих форматов ввода
	s = strings.ReplaceAll(s, ",", ".")
	if s == "" {
		return nil, nil
	}
	val, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil, err
	}
	return &val, nil
}

func floatPtrToStr(p *float64) string {
	if p == nil {
		return ""
	}
	return fmt.Sprintf("%.2f", *p)
}

// ─── UI: Карточка проекта ───────────────────────────────────

type projectCard struct {
	project   Project
	container *fyne.Container
	bg        *canvas.Rectangle
	accent    *canvas.Rectangle
}

func (c *projectCard) refreshTheme() {
	v := fyne.CurrentApp().Settings().ThemeVariant()
	if c.bg != nil {
		c.bg.FillColor = theme.Current().Color(ColorNameCardBackground, v)
		c.bg.Refresh()
	}
	if c.accent != nil {
		c.accent.FillColor = theme.PrimaryColor()
		c.accent.Refresh()
	}
}

// ─── UI: строка оборудования ─────────────────────────────────

// equipmentRow хранит ссылки на виджеты одной строки
type equipmentRow struct {
	// Общие
	typeSelect *widget.Select
	tagEntry   *widget.Entry
	qtyEntry   *widget.Entry

	// Насос
	flowLabel        *canvas.Text
	flowEntry        *widget.Entry
	headLabel        *canvas.Text
	headEntry        *widget.Entry
	rpmLabel         *canvas.Text
	rpmEntry         *widget.Entry
	specGravityLabel *canvas.Text
	specGravityEntry *widget.Entry
	powerLabel       *canvas.Text
	powerEntry       *widget.Entry

	// Конвейер
	conveyorLengthLabel *canvas.Text
	conveyorLengthEntry *widget.Entry
	beltWidthLabel      *canvas.Text
	beltWidthEntry      *widget.Entry

	// Vessel / Drum
	vesselDiameterLabel               *canvas.Text
	vesselDiameterEntry               *widget.Entry
	designTangentToTangentLengthLabel *canvas.Text
	designTangentToTangentLengthEntry *widget.Entry
	vesselTangentToTangentHeightLabel *canvas.Text
	vesselTangentToTangentHeightEntry *widget.Entry
	designGaugePressureLabel          *canvas.Text
	designGaugePressureEntry          *widget.Entry
	designTemperatureLabel            *canvas.Text
	designTemperatureEntry            *widget.Entry
	skirtHeightLabel                  *canvas.Text
	skirtHeightEntry                  *widget.Entry
	vesselLegHeightLabel              *canvas.Text
	vesselLegHeightEntry              *widget.Entry

	// Контейнер с полями характеристик
	fieldsContainer *fyne.Container

	// Результат
	resultLabel *widget.Label
	expandBtn   *widget.Button
	deleteBtn   *widget.Button
	container   *fyne.Container

	// Фоны для обновления темы
	cardBg    *canvas.Rectangle
	accentBar *canvas.Rectangle
	expandBg  *canvas.Rectangle
	deleteBg  *canvas.Rectangle
}

func (r *equipmentRow) refreshTheme() {
	v := fyne.CurrentApp().Settings().ThemeVariant()
	if r.cardBg != nil {
		r.cardBg.FillColor = theme.Current().Color(ColorNameCardBackground, v)
		r.cardBg.Refresh()
	}
	if r.accentBar != nil {
		r.accentBar.FillColor = theme.PrimaryColor()
		r.accentBar.Refresh()
	}
	if r.expandBg != nil {
		r.expandBg.FillColor = theme.Current().Color(theme.ColorNameInputBackground, v)
		r.expandBg.Refresh()
	}
	if r.deleteBg != nil {
		r.deleteBg.FillColor = theme.Current().Color(theme.ColorNameInputBackground, v)
		r.deleteBg.Refresh()
	}
	if r.expandBtn != nil {
		r.expandBtn.Refresh()
	}
	if r.deleteBtn != nil {
		r.deleteBtn.Refresh()
	}

	// Обновляем все лейблы
	labels := []*canvas.Text{
		r.flowLabel, r.headLabel, r.rpmLabel, r.specGravityLabel, r.powerLabel,
		r.conveyorLengthLabel, r.beltWidthLabel,
		r.vesselDiameterLabel, r.vesselTangentToTangentHeightLabel,
		r.designGaugePressureLabel, r.designTemperatureLabel,
		r.skirtHeightLabel, r.vesselLegHeightLabel,
		r.designTangentToTangentLengthLabel,
	}
	for _, l := range labels {
		if l != nil {
			l.Color = theme.ForegroundColor()
			l.Refresh()
		}
	}

	// Стиль кнопок-подложек
	// (Они используют InputBackgroundColor)
	r.container.Refresh()
}

func (r *equipmentRow) markFieldInvalid(e *widget.Entry, label *canvas.Text, hasError bool) {
	if e == nil {
		return
	}
	if hasError {
		e.SetValidationError(fmt.Errorf("invalid"))
	} else {
		e.SetValidationError(nil)
	}
	setLabelError(label, hasError)
}

// collectEquipment собирает данные из виджетов строки
func (r *equipmentRow) collectEquipment() (Equipment, error) {
	r.clearValidation()

	eq := Equipment{
		Type: strings.TrimSpace(r.typeSelect.Selected),
		Tag:  strings.TrimSpace(r.tagEntry.Text),
	}

	if eq.Tag == "" {
		r.markFieldInvalid(r.tagEntry, nil, true)
		return eq, fmt.Errorf("Тэг обязателен")
	}

	q, err := strconv.Atoi(strings.TrimSpace(r.qtyEntry.Text))
	if err != nil || q < 1 {
		r.markFieldInvalid(r.qtyEntry, nil, true)
		return eq, fmt.Errorf("Кол-во должно быть >= 1")
	}
	eq.Quantity = q

	switch eq.Type {
	case "Насосы":
		flow, err := parseOptionalFloat(r.flowEntry.Text)
		if err != nil || flow == nil {
			r.markFieldInvalid(r.flowEntry, r.flowLabel, true)
			return eq, fmt.Errorf("Расход (Flow Rate) обязателен")
		}
		eq.FlowRate = flow

		head, err := parseOptionalFloat(r.headEntry.Text)
		if err != nil || head == nil {
			r.markFieldInvalid(r.headEntry, r.headLabel, true)
			return eq, fmt.Errorf("Напор (Fluid Head) обязателен")
		}
		eq.FluidHead = head

		eq.RPM, err = parseOptionalFloat(r.rpmEntry.Text)
		if err != nil {
			r.markFieldInvalid(r.rpmEntry, r.rpmLabel, true)
		}
		eq.SpecGravity, err = parseOptionalFloat(r.specGravityEntry.Text)
		if err != nil {
			r.markFieldInvalid(r.specGravityEntry, r.specGravityLabel, true)
		}
		eq.PowerKW, err = parseOptionalFloat(r.powerEntry.Text)
		if err != nil {
			r.markFieldInvalid(r.powerEntry, r.powerLabel, true)
		}

	case "Конвейер":
		conveyorLength, err := parseOptionalFloat(r.conveyorLengthEntry.Text)
		if err != nil || conveyorLength == nil {
			r.markFieldInvalid(r.conveyorLengthEntry, r.conveyorLengthLabel, true)
			return eq, fmt.Errorf("Длина конвейера (Conveyor Length) обязательна")
		}
		eq.ConveyorLength = conveyorLength

		beltWidth, err := parseOptionalFloat(r.beltWidthEntry.Text)
		if err != nil || beltWidth == nil {
			r.markFieldInvalid(r.beltWidthEntry, r.beltWidthLabel, true)
			return eq, fmt.Errorf("Ширина ленты (Belt Width) обязательна")
		}
		eq.BeltWidth = beltWidth

	case "Вертикальный аппарат":
		vesselDiameter, err := parseOptionalFloat(r.vesselDiameterEntry.Text)
		if err != nil || vesselDiameter == nil {
			r.markFieldInvalid(r.vesselDiameterEntry, r.vesselDiameterLabel, true)
			return eq, fmt.Errorf("Диаметр аппарата (Vessel Diameter) обязателен")
		}
		eq.VesselDiameter = vesselDiameter

		vesselHeight, err := parseOptionalFloat(r.vesselTangentToTangentHeightEntry.Text)
		if err != nil || vesselHeight == nil {
			r.markFieldInvalid(r.vesselTangentToTangentHeightEntry, r.vesselTangentToTangentHeightLabel, true)
			return eq, fmt.Errorf("Высота tangent-to-tangent обязательна")
		}
		eq.VesselTangentToTangentHeight = vesselHeight

		eq.DesignGaugePressure, err = parseOptionalFloat(r.designGaugePressureEntry.Text)
		if err != nil {
			r.markFieldInvalid(r.designGaugePressureEntry, r.designGaugePressureLabel, true)
		}
		eq.DesignTemperature, err = parseOptionalFloat(r.designTemperatureEntry.Text)
		if err != nil {
			r.markFieldInvalid(r.designTemperatureEntry, r.designTemperatureLabel, true)
		}
		eq.SkirtHeight, err = parseOptionalFloat(r.skirtHeightEntry.Text)
		if err != nil {
			r.markFieldInvalid(r.skirtHeightEntry, r.skirtHeightLabel, true)
		}
		eq.VesselLegHeight, err = parseOptionalFloat(r.vesselLegHeightEntry.Text)
		if err != nil {
			r.markFieldInvalid(r.vesselLegHeightEntry, r.vesselLegHeightLabel, true)
		}

		if eq.SkirtHeight != nil && eq.VesselLegHeight != nil {
			r.markFieldInvalid(r.skirtHeightEntry, r.skirtHeightLabel, true)
			r.markFieldInvalid(r.vesselLegHeightEntry, r.vesselLegHeightLabel, true)
			return eq, fmt.Errorf("Нельзя указывать одновременно высоту юбки и высоту опор")
		}

	case "Горизонтальная емкость":
		vesselDiameter, err := parseOptionalFloat(r.vesselDiameterEntry.Text)
		if err != nil || vesselDiameter == nil {
			r.markFieldInvalid(r.vesselDiameterEntry, r.vesselDiameterLabel, true)
			return eq, fmt.Errorf("Диаметр аппарата (Vessel Diameter) обязателен")
		}
		eq.VesselDiameter = vesselDiameter

		designLength, err := parseOptionalFloat(r.designTangentToTangentLengthEntry.Text)
		if err != nil || designLength == nil {
			r.markFieldInvalid(r.designTangentToTangentLengthEntry, r.designTangentToTangentLengthLabel, true)
			return eq, fmt.Errorf("Длина tangent-to-tangent обязательна")
		}
		eq.DesignTangentToTangentLength = designLength
	}

	return eq, nil
}

// ─── UI: Экраны ──────────────────────────────────────────────

// showStartScreen — начальный экран с кнопкой «Начать»
func showStartScreen(w fyne.Window) {
	title := widget.NewLabel("ConstructMaterialAI")
	title.Alignment = fyne.TextAlignCenter
	title.TextStyle = fyne.TextStyle{Bold: true}

	subtitle := widget.NewLabel("Система учёта веса оборудования")
	subtitle.Alignment = fyne.TextAlignCenter

	startBtn := widget.NewButtonWithIcon("Начать", theme.NavigateNextIcon(), func() {
		showProjectList(w)
	})
	startBtn.Importance = widget.HighImportance

	content := container.NewVBox(
		layout.NewSpacer(),
		container.NewCenter(title),
		container.NewCenter(subtitle),
		widget.NewLabel(""),
		container.NewCenter(startBtn),
		layout.NewSpacer(),
	)

	w.SetContent(container.NewPadded(content))
	w.Resize(fyne.NewSize(windowWidth, windowHeight))
}

// createProjectCard — создает визуальный блок проекта
func createProjectCard(w fyne.Window, proj Project, onOpen func(), onDelete func()) *projectCard {
	card := &projectCard{project: proj}

	title := widget.NewLabelWithStyle(proj.Name, fyne.TextAlignLeading, fyne.TextStyle{Bold: true})
	title.Truncation = fyne.TextTruncateEllipsis

	eqCount := proj.EquipmentCount()
	weight := proj.TotalWeight()

	info := widget.NewLabel(fmt.Sprintf("Оборудование: %d | Вес: %.2f кг", eqCount, weight))
	info.TextStyle = fyne.TextStyle{Italic: true}

	openBtn := widget.NewButtonWithIcon("Открыть", theme.FolderOpenIcon(), onOpen)
	openBtn.Importance = widget.HighImportance

	deleteBtn := widget.NewButtonWithIcon("", theme.DeleteIcon(), onDelete)
	deleteBtn.Importance = widget.LowImportance

	v := fyne.CurrentApp().Settings().ThemeVariant()
	card.bg = canvas.NewRectangle(theme.Current().Color(ColorNameCardBackground, v))
	card.bg.CornerRadius = 12

	card.accent = canvas.NewRectangle(theme.PrimaryColor())
	card.accent.SetMinSize(fyne.NewSize(4, 0))

	content := container.NewPadded(container.NewHBox(
		card.accent,
		container.NewVBox(
			title,
			info,
		),
		layout.NewSpacer(),
		container.NewHBox(openBtn, deleteBtn),
	))

	card.container = container.NewStack(card.bg, content)
	return card
}

// showProjectList — экран выбора / создания проекта
func showProjectList(w fyne.Window) {
	appData := loadProjects()

	title := widget.NewLabel("Менеджер проектов")
	title.Alignment = fyne.TextAlignCenter
	title.TextStyle = fyne.TextStyle{Bold: true}

	// Панель статистики
	totalProjectsLabel := widget.NewLabel("")

	updateStats := func() {
		totalProjectsLabel.SetText(fmt.Sprintf("Всего проектов: %d", len(appData.Projects)))
	}
	updateStats()

	statsBar := container.NewHBox(
		container.NewPadded(totalProjectsLabel),
		layout.NewSpacer(),
	)

	projectList := container.NewVBox()
	cards := []*projectCard{}

	renderProjects := func(filter string) {
		projectList.RemoveAll()
		cards = []*projectCard{}
		filter = strings.ToLower(filter)

		found := false
		for i := range appData.Projects {
			idx := i
			proj := appData.Projects[idx]

			if filter != "" && !strings.Contains(strings.ToLower(proj.Name), filter) {
				continue
			}
			found = true

			card := createProjectCard(w, proj,
				func() {
					showProject(w, proj.Name)
				},
				func() {
					dialog.ShowConfirm("Удалить проект",
						fmt.Sprintf("Удалить проект «%s»?", proj.Name),
						func(ok bool) {
							if !ok {
								return
							}
							appData.Projects = append(appData.Projects[:idx], appData.Projects[idx+1:]...)
							_ = saveProjects(appData)
							showProjectList(w)
						}, w)
				})
			cards = append(cards, card)
			projectList.Add(container.NewPadded(card.container))
		}

		if !found {
			msg := "Нет проектов"
			if filter != "" {
				msg = "Ничего не найдено"
			}
			projectList.Add(container.NewCenter(container.NewVBox(
				widget.NewLabel(""),
				widget.NewLabel(msg),
			)))
		}
		projectList.Refresh()
	}

	searchEntry := widget.NewEntry()
	searchEntry.SetPlaceHolder("Поиск проекта...")
	searchEntry.OnChanged = renderProjects

	renderProjects("")

	createBtn := widget.NewButtonWithIcon("Создать проект", theme.ContentAddIcon(), func() {
		nameEntry := widget.NewEntry()
		nameEntry.SetPlaceHolder("Название проекта")

		dialog.ShowForm("Новый проект", "Создать", "Отмена",
			[]*widget.FormItem{
				widget.NewFormItem("Имя", nameEntry),
			},
			func(ok bool) {
				if !ok || strings.TrimSpace(nameEntry.Text) == "" {
					return
				}
				newProject := Project{
					Name:      strings.TrimSpace(nameEntry.Text),
					Equipment: []Equipment{},
				}
				appData.Projects = append(appData.Projects, newProject)
				if err := saveProjects(appData); err != nil {
					dialog.ShowError(err, w)
					return
				}
				showProjectList(w)
			}, w)
	})
	createBtn.Importance = widget.HighImportance

	backBtn := widget.NewButtonWithIcon("Назад", theme.NavigateBackIcon(), func() {
		showStartScreen(w)
	})

	themeBtn := widget.NewButtonWithIcon("", theme.ColorPaletteIcon(), func() {
		current := fyne.CurrentApp().Settings().Theme()
		if m, ok := current.(*modernTheme); ok && m.variant == theme.VariantDark {
			fyne.CurrentApp().Settings().SetTheme(newModernLightTheme())
		} else {
			fyne.CurrentApp().Settings().SetTheme(newModernDarkTheme())
		}
		// Обновляем карточки
		for _, c := range cards {
			c.refreshTheme()
		}
		w.Content().Refresh()
	})

	scrollable := container.NewVScroll(projectList)
	scrollable.SetMinSize(fyne.NewSize(600, 400))

	header := container.NewVBox(
		container.NewHBox(backBtn, layout.NewSpacer(), themeBtn),
		title,
		statsBar,
		container.NewPadded(searchEntry),
		widget.NewSeparator(),
	)

	content := container.NewBorder(
		header,
		container.NewPadded(createBtn),
		nil, nil,
		scrollable,
	)

	w.SetContent(container.NewPadded(content))
	w.Resize(fyne.NewSize(windowWidth, windowHeight))
}

func createLabel(text string, required bool) (*canvas.Text, fyne.CanvasObject) {
	t := canvas.NewText(text, theme.ForegroundColor())
	t.TextSize = theme.TextSize() + 1 // Чуть больше
	t.TextStyle.Bold = true           // Жирнее

	var finalObj fyne.CanvasObject
	if !required {
		finalObj = t
	} else {
		ast := canvas.NewText(" *", color.NRGBA{R: 255, G: 0, B: 0, A: 255})
		ast.TextSize = t.TextSize
		ast.TextStyle.Bold = true
		finalObj = container.NewHBox(t, ast)
	}

	// Оборачиваем в контейнер с фиксированной шириной для выравнивания во всех карточках
	return t, container.NewGridWrap(fyne.NewSize(260, 40), finalObj)
}

func setLabelError(t *canvas.Text, hasError bool) {
	if t == nil {
		return
	}
	fyne.Do(func() {
		t.Color = theme.ForegroundColor()
		if hasError {
			t.TextStyle.Bold = true
		} else {
			// Для обычных полей оставляем Bold=true, как в createLabel,
			// чтобы не "прыгал" текст при валидации.
			t.TextStyle.Bold = true
		}
		t.Refresh()
	})
}

func (r *equipmentRow) clearValidation() {
	// Очистка валидации для всех полей
	entries := []*widget.Entry{
		r.tagEntry, r.qtyEntry, r.flowEntry, r.headEntry, r.rpmEntry,
		r.specGravityEntry, r.powerEntry, r.conveyorLengthEntry, r.beltWidthEntry,
		r.vesselDiameterEntry, r.designTangentToTangentLengthEntry,
		r.vesselTangentToTangentHeightEntry, r.designGaugePressureEntry,
		r.designTemperatureEntry, r.skirtHeightEntry, r.vesselLegHeightEntry,
	}
	for _, e := range entries {
		if e != nil {
			e.SetValidationError(nil)
		}
	}

	setLabelError(r.flowLabel, false)
	setLabelError(r.headLabel, false)
	setLabelError(r.rpmLabel, false)
	setLabelError(r.specGravityLabel, false)
	setLabelError(r.powerLabel, false)
	setLabelError(r.conveyorLengthLabel, false)
	setLabelError(r.beltWidthLabel, false)
	setLabelError(r.vesselDiameterLabel, false)
	setLabelError(r.designTangentToTangentLengthLabel, false)
	setLabelError(r.vesselTangentToTangentHeightLabel, false)
	setLabelError(r.designGaugePressureLabel, false)
	setLabelError(r.designTemperatureLabel, false)
	setLabelError(r.skirtHeightLabel, false)
	setLabelError(r.vesselLegHeightLabel, false)
}

func buildPumpFields(row *equipmentRow) *fyne.Container {
	flowLabel, flowObj := createLabel("Расход (м³/ч):", true)
	row.flowLabel = flowLabel
	row.flowEntry = widget.NewEntry()
	row.flowEntry.SetPlaceHolder("Введите расход (обязательно)")
	row.flowEntry.OnChanged = func(s string) {
		val, err := parseOptionalFloat(s)
		row.markFieldInvalid(row.flowEntry, row.flowLabel, err != nil || val == nil)
	}

	headLabel, headObj := createLabel("Напор (м):", true)
	row.headLabel = headLabel
	row.headEntry = widget.NewEntry()
	row.headEntry.SetPlaceHolder("Введите напор (обязательно)")
	row.headEntry.OnChanged = func(s string) {
		val, err := parseOptionalFloat(s)
		row.markFieldInvalid(row.headEntry, row.headLabel, err != nil || val == nil)
	}

	rpmLabel, rpmObj := createLabel("Частота вращения (об/мин):", false)
	row.rpmLabel = rpmLabel
	row.rpmEntry = widget.NewEntry()
	row.rpmEntry.SetPlaceHolder("Частота вращения (об/мин)")
	row.rpmEntry.OnChanged = func(s string) {
		_, err := parseOptionalFloat(s)
		row.markFieldInvalid(row.rpmEntry, row.rpmLabel, err != nil)
	}

	specGravityLabel, specGravityObj := createLabel("Удельный вес:", false)
	row.specGravityLabel = specGravityLabel
	row.specGravityEntry = widget.NewEntry()
	row.specGravityEntry.SetPlaceHolder("Удельный вес")
	row.specGravityEntry.OnChanged = func(s string) {
		_, err := parseOptionalFloat(s)
		row.markFieldInvalid(row.specGravityEntry, row.specGravityLabel, err != nil)
	}

	powerLabel, powerObj := createLabel("Мощность (кВт):", false)
	row.powerLabel = powerLabel
	row.powerEntry = widget.NewEntry()
	row.powerEntry.SetPlaceHolder("Мощность (кВт)")
	row.powerEntry.OnChanged = func(s string) {
		_, err := parseOptionalFloat(s)
		row.markFieldInvalid(row.powerEntry, row.powerLabel, err != nil)
	}

	return container.New(layout.NewFormLayout(),
		flowObj, row.flowEntry,
		headObj, row.headEntry,
		rpmObj, row.rpmEntry,
		specGravityObj, row.specGravityEntry,
		powerObj, row.powerEntry,
	)
}

func buildConveyorFields(row *equipmentRow) *fyne.Container {
	conveyorLengthLabel, conveyorLengthObj := createLabel("Длина конвейера (м):", true)
	row.conveyorLengthLabel = conveyorLengthLabel
	row.conveyorLengthEntry = widget.NewEntry()
	row.conveyorLengthEntry.SetPlaceHolder("Введите длину (обязательно)")
	row.conveyorLengthEntry.OnChanged = func(s string) {
		val, err := parseOptionalFloat(s)
		row.markFieldInvalid(row.conveyorLengthEntry, row.conveyorLengthLabel, err != nil || val == nil)
	}

	beltWidthLabel, beltWidthObj := createLabel("Ширина ленты (мм):", true)
	row.beltWidthLabel = beltWidthLabel
	row.beltWidthEntry = widget.NewEntry()
	row.beltWidthEntry.SetPlaceHolder("Введите ширину (обязательно)")
	row.beltWidthEntry.OnChanged = func(s string) {
		val, err := parseOptionalFloat(s)
		row.markFieldInvalid(row.beltWidthEntry, row.beltWidthLabel, err != nil || val == nil)
	}

	return container.New(layout.NewFormLayout(),
		conveyorLengthObj, row.conveyorLengthEntry,
		beltWidthObj, row.beltWidthEntry,
	)
}

func buildVesselFields(row *equipmentRow) *fyne.Container {
	vesselDiameterLabel, vesselDiameterObj := createLabel("Диаметр аппарата (мм):", true)
	row.vesselDiameterLabel = vesselDiameterLabel
	row.vesselDiameterEntry = widget.NewEntry()
	row.vesselDiameterEntry.SetPlaceHolder("Введите диаметр (обязательно)")
	row.vesselDiameterEntry.OnChanged = func(s string) {
		val, err := parseOptionalFloat(s)
		row.markFieldInvalid(row.vesselDiameterEntry, row.vesselDiameterLabel, err != nil || val == nil)
	}

	vesselTangentToTangentHeightLabel, vesselTangentToTangentHeightObj := createLabel("Высота (T/T, мм):", true)
	row.vesselTangentToTangentHeightLabel = vesselTangentToTangentHeightLabel
	row.vesselTangentToTangentHeightEntry = widget.NewEntry()
	row.vesselTangentToTangentHeightEntry.SetPlaceHolder("Введите высоту (обязательно)")
	row.vesselTangentToTangentHeightEntry.OnChanged = func(s string) {
		val, err := parseOptionalFloat(s)
		row.markFieldInvalid(row.vesselTangentToTangentHeightEntry, row.vesselTangentToTangentHeightLabel, err != nil || val == nil)
	}

	designGaugePressureLabel, designGaugePressureObj := createLabel("Давление (МПа):", false)
	row.designGaugePressureLabel = designGaugePressureLabel
	row.designGaugePressureEntry = widget.NewEntry()
	row.designGaugePressureEntry.SetPlaceHolder("Давление")
	row.designGaugePressureEntry.OnChanged = func(s string) {
		_, err := parseOptionalFloat(s)
		row.markFieldInvalid(row.designGaugePressureEntry, row.designGaugePressureLabel, err != nil)
	}

	designTemperatureLabel, designTemperatureObj := createLabel("Температура (°C):", false)
	row.designTemperatureLabel = designTemperatureLabel
	row.designTemperatureEntry = widget.NewEntry()
	row.designTemperatureEntry.SetPlaceHolder("Температура")
	row.designTemperatureEntry.OnChanged = func(s string) {
		_, err := parseOptionalFloat(s)
		row.markFieldInvalid(row.designTemperatureEntry, row.designTemperatureLabel, err != nil)
	}

	skirtHeightLabel, skirtHeightObj := createLabel("Высота юбки (мм):", false)
	row.skirtHeightLabel = skirtHeightLabel
	row.skirtHeightEntry = widget.NewEntry()
	row.skirtHeightEntry.SetPlaceHolder("Высота юбки")
	row.skirtHeightEntry.OnChanged = func(s string) {
		_, err := parseOptionalFloat(s)
		row.markFieldInvalid(row.skirtHeightEntry, row.skirtHeightLabel, err != nil)
	}

	vesselLegHeightLabel, vesselLegHeightObj := createLabel("Высота опор (мм):", false)
	row.vesselLegHeightLabel = vesselLegHeightLabel
	row.vesselLegHeightEntry = widget.NewEntry()
	row.vesselLegHeightEntry.SetPlaceHolder("Высота опор")
	row.vesselLegHeightEntry.OnChanged = func(s string) {
		_, err := parseOptionalFloat(s)
		row.markFieldInvalid(row.vesselLegHeightEntry, row.vesselLegHeightLabel, err != nil)
	}

	return container.New(layout.NewFormLayout(),
		vesselDiameterObj, row.vesselDiameterEntry,
		vesselTangentToTangentHeightObj, row.vesselTangentToTangentHeightEntry,
		designGaugePressureObj, row.designGaugePressureEntry,
		designTemperatureObj, row.designTemperatureEntry,
		skirtHeightObj, row.skirtHeightEntry,
		vesselLegHeightObj, row.vesselLegHeightEntry,
	)
}

func buildDrumFields(row *equipmentRow) *fyne.Container {
	vesselDiameterLabel, vesselDiameterObj := createLabel("Диаметр аппарата (мм):", true)
	row.vesselDiameterLabel = vesselDiameterLabel
	row.vesselDiameterEntry = widget.NewEntry()
	row.vesselDiameterEntry.SetPlaceHolder("Введите диаметр (обязательно)")
	row.vesselDiameterEntry.OnChanged = func(s string) {
		val, err := parseOptionalFloat(s)
		row.markFieldInvalid(row.vesselDiameterEntry, row.vesselDiameterLabel, err != nil || val == nil)
	}

	designTangentToTangentLengthLabel, designTangentToTangentLengthObj := createLabel("Длина (T/T, мм):", true)
	row.designTangentToTangentLengthLabel = designTangentToTangentLengthLabel
	row.designTangentToTangentLengthEntry = widget.NewEntry()
	row.designTangentToTangentLengthEntry.SetPlaceHolder("Введите длину (обязательно)")
	row.designTangentToTangentLengthEntry.OnChanged = func(s string) {
		val, err := parseOptionalFloat(s)
		row.markFieldInvalid(row.designTangentToTangentLengthEntry, row.designTangentToTangentLengthLabel, err != nil || val == nil)
	}

	return container.New(layout.NewFormLayout(),
		vesselDiameterObj, row.vesselDiameterEntry,
		designTangentToTangentLengthObj, row.designTangentToTangentLengthEntry,
	)
}

// showProject — главное окно проекта с динамическим списком оборудования
func showProject(w fyne.Window, projectName string) {
	appData := loadProjects()

	var projIdx int = -1
	for i, p := range appData.Projects {
		if p.Name == projectName {
			projIdx = i
			break
		}
	}
	if projIdx == -1 {
		dialog.ShowError(fmt.Errorf("проект '%s' не найден", projectName), w)
		return
	}

	title := widget.NewLabel(fmt.Sprintf("Проект: %s", projectName))
	title.Alignment = fyne.TextAlignCenter
	title.TextStyle = fyne.TextStyle{Bold: true}

	rowsContainer := container.NewVBox()
	var rows []*equipmentRow

	totalWeightLabel := widget.NewLabel("—")
	totalWeightLabel.TextStyle = fyne.TextStyle{Bold: true}
	totalWeightLabel.Alignment = fyne.TextAlignLeading
	byTypeLabel := widget.NewLabel("По типам: —")

	recalcAll := func() {
		var grandTotal float64
		typeWeights := make(map[string]float64)

		// Чтение данных из виджетов должно происходить в основном потоке
		fyne.DoAndWait(func() {
			for _, r := range rows {
				eq, err := r.collectEquipment()
				if err != nil {
					continue
				}

				var unitWeight float64
				text := r.resultLabel.Text
				if strings.HasSuffix(text, " кг/ед.") {
					text = strings.TrimSuffix(text, " кг/ед.")
					text = strings.TrimPrefix(text, "✓ ")
					unitWeight, _ = strconv.ParseFloat(text, 64)
				}

				lineTotal := unitWeight * float64(eq.Quantity)
				grandTotal += lineTotal
				if unitWeight > 0 {
					typeWeights[eq.Type] += lineTotal
				}
			}
		})

		fyne.Do(func() {
			totalWeightLabel.SetText(fmt.Sprintf("%.2f кг", grandTotal))

			var parts []string
			for _, t := range equipmentTypes {
				if ww, ok := typeWeights[t]; ok && ww > 0 {
					parts = append(parts, fmt.Sprintf("%s: %.2f кг", t, ww))
				}
			}
			if len(parts) > 0 {
				byTypeLabel.SetText("По типам: " + strings.Join(parts, " | "))
			} else {
				byTypeLabel.SetText("По типам: —")
			}
		})
	}

	removeRow := func(target *equipmentRow) {
		newRows := make([]*equipmentRow, 0, len(rows))
		for _, r := range rows {
			if r != target {
				newRows = append(newRows, r)
			}
		}
		rows = newRows
		rowsContainer.Remove(target.container)
		rowsContainer.Refresh()
		recalcAll()
	}

	buildFieldsByType := func(row *equipmentRow, eqType string) *fyne.Container {
		switch eqType {
		case "Насосы":
			return buildPumpFields(row)
		case "Конвейер":
			return buildConveyorFields(row)
		case "Вертикальный аппарат":
			return buildVesselFields(row)
		case "Горизонтальная емкость":
			return buildDrumFields(row)
		default:
			return container.NewVBox(widget.NewLabel("Неизвестный тип оборудования"))
		}
	}

	addEquipmentRow := func(eq Equipment) {
		row := &equipmentRow{}

		row.typeSelect = widget.NewSelect(equipmentTypes, nil)
		if eq.Type != "" {
			row.typeSelect.SetSelected(eq.Type)
		} else {
			row.typeSelect.SetSelectedIndex(0)
		}

		row.tagEntry = widget.NewEntry()
		row.tagEntry.SetPlaceHolder("Тэг / Имя")
		row.tagEntry.SetText(eq.Tag)
		row.tagEntry.OnChanged = func(s string) {
			row.markFieldInvalid(row.tagEntry, nil, strings.TrimSpace(s) == "")
		}
		tagContainer := container.NewGridWrap(fyne.NewSize(150, 40), row.tagEntry)

		row.qtyEntry = widget.NewEntry()
		row.qtyEntry.SetPlaceHolder("Кол-во")
		if eq.Quantity > 0 {
			row.qtyEntry.SetText(strconv.Itoa(eq.Quantity))
		} else {
			row.qtyEntry.SetText("1")
		}
		row.qtyEntry.OnChanged = func(s string) {
			q, err := strconv.Atoi(strings.TrimSpace(s))
			row.markFieldInvalid(row.qtyEntry, nil, err != nil || q < 1)
		}

		row.resultLabel = widget.NewLabel("—")
		row.resultLabel.TextStyle = fyne.TextStyle{Bold: true}

		if eq.CalculatedWeight > 0 {
			row.resultLabel.SetText(fmt.Sprintf("%.2f кг/ед.", eq.CalculatedWeight))
		}

		currentType := row.typeSelect.Selected
		row.fieldsContainer = buildFieldsByType(row, currentType)
		qtyContainer := container.NewGridWrap(fyne.NewSize(70, 40), row.qtyEntry)

		switch currentType {
		case "Насосы":
			row.flowEntry.SetText(floatPtrToStr(eq.FlowRate))
			row.headEntry.SetText(floatPtrToStr(eq.FluidHead))
			row.rpmEntry.SetText(floatPtrToStr(eq.RPM))
			row.specGravityEntry.SetText(floatPtrToStr(eq.SpecGravity))
			row.powerEntry.SetText(floatPtrToStr(eq.PowerKW))
		case "Конвейер":
			row.conveyorLengthEntry.SetText(floatPtrToStr(eq.ConveyorLength))
			row.beltWidthEntry.SetText(floatPtrToStr(eq.BeltWidth))
		case "Вертикальный аппарат":
			row.vesselDiameterEntry.SetText(floatPtrToStr(eq.VesselDiameter))
			row.vesselTangentToTangentHeightEntry.SetText(floatPtrToStr(eq.VesselTangentToTangentHeight))
			row.designGaugePressureEntry.SetText(floatPtrToStr(eq.DesignGaugePressure))
			row.designTemperatureEntry.SetText(floatPtrToStr(eq.DesignTemperature))
			row.skirtHeightEntry.SetText(floatPtrToStr(eq.SkirtHeight))
			row.vesselLegHeightEntry.SetText(floatPtrToStr(eq.VesselLegHeight))
		case "Горизонтальная емкость":
			row.vesselDiameterEntry.SetText(floatPtrToStr(eq.VesselDiameter))
			row.designTangentToTangentLengthEntry.SetText(floatPtrToStr(eq.DesignTangentToTangentLength))
		}

		calcBtn := widget.NewButtonWithIcon("Рассчитать", theme.ConfirmIcon(), func() {
			eqData, err := row.collectEquipment()
			if err != nil {
				row.resultLabel.SetText("⚠ " + err.Error())
				return
			}

			fyne.Do(func() {
				row.resultLabel.SetText("⏳ Расчёт...")
				row.resultLabel.Refresh()
			})

			go func() {
				weight, err := sendEquipmentToBackend(eqData)
				fyne.Do(func() {
					if err != nil {
						row.resultLabel.SetText("✗ " + err.Error())
					} else {
						row.resultLabel.SetText(fmt.Sprintf("%.2f кг/ед.", weight))
					}
					row.resultLabel.Refresh()
					recalcAll()
				})
			}()
		})
		calcBtn.Importance = widget.HighImportance

		row.deleteBtn = widget.NewButtonWithIcon("", theme.DeleteIcon(), func() {
			removeRow(row)
		})
		row.deleteBtn.Importance = widget.LowImportance

		row.deleteBg = canvas.NewRectangle(theme.InputBackgroundColor())
		row.deleteBg.CornerRadius = theme.InputRadiusSize()
		styledDeleteBtn := container.NewStack(row.deleteBg, row.deleteBtn)

		row.typeSelect.OnChanged = func(selected string) {
			newFields := buildFieldsByType(row, selected)

			row.fieldsContainer.RemoveAll()
			for _, obj := range newFields.Objects {
				row.fieldsContainer.Add(obj)
			}
			row.fieldsContainer.Show()
			row.expandBtn.SetIcon(theme.MenuDropUpIcon())
			row.fieldsContainer.Refresh()
			row.resultLabel.SetText("—")
			recalcAll()
		}

		row.expandBtn = widget.NewButtonWithIcon("", theme.MenuDropUpIcon(), func() {
			if row.fieldsContainer.Visible() {
				row.fieldsContainer.Hide()
				row.expandBtn.SetIcon(theme.MenuDropDownIcon())
			} else {
				row.fieldsContainer.Show()
				row.expandBtn.SetIcon(theme.MenuDropUpIcon())
			}
			row.container.Refresh()
		})
		row.expandBtn.Importance = widget.LowImportance

		// Стилизуем кнопку сворачивания
		row.expandBg = canvas.NewRectangle(theme.InputBackgroundColor())
		row.expandBg.CornerRadius = theme.InputRadiusSize()
		styledExpandBtn := container.NewGridWrap(fyne.NewSize(40, 40), container.NewStack(row.expandBg, row.expandBtn))

		// Создаем "карточный" заголовок
		weightHeaderLabel := widget.NewLabelWithStyle("Вес единицы:", fyne.TextAlignTrailing, fyne.TextStyle{Bold: true})

		topRow := container.NewHBox(
			styledExpandBtn,
			container.NewGridWrap(fyne.NewSize(180, 40), row.typeSelect),
			tagContainer,
			container.NewHBox(widget.NewLabel("Кол-во:"), qtyContainer),
			layout.NewSpacer(),
			container.NewHBox(
				weightHeaderLabel,
				row.resultLabel,
			),
			container.NewGridWrap(fyne.NewSize(140, 40), calcBtn),
			container.NewGridWrap(fyne.NewSize(40, 40), styledDeleteBtn),
		)

		// Настройка внешнего вида результата
		row.resultLabel.Alignment = fyne.TextAlignTrailing
		row.resultLabel.TextStyle = fyne.TextStyle{Bold: true}

		// Оборачиваем в карточку с фоном и акцентом
		v := fyne.CurrentApp().Settings().ThemeVariant()
		row.cardBg = canvas.NewRectangle(theme.Current().Color(ColorNameCardBackground, v))
		row.cardBg.CornerRadius = 12

		row.accentBar = canvas.NewRectangle(theme.PrimaryColor())
		row.accentBar.SetMinSize(fyne.NewSize(4, 0))

		content := container.NewPadded(container.NewVBox(
			topRow,
			container.NewPadded(row.fieldsContainer),
		))

		cardContent := container.NewHBox(row.accentBar, content)

		row.container = container.NewStack(
			row.cardBg,
			cardContent,
		)

		// Добавим отступы между карточками
		row.container = container.NewPadded(row.container)

		rows = append(rows, row)
		rowsContainer.Add(row.container)
		rowsContainer.Refresh()
	}

	for _, eq := range appData.Projects[projIdx].Equipment {
		addEquipmentRow(eq)
	}

	addBtn := widget.NewButtonWithIcon("Добавить единицу оборудования", theme.ContentAddIcon(), func() {
		addEquipmentRow(Equipment{})
	})
	addBtn.Importance = widget.HighImportance

	calcAllBtn := widget.NewButtonWithIcon("Рассчитать всё", theme.ComputerIcon(), func() {
		fyne.Do(func() {
			totalWeightLabel.SetText("⏳ Расчёт...")
		})

		go func() {
			for _, r := range rows {
				var eq Equipment
				var err error
				fyne.DoAndWait(func() {
					eq, err = r.collectEquipment()
					if err == nil {
						r.resultLabel.SetText("⏳...")
						r.resultLabel.Refresh()
					}
				})
				if err != nil {
					continue
				}

				weight, err := sendEquipmentToBackend(eq)
				fyne.Do(func() {
					if err != nil {
						r.resultLabel.SetText("✗ " + err.Error())
					} else {
						r.resultLabel.SetText(fmt.Sprintf("%.2f кг/ед.", weight))
					}
					r.resultLabel.Refresh()
				})
			}
			recalcAll()
		}()
	})
	calcAllBtn.Importance = widget.HighImportance

	saveBtn := widget.NewButtonWithIcon("Сохранить проект", theme.DocumentSaveIcon(), func() {
		var equipment []Equipment
		for _, r := range rows {
			eq, err := r.collectEquipment()
			if err != nil {
				continue
			}
			text := r.resultLabel.Text
			if strings.HasSuffix(text, " кг/ед.") {
				text = strings.TrimSuffix(text, " кг/ед.")
				eq.CalculatedWeight, _ = strconv.ParseFloat(text, 64)
			}
			equipment = append(equipment, eq)
		}
		appData.Projects[projIdx].Equipment = equipment
		if err := saveProjects(appData); err != nil {
			dialog.ShowError(err, w)
			return
		}
		dialog.ShowInformation("Сохранено", "Проект успешно сохранён.", w)
	})

	backBtn := widget.NewButtonWithIcon("Назад к проектам", theme.NavigateBackIcon(), func() {
		showProjectList(w)
	})

	scrollable := container.NewVScroll(rowsContainer)
	scrollable.SetMinSize(fyne.NewSize(800, 400))

	footer := container.NewVBox(
		widget.NewSeparator(),
		container.NewHBox(
			widget.NewLabelWithStyle("Итоговый вес проекта:", fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
			totalWeightLabel,
		),
		byTypeLabel,
	)

	collapseAllBtn := widget.NewButtonWithIcon("Свернуть всё", theme.MenuDropDownIcon(), func() {
		for _, r := range rows {
			r.fieldsContainer.Hide()
			r.expandBtn.SetIcon(theme.MenuDropDownIcon())
			r.container.Refresh()
		}
	})

	expandAllBtn := widget.NewButtonWithIcon("Развернуть всё", theme.MenuDropUpIcon(), func() {
		for _, r := range rows {
			r.fieldsContainer.Show()
			r.expandBtn.SetIcon(theme.MenuDropUpIcon())
			r.container.Refresh()
		}
	})

	// Кнопка Help (инструкция)
	helpBtn := widget.NewButtonWithIcon("", theme.QuestionIcon(), func() {
		instructionText := `Инструкция по работе с шаблоном Excel:

1. Нажмите «Шаблон» для скачивания пустого файла.
2. Заполните соответствующие листы шаблона.
3. Не меняйте порядок колонок.
4. Обязательные поля отмечены в шаблоне красным цветом.
5. Нажмите «Импорт» и выберите заполненный файл.

Поддерживаемые типы оборудования:
  • Насосы
  • Конвейер
  • Вертикальные аппараты
  • Горизонтальные емкости

При импорте данные добавляются к текущему проекту.`
		dialog.ShowInformation("Справка: Импорт/Экспорт", instructionText, w)
	})

	// Кнопка «Шаблон»
	templateBtn := widget.NewButtonWithIcon("Шаблон", theme.DownloadIcon(), func() {
		saveDialog := dialog.NewFileSave(func(writer fyne.URIWriteCloser, err error) {
			if err != nil {
				dialog.ShowError(err, w)
				return
			}
			if writer == nil {
				return // пользователь отменил
			}
			filePath := writer.URI().Path()
			writer.Close()
			// Удаляем пустой файл, созданный Fyne, чтобы excelize мог записать свой
			os.Remove(filePath)

			if err := generateTemplate(filePath); err != nil {
				dialog.ShowError(fmt.Errorf("Ошибка создания шаблона: %w", err), w)
				return
			}
			dialog.ShowInformation("Готово", "Шаблон успешно сохранён.", w)
		}, w)
		saveDialog.SetFileName("шаблон_оборудования.xlsx")
		saveDialog.SetFilter(storage.NewExtensionFileFilter([]string{".xlsx"}))
		saveDialog.Show()
	})

	// Кнопка «Импорт»
	importBtn := widget.NewButtonWithIcon("Импорт", theme.FolderOpenIcon(), func() {
		openDialog := dialog.NewFileOpen(func(reader fyne.URIReadCloser, err error) {
			if err != nil {
				dialog.ShowError(err, w)
				return
			}
			if reader == nil {
				return // пользователь отменил
			}
			filePath := reader.URI().Path()
			reader.Close()

			imported, importErrors := importProject(filePath)

			// Добавляем импортированное оборудование
			for _, eq := range imported {
				addEquipmentRow(eq)
			}

			// Показываем отчёт
			if len(importErrors) > 0 {
				var report strings.Builder
				report.WriteString(fmt.Sprintf("Успешно импортировано: %d записей.\n\n", len(imported)))
				report.WriteString("Ошибки при импорте:\n")
				for _, ie := range importErrors {
					report.WriteString("• " + ie.String() + "\n")
				}
				dialog.ShowInformation("Результат импорта", report.String(), w)
			} else if len(imported) > 0 {
				dialog.ShowInformation("Импорт завершён",
					fmt.Sprintf("Успешно импортировано: %d записей.", len(imported)), w)
			} else {
				dialog.ShowInformation("Импорт", "Файл не содержит данных для импорта.", w)
			}
		}, w)
		openDialog.SetFilter(storage.NewExtensionFileFilter([]string{".xlsx"}))
		openDialog.Show()
	})

	// Кнопка «Экспорт»
	exportBtn := widget.NewButtonWithIcon("Экспорт", theme.DocumentCreateIcon(), func() {
		// Собираем оборудование из строк
		var equipment []Equipment
		hasUncalculated := false
		for _, r := range rows {
			eq, err := r.collectEquipment()
			if err != nil {
				continue
			}
			text := r.resultLabel.Text
			if strings.HasSuffix(text, " кг/ед.") {
				text = strings.TrimSuffix(text, " кг/ед.")
				text = strings.TrimPrefix(text, "✓ ")
				eq.CalculatedWeight, _ = strconv.ParseFloat(text, 64)
			} else {
				hasUncalculated = true
			}
			equipment = append(equipment, eq)
		}

		if len(equipment) == 0 {
			dialog.ShowInformation("Экспорт", "Нет данных для экспорта.", w)
			return
		}

		doExport := func(eqList []Equipment) {
			saveDialog := dialog.NewFileSave(func(writer fyne.URIWriteCloser, err error) {
				if err != nil {
					dialog.ShowError(err, w)
					return
				}
				if writer == nil {
					return
				}
				filePath := writer.URI().Path()
				writer.Close()
				os.Remove(filePath)

				if err := exportProject(filePath, eqList); err != nil {
					dialog.ShowError(fmt.Errorf("Ошибка экспорта: %w", err), w)
					return
				}
				dialog.ShowInformation("Готово", "Данные успешно экспортированы.", w)
			}, w)
			saveDialog.SetFileName(projectName + ".xlsx")
			saveDialog.SetFilter(storage.NewExtensionFileFilter([]string{".xlsx"}))
			saveDialog.Show()
		}

		if hasUncalculated {
			// Диалог с двумя кнопками: продолжить или рассчитать
			continueBtn := widget.NewButton("Всё равно продолжить", func() {
				// Закрываем текущий диалог — просто вызываем экспорт
				doExport(equipment)
			})
			calcBtn := widget.NewButton("Провести расчёт", func() {
				// Запускаем расчёт всех строк
				fyne.Do(func() {
					totalWeightLabel.SetText("⏳ Расчёт...")
				})
				go func() {
					for _, r := range rows {
						var eq Equipment
						var err error
						fyne.DoAndWait(func() {
							eq, err = r.collectEquipment()
							if err == nil {
								r.resultLabel.SetText("⏳...")
								r.resultLabel.Refresh()
							}
						})
						if err != nil {
							continue
						}

						weight, err := sendEquipmentToBackend(eq)
						fyne.Do(func() {
							if err != nil {
								r.resultLabel.SetText("✗ " + err.Error())
							} else {
								r.resultLabel.SetText(fmt.Sprintf("✓ %.2f кг/ед.", weight))
							}
							r.resultLabel.Refresh()
						})
					}
					recalcAll()

					// После расчёта — собираем заново и экспортируем
					var updatedEquipment []Equipment
					for _, r := range rows {
						eq, err := r.collectEquipment()
						if err != nil {
							continue
						}
						text := r.resultLabel.Text
						if strings.HasSuffix(text, " кг/ед.") {
							text = strings.TrimSuffix(text, " кг/ед.")
							text = strings.TrimPrefix(text, "✓ ")
							eq.CalculatedWeight, _ = strconv.ParseFloat(text, 64)
						}
						updatedEquipment = append(updatedEquipment, eq)
					}
					doExport(updatedEquipment)
				}()
			})
			calcBtn.Importance = widget.HighImportance

			warningContent := container.NewVBox(
				widget.NewLabel("Не у всех строк рассчитан вес.\nКолонка «Вес» будет пустой для нерассчитанных строк."),
				container.NewHBox(continueBtn, calcBtn),
			)

			warningDialog := dialog.NewCustomWithoutButtons("Предупреждение", warningContent, w)
			// Переопределяем кнопки для закрытия диалога
			continueBtn.OnTapped = func() {
				warningDialog.Hide()
				doExport(equipment)
			}
			calcBtn.OnTapped = func() {
				warningDialog.Hide()
				fyne.Do(func() {
					totalWeightLabel.SetText("⏳ Расчёт...")
				})
				go func() {
					for _, r := range rows {
						var eq Equipment
						var err error
						fyne.DoAndWait(func() {
							eq, err = r.collectEquipment()
							if err == nil {
								r.resultLabel.SetText("⏳...")
								r.resultLabel.Refresh()
							}
						})
						if err != nil {
							continue
						}

						weight, err := sendEquipmentToBackend(eq)
						fyne.Do(func() {
							if err != nil {
								r.resultLabel.SetText("✗ " + err.Error())
							} else {
								r.resultLabel.SetText(fmt.Sprintf("%.2f кг/ед.", weight))
							}
							r.resultLabel.Refresh()
						})
					}
					recalcAll()

					var updatedEquipment []Equipment
					for _, r := range rows {
						eq, err := r.collectEquipment()
						if err != nil {
							continue
						}
						text := r.resultLabel.Text
						if strings.HasSuffix(text, " кг/ед.") {
							text = strings.TrimSuffix(text, " кг/ед.")
							text = strings.TrimPrefix(text, "✓ ")
							eq.CalculatedWeight, _ = strconv.ParseFloat(text, 64)
						}
						updatedEquipment = append(updatedEquipment, eq)
					}
					doExport(updatedEquipment)
				}()
			}
			warningDialog.Show()
		} else {
			doExport(equipment)
		}
	})

	themeBtn := widget.NewButtonWithIcon("", theme.ColorPaletteIcon(), func() {
		current := fyne.CurrentApp().Settings().Theme()
		if m, ok := current.(*modernTheme); ok && m.variant == theme.VariantDark {
			fyne.CurrentApp().Settings().SetTheme(newModernLightTheme())
		} else {
			fyne.CurrentApp().Settings().SetTheme(newModernDarkTheme())
		}

		// Обновляем все существующие строки оборудования
		for _, r := range rows {
			r.refreshTheme()
		}
		w.Content().Refresh()
	})

	toolbarTop := container.NewHBox(
		backBtn,
		layout.NewSpacer(),
		themeBtn,
		helpBtn,
	)

	toolbarActions := container.NewHBox(
		templateBtn,
		importBtn,
		exportBtn,
		widget.NewSeparator(),
		collapseAllBtn,
		expandAllBtn,
		layout.NewSpacer(),
		addBtn,
	)

	bottomButtons := container.NewHBox(
		calcAllBtn,
		saveBtn,
	)

	content := container.NewBorder(
		container.NewVBox(toolbarTop, toolbarActions, title, widget.NewSeparator()),
		container.NewVBox(footer, bottomButtons),
		nil, nil,
		scrollable,
	)

	w.SetContent(container.NewPadded(content))
	w.Resize(fyne.NewSize(windowWidth, windowHeight))
}

func main() {
	fmt.Println("Запуск десктопного приложения...")

	myApp := app.New()
	myApp.Settings().SetTheme(newModernDarkTheme())
	myWindow := myApp.NewWindow("ConstructMaterialAI: Учёт оборудования")
	myWindow.Resize(fyne.NewSize(windowWidth, windowHeight))

	showStartScreen(myWindow)

	myWindow.CenterOnScreen()
	myWindow.ShowAndRun()

	fmt.Println("Приложение закрыто.")
}
