package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/layout"
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

	// Рассчитанный вес за единицу
	CalculatedWeight float64 `json:"calculated_weight"`
}

// Project — проект со списком оборудования
type Project struct {
	Name      string      `json:"name"`
	Equipment []Equipment `json:"equipment"`
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

// ─── UI: строка оборудования ─────────────────────────────────

// equipmentRow хранит ссылки на виджеты одной строки
type equipmentRow struct {
	// Общие
	typeSelect *widget.Select
	tagEntry   *widget.Entry
	qtyEntry   *widget.Entry

	// Насос
	flowEntry        *widget.Entry
	headEntry        *widget.Entry
	rpmEntry         *widget.Entry
	specGravityEntry *widget.Entry
	powerEntry       *widget.Entry

	// Конвейер
	conveyorLengthEntry *widget.Entry
	beltWidthEntry      *widget.Entry

	// Vessel / Drum
	vesselDiameterEntry               *widget.Entry
	designTangentToTangentLengthEntry *widget.Entry
	vesselTangentToTangentHeightEntry *widget.Entry

	// Контейнер с полями характеристик
	fieldsContainer *fyne.Container

	// Результат
	resultLabel *widget.Label
	container   *fyne.Container
}

// collectEquipment собирает данные из виджетов строки
func (r *equipmentRow) collectEquipment() (Equipment, error) {
	eq := Equipment{
		Type: strings.TrimSpace(r.typeSelect.Selected),
		Tag:  strings.TrimSpace(r.tagEntry.Text),
	}

	if eq.Tag == "" {
		return eq, fmt.Errorf("тэг обязателен")
	}

	q, err := strconv.Atoi(strings.TrimSpace(r.qtyEntry.Text))
	if err != nil || q < 1 {
		return eq, fmt.Errorf("кол-во должно быть >= 1")
	}
	eq.Quantity = q

	switch eq.Type {
	case "Насосы":
		flow, err := parseOptionalFloat(r.flowEntry.Text)
		if err != nil || flow == nil {
			return eq, fmt.Errorf("расход (Flow Rate) обязателен")
		}
		eq.FlowRate = flow

		head, err := parseOptionalFloat(r.headEntry.Text)
		if err != nil || head == nil {
			return eq, fmt.Errorf("напор (Fluid Head) обязателен")
		}
		eq.FluidHead = head

		eq.RPM, _ = parseOptionalFloat(r.rpmEntry.Text)
		eq.SpecGravity, _ = parseOptionalFloat(r.specGravityEntry.Text)
		eq.PowerKW, _ = parseOptionalFloat(r.powerEntry.Text)

	case "Конвейер":
		conveyorLength, err := parseOptionalFloat(r.conveyorLengthEntry.Text)
		if err != nil || conveyorLength == nil {
			return eq, fmt.Errorf("длина конвейера (Conveyor Length) обязательна")
		}
		eq.ConveyorLength = conveyorLength

		beltWidth, err := parseOptionalFloat(r.beltWidthEntry.Text)
		if err != nil || beltWidth == nil {
			return eq, fmt.Errorf("ширина ленты (Belt Width) обязательна")
		}
		eq.BeltWidth = beltWidth

	case "Вертикальный аппарат":
		vesselDiameter, err := parseOptionalFloat(r.vesselDiameterEntry.Text)
		if err != nil || vesselDiameter == nil {
			return eq, fmt.Errorf("диаметр аппарата (Vessel Diameter) обязателен")
		}
		eq.VesselDiameter = vesselDiameter

		vesselHeight, err := parseOptionalFloat(r.vesselTangentToTangentHeightEntry.Text)
		if err != nil || vesselHeight == nil {
			return eq, fmt.Errorf("высота tangent-to-tangent обязательна")
		}
		eq.VesselTangentToTangentHeight = vesselHeight

	case "Горизонтальная емкость":
		vesselDiameter, err := parseOptionalFloat(r.vesselDiameterEntry.Text)
		if err != nil || vesselDiameter == nil {
			return eq, fmt.Errorf("диаметр аппарата (Vessel Diameter) обязателен")
		}
		eq.VesselDiameter = vesselDiameter

		designLength, err := parseOptionalFloat(r.designTangentToTangentLengthEntry.Text)
		if err != nil || designLength == nil {
			return eq, fmt.Errorf("длина tangent-to-tangent обязательна")
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
}

// showProjectList — экран выбора / создания проекта
func showProjectList(w fyne.Window) {
	appData := loadProjects()

	title := widget.NewLabel("Проекты")
	title.Alignment = fyne.TextAlignCenter
	title.TextStyle = fyne.TextStyle{Bold: true}

	projectList := container.NewVBox()
	for i := range appData.Projects {
		idx := i
		proj := appData.Projects[idx]

		openBtn := widget.NewButtonWithIcon(proj.Name, theme.FolderOpenIcon(), func() {
			showProject(w, proj.Name)
		})

		deleteBtn := widget.NewButtonWithIcon("", theme.DeleteIcon(), func() {
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

		row := container.NewHBox(openBtn, layout.NewSpacer(), deleteBtn)
		projectList.Add(row)
	}

	if len(appData.Projects) == 0 {
		projectList.Add(widget.NewLabel("Нет проектов. Создайте новый."))
	}

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

	scrollable := container.NewVScroll(projectList)
	scrollable.SetMinSize(fyne.NewSize(400, 300))

	content := container.NewVBox(
		backBtn,
		title,
		widget.NewSeparator(),
		scrollable,
		layout.NewSpacer(),
		createBtn,
	)

	w.SetContent(container.NewPadded(content))
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

	totalWeightLabel := widget.NewLabel("Общий вес: —")
	totalWeightLabel.TextStyle = fyne.TextStyle{Bold: true}
	byTypeLabel := widget.NewLabel("По типам: —")

	recalcAll := func() {
		var grandTotal float64
		typeWeights := make(map[string]float64)

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

		totalWeightLabel.SetText(fmt.Sprintf("Общий вес: %.2f кг", grandTotal))

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

	buildPumpFields := func(row *equipmentRow) *fyne.Container {
		row.flowEntry = widget.NewEntry()
		row.flowEntry.SetPlaceHolder("Расход (м³/ч)")

		row.headEntry = widget.NewEntry()
		row.headEntry.SetPlaceHolder("Напор (м)")

		row.rpmEntry = widget.NewEntry()
		row.rpmEntry.SetPlaceHolder("Частота вращения (об/мин)")

		row.specGravityEntry = widget.NewEntry()
		row.specGravityEntry.SetPlaceHolder("Удельный вес")

		row.powerEntry = widget.NewEntry()
		row.powerEntry.SetPlaceHolder("Мощность (кВт)")

		return container.New(layout.NewFormLayout(),
			widget.NewLabel("Расход (м³/ч):"), row.flowEntry,
			widget.NewLabel("Напор (м):"), row.headEntry,
			widget.NewLabel("Частота вращения (об/мин):"), row.rpmEntry,
			widget.NewLabel("Удельный вес:"), row.specGravityEntry,
			widget.NewLabel("Мощность (кВт):"), row.powerEntry,
		)
	}

	buildConveyorFields := func(row *equipmentRow) *fyne.Container {
		row.conveyorLengthEntry = widget.NewEntry()
		row.conveyorLengthEntry.SetPlaceHolder("Длина конвейера (м)")

		row.beltWidthEntry = widget.NewEntry()
		row.beltWidthEntry.SetPlaceHolder("Ширина ленты (мм)")

		return container.New(layout.NewFormLayout(),
			widget.NewLabel("Длина конвейера (м):"), row.conveyorLengthEntry,
			widget.NewLabel("Ширина ленты (мм):"), row.beltWidthEntry,
		)
	}

	buildVesselFields := func(row *equipmentRow) *fyne.Container {
		row.vesselDiameterEntry = widget.NewEntry()
		row.vesselDiameterEntry.SetPlaceHolder("Диаметр аппарата (мм)")

		row.vesselTangentToTangentHeightEntry = widget.NewEntry()
		row.vesselTangentToTangentHeightEntry.SetPlaceHolder("Высота (T/T, мм)")

		return container.New(layout.NewFormLayout(),
			widget.NewLabel("Диаметр аппарата (мм):"), row.vesselDiameterEntry,
			widget.NewLabel("Высота (T/T, мм):"), row.vesselTangentToTangentHeightEntry,
		)
	}

	buildDrumFields := func(row *equipmentRow) *fyne.Container {
		row.vesselDiameterEntry = widget.NewEntry()
		row.vesselDiameterEntry.SetPlaceHolder("Диаметр аппарата (мм)")

		row.designTangentToTangentLengthEntry = widget.NewEntry()
		row.designTangentToTangentLengthEntry.SetPlaceHolder("Длина (T/T, мм)")

		return container.New(layout.NewFormLayout(),
			widget.NewLabel("Диаметр аппарата (мм):"), row.vesselDiameterEntry,
			widget.NewLabel("Длина (T/T, мм):"), row.designTangentToTangentLengthEntry,
		)
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
		tagContainer := container.NewGridWrap(fyne.NewSize(150, 36), row.tagEntry)

		row.qtyEntry = widget.NewEntry()
		row.qtyEntry.SetPlaceHolder("Кол-во")
		if eq.Quantity > 0 {
			row.qtyEntry.SetText(strconv.Itoa(eq.Quantity))
		} else {
			row.qtyEntry.SetText("1")
		}

		row.resultLabel = widget.NewLabel("—")
		row.resultLabel.TextStyle = fyne.TextStyle{Bold: true}

		if eq.CalculatedWeight > 0 {
			row.resultLabel.SetText(fmt.Sprintf("✓ %.2f кг/ед.", eq.CalculatedWeight))
		}

		currentType := row.typeSelect.Selected
		row.fieldsContainer = buildFieldsByType(row, currentType)

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

			row.resultLabel.SetText("⏳ Расчёт...")
			row.resultLabel.Refresh()

			go func() {
				weight, err := sendEquipmentToBackend(eqData)
				if err != nil {
					row.resultLabel.SetText("✗ " + err.Error())
				} else {
					row.resultLabel.SetText(fmt.Sprintf("✓ %.2f кг/ед.", weight))
				}
				row.resultLabel.Refresh()
				recalcAll()
			}()
		})
		calcBtn.Importance = widget.HighImportance

		deleteBtn := widget.NewButtonWithIcon("", theme.DeleteIcon(), func() {
			removeRow(row)
		})

		row.typeSelect.OnChanged = func(selected string) {
			newFields := buildFieldsByType(row, selected)

			row.fieldsContainer.RemoveAll()
			for _, obj := range newFields.Objects {
				row.fieldsContainer.Add(obj)
			}
			row.fieldsContainer.Refresh()
			row.resultLabel.SetText("—")
			recalcAll()
		}

		topRow := container.NewHBox(
			row.typeSelect,
			tagContainer,
			widget.NewLabel("Кол-во:"),
			row.qtyEntry,
			calcBtn,
			row.resultLabel,
			layout.NewSpacer(),
			deleteBtn,
		)

		row.container = container.NewVBox(
			topRow,
			row.fieldsContainer,
			widget.NewSeparator(),
		)

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
		totalWeightLabel.SetText("⏳ Расчёт...")

		go func() {
			for _, r := range rows {
				eq, err := r.collectEquipment()
				if err != nil {
					continue
				}

				r.resultLabel.SetText("⏳...")
				r.resultLabel.Refresh()

				weight, err := sendEquipmentToBackend(eq)
				if err != nil {
					r.resultLabel.SetText("✗ " + err.Error())
				} else {
					r.resultLabel.SetText(fmt.Sprintf("✓ %.2f кг/ед.", weight))
				}
				r.resultLabel.Refresh()
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
				text = strings.TrimPrefix(text, "✓ ")
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
		totalWeightLabel,
		byTypeLabel,
	)

	toolbar := container.NewHBox(
		backBtn,
		layout.NewSpacer(),
		addBtn,
	)

	bottomButtons := container.NewHBox(
		calcAllBtn,
		saveBtn,
	)

	content := container.NewBorder(
		container.NewVBox(toolbar, title, widget.NewSeparator()),
		container.NewVBox(footer, bottomButtons),
		nil, nil,
		scrollable,
	)

	w.SetContent(container.NewPadded(content))
}

// ─── Main ────────────────────────────────────────────────────

func main() {
	fmt.Println("Запуск десктопного приложения...")

	myApp := app.New()
	myWindow := myApp.NewWindow("ConstructMaterialAI: Учёт оборудования")
	myWindow.Resize(fyne.NewSize(900, 650))

	showStartScreen(myWindow)

	myWindow.CenterOnScreen()
	myWindow.ShowAndRun()

	fmt.Println("Приложение закрыто.")
}
