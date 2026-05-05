package main

import (
	"fmt"
	"image/color"
	"strconv"
	"strings"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/canvas"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/driver/desktop"
	"fyne.io/fyne/v2/layout"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
)

// Элементы UI
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

type equipmentRow struct {
	typeSelect *widget.Select
	tagEntry   *widget.Entry
	qtyEntry   *widget.Entry

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

	conveyorLengthLabel   *canvas.Text
	conveyorLengthEntry   *widget.Entry
	beltWidthLabel        *canvas.Text
	beltWidthEntry        *widget.Entry
	conveyorFlowRateLabel *canvas.Text
	conveyorFlowRateEntry *widget.Entry

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

	fieldsContainer *fyne.Container
	resultLabel     *widget.Label
	expandBtn       *widget.Button
	deleteBtn       *widget.Button
	container       *fyne.Container

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

	labels := []*canvas.Text{
		r.flowLabel, r.headLabel, r.rpmLabel, r.specGravityLabel, r.powerLabel,
		r.conveyorLengthLabel, r.beltWidthLabel, r.conveyorFlowRateLabel,
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
			return eq, fmt.Errorf("Расход обязателен")
		}
		eq.FlowRate = flow

		head, err := parseOptionalFloat(r.headEntry.Text)
		if err != nil || head == nil {
			r.markFieldInvalid(r.headEntry, r.headLabel, true)
			return eq, fmt.Errorf("Напор обязателен")
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
			return eq, fmt.Errorf("Длина конвейера обязательна")
		}
		eq.ConveyorLength = conveyorLength

		beltWidth, err := parseOptionalFloat(r.beltWidthEntry.Text)
		if err != nil || beltWidth == nil {
			r.markFieldInvalid(r.beltWidthEntry, r.beltWidthLabel, true)
			return eq, fmt.Errorf("Ширина ленты обязательна")
		}
		eq.BeltWidth = beltWidth

		eq.ConveyorFlowRate, err = parseOptionalFloat(r.conveyorFlowRateEntry.Text)
		if err != nil {
			r.markFieldInvalid(r.conveyorFlowRateEntry, r.conveyorFlowRateLabel, true)
		}

	case "Вертикальный аппарат":
		vesselDiameter, err := parseOptionalFloat(r.vesselDiameterEntry.Text)
		if err != nil || vesselDiameter == nil {
			r.markFieldInvalid(r.vesselDiameterEntry, r.vesselDiameterLabel, true)
			return eq, fmt.Errorf("Диаметр обязателен")
		}
		eq.VesselDiameter = vesselDiameter

		vesselHeight, err := parseOptionalFloat(r.vesselTangentToTangentHeightEntry.Text)
		if err != nil || vesselHeight == nil {
			r.markFieldInvalid(r.vesselTangentToTangentHeightEntry, r.vesselTangentToTangentHeightLabel, true)
			return eq, fmt.Errorf("Высота обязательна")
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
			return eq, fmt.Errorf("Нельзя указывать одновременно высоту юбки и опор")
		}

	case "Горизонтальная емкость":
		vesselDiameter, err := parseOptionalFloat(r.vesselDiameterEntry.Text)
		if err != nil || vesselDiameter == nil {
			r.markFieldInvalid(r.vesselDiameterEntry, r.vesselDiameterLabel, true)
			return eq, fmt.Errorf("Диаметр обязателен")
		}
		eq.VesselDiameter = vesselDiameter

		designLength, err := parseOptionalFloat(r.designTangentToTangentLengthEntry.Text)
		if err != nil || designLength == nil {
			r.markFieldInvalid(r.designTangentToTangentLengthEntry, r.designTangentToTangentLengthLabel, true)
			return eq, fmt.Errorf("Длина обязательна")
		}
		eq.DesignTangentToTangentLength = designLength

		eq.DesignGaugePressure, err = parseOptionalFloat(r.designGaugePressureEntry.Text)
		if err != nil {
			r.markFieldInvalid(r.designGaugePressureEntry, r.designGaugePressureLabel, true)
		}
		eq.DesignTemperature, err = parseOptionalFloat(r.designTemperatureEntry.Text)
		if err != nil {
			r.markFieldInvalid(r.designTemperatureEntry, r.designTemperatureLabel, true)
		}
	}

	return eq, nil
}

// HoverButton — кнопка с поддержкой событий наведения
type HoverButton struct {
	widget.Button
	OnHover func(bool)
}

func (b *HoverButton) MouseIn(*desktop.MouseEvent) {
	if b.OnHover != nil {
		b.OnHover(true)
	}
}

func (b *HoverButton) MouseOut() {
	if b.OnHover != nil {
		b.OnHover(false)
	}
}

func NewHoverButton(label string, icon fyne.Resource, tapped func(), hover func(bool)) *HoverButton {
	b := &HoverButton{OnHover: hover}
	b.Text = label
	b.Icon = icon
	b.OnTapped = tapped
	b.ExtendBaseWidget(b)
	return b
}

// NewThemedHoverButton создает кнопку с подложкой и эффектом темно-синего наведения
func NewThemedHoverButton(label string, icon fyne.Resource, tapped func()) fyne.CanvasObject {
	bg := canvas.NewRectangle(theme.PrimaryColor())
	bg.CornerRadius = 10

	btn := NewHoverButton(label, icon, tapped, nil)
	btn.Importance = widget.LowImportance

	btn.OnHover = func(hover bool) {
		if hover {
			bg.FillColor = color.NRGBA{R: 2, G: 132, B: 199, A: 255}
		} else {
			bg.FillColor = theme.PrimaryColor()
		}
		bg.Refresh()
	}
	return container.NewStack(bg, btn)
}
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
		r.conveyorFlowRateEntry,
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
	setLabelError(r.conveyorFlowRateLabel, false)
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

	conveyorFlowRateLabel, conveyorFlowRateObj := createLabel("Производительность (т/ч):", false)
	row.conveyorFlowRateLabel = conveyorFlowRateLabel
	row.conveyorFlowRateEntry = widget.NewEntry()
	row.conveyorFlowRateEntry.SetPlaceHolder("Введите производительность")
	row.conveyorFlowRateEntry.OnChanged = func(s string) {
		_, err := parseOptionalFloat(s)
		row.markFieldInvalid(row.conveyorFlowRateEntry, row.conveyorFlowRateLabel, err != nil)
	}

	return container.New(layout.NewFormLayout(),
		conveyorLengthObj, row.conveyorLengthEntry,
		beltWidthObj, row.beltWidthEntry,
		conveyorFlowRateObj, row.conveyorFlowRateEntry,
	)
}

func buildVesselFields(row *equipmentRow) *fyne.Container {
	vesselDiameterLabel, vesselDiameterObj := createLabel("Диаметр аппарата (м):", true)
	row.vesselDiameterLabel = vesselDiameterLabel
	row.vesselDiameterEntry = widget.NewEntry()
	row.vesselDiameterEntry.SetPlaceHolder("Диаметр корпуса (м)")
	row.vesselDiameterEntry.OnChanged = func(s string) {
		val, err := parseOptionalFloat(s)
		row.markFieldInvalid(row.vesselDiameterEntry, row.vesselDiameterLabel, err != nil || val == nil)
	}

	vesselTangentToTangentHeightLabel, vesselTangentToTangentHeightObj := createLabel("Высота (T/T, м):", true)
	row.vesselTangentToTangentHeightLabel = vesselTangentToTangentHeightLabel
	row.vesselTangentToTangentHeightEntry = widget.NewEntry()
	row.vesselTangentToTangentHeightEntry.SetPlaceHolder("Высота Straight Side (м)")
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

	skirtHeightLabel, skirtHeightObj := createLabel("Высота юбки (м):", false)
	row.skirtHeightLabel = skirtHeightLabel
	row.skirtHeightEntry = widget.NewEntry()
	row.skirtHeightEntry.SetPlaceHolder("Высота юбки (м)")
	row.skirtHeightEntry.OnChanged = func(s string) {
		_, err := parseOptionalFloat(s)
		row.markFieldInvalid(row.skirtHeightEntry, row.skirtHeightLabel, err != nil)
	}

	vesselLegHeightLabel, vesselLegHeightObj := createLabel("Высота опор (м):", false)
	row.vesselLegHeightLabel = vesselLegHeightLabel
	row.vesselLegHeightEntry = widget.NewEntry()
	row.vesselLegHeightEntry.SetPlaceHolder("Высота опор (м)")
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

	return container.New(layout.NewFormLayout(),
		vesselDiameterObj, row.vesselDiameterEntry,
		designTangentToTangentLengthObj, row.designTangentToTangentLengthEntry,
		designGaugePressureObj, row.designGaugePressureEntry,
		designTemperatureObj, row.designTemperatureEntry,
	)
}
