package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/canvas"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/layout"
	"fyne.io/fyne/v2/storage"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
)

func showStartScreen(w fyne.Window) {
	// Основной заголовок
	title := canvas.NewText("ConstructMaterialAI", theme.PrimaryColor())
	title.TextSize = 52
	title.TextStyle = fyne.TextStyle{Bold: true}
	title.Alignment = fyne.TextAlignCenter

	subtitle := widget.NewLabel("Интеллектуальная система оценки веса промышленного оборудования")
	subtitle.Alignment = fyne.TextAlignCenter
	subtitle.TextStyle = fyne.TextStyle{Italic: true}

	// Кнопка начала
	startBtn := NewThemedHoverButton("Открыть менеджер проектов", theme.FolderOpenIcon(), func() {
		showProjectList(w)
	})

	// Блок авторизации
	var authWidget fyne.CanvasObject
	if currentAuth.IsLoggedIn() {
		userLabel := widget.NewLabel(currentAuth.GetEmail())
		userLabel.Alignment = fyne.TextAlignCenter
		userLabel.TextStyle = fyne.TextStyle{Bold: true}

		logoutBtn := widget.NewButtonWithIcon("Выйти", theme.CancelIcon(), func() {
			clearAuthState()
			showStartScreen(w)
		})
		logoutBtn.Importance = widget.LowImportance

		authWidget = container.NewVBox(
			container.NewCenter(container.NewHBox(widget.NewIcon(theme.AccountIcon()), userLabel)),
			container.NewCenter(logoutBtn),
		)
	} else {
		loginBtn := NewThemedHoverButton("Войти в облако", theme.AccountIcon(), func() {
			showLoginScreen(w)
		})
		authWidget = container.NewVBox(
			widget.NewLabelWithStyle("Синхронизация отключена", fyne.TextAlignCenter, fyne.TextStyle{Italic: true}),
			container.NewCenter(container.NewGridWrap(fyne.NewSize(200, 40), loginBtn)),
		)
	}

	// Карточка действий
	v := fyne.CurrentApp().Settings().ThemeVariant()
	cardBg := canvas.NewRectangle(theme.Current().Color(ColorNameCardBackground, v))
	cardBg.CornerRadius = 24
	cardBg.StrokeColor = theme.PrimaryColor()
	cardBg.StrokeWidth = 0.5

	mainActions := container.NewStack(
		cardBg,
		container.NewPadded(container.NewVBox(
			layout.NewSpacer(),
			container.NewCenter(container.NewGridWrap(fyne.NewSize(340, 56), startBtn)),
			authWidget,
			layout.NewSpacer(),
		)),
	)

	// Сборка контента
	content := container.NewVBox(
		layout.NewSpacer(),
		container.NewCenter(title),
		subtitle,
		widget.NewLabel(""),
		container.NewCenter(container.NewGridWrap(fyne.NewSize(460, 260), mainActions)),
		layout.NewSpacer(),
		layout.NewSpacer(),
	)

	w.SetContent(container.NewStack(
		canvas.NewRectangle(theme.BackgroundColor()),
		content,
	))
	w.Resize(fyne.NewSize(windowWidth, windowHeight))
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

	// Кнопки облака (отображаются только если пользователь авторизован)
	var cloudButtons fyne.CanvasObject
	if currentAuth.IsLoggedIn() {
		cloudSaveInfo := widget.NewLabel("☁ " + currentAuth.GetEmail())
		cloudSaveInfo.TextStyle = fyne.TextStyle{Italic: true}

		cloudLoadBtn := widget.NewButtonWithIcon("Загрузить из облака", theme.DownloadIcon(), func() {
			showCloudLoadDialog(w)
		})
		cloudButtons = container.NewHBox(cloudSaveInfo, layout.NewSpacer(), cloudLoadBtn)
	}

	scrollable := container.NewVScroll(projectList)
	scrollable.SetMinSize(fyne.NewSize(600, 400))

	headerItems := []fyne.CanvasObject{
		container.NewHBox(backBtn, layout.NewSpacer(), themeBtn),
		title,
		statsBar,
		container.NewPadded(searchEntry),
	}
	if cloudButtons != nil {
		headerItems = append(headerItems, container.NewPadded(cloudButtons))
	}
	headerItems = append(headerItems, widget.NewSeparator())
	header := container.NewVBox(headerItems...)

	content := container.NewBorder(
		header,
		container.NewPadded(createBtn),
		nil, nil,
		scrollable,
	)

	w.SetContent(container.NewPadded(content))
	w.Resize(fyne.NewSize(windowWidth, windowHeight))
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
			row.conveyorFlowRateEntry.SetText(floatPtrToStr(eq.ConveyorFlowRate))
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
			row.designGaugePressureEntry.SetText(floatPtrToStr(eq.DesignGaugePressure))
			row.designTemperatureEntry.SetText(floatPtrToStr(eq.DesignTemperature))
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

	isAllExpanded := true
	toggleAllBtn := widget.NewButtonWithIcon("Свернуть всё", theme.MenuDropUpIcon(), nil)
	toggleAllBtn.OnTapped = func() {
		if isAllExpanded {
			for _, r := range rows {
				r.fieldsContainer.Hide()
				r.expandBtn.SetIcon(theme.MenuDropDownIcon())
				r.container.Refresh()
			}
			toggleAllBtn.SetText("Развернуть всё")
			toggleAllBtn.SetIcon(theme.MenuDropDownIcon())
			isAllExpanded = false
		} else {
			for _, r := range rows {
				r.fieldsContainer.Show()
				r.expandBtn.SetIcon(theme.MenuDropUpIcon())
				r.container.Refresh()
			}
			toggleAllBtn.SetText("Свернуть всё")
			toggleAllBtn.SetIcon(theme.MenuDropUpIcon())
			isAllExpanded = true
		}
	}

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
		toggleAllBtn,
		layout.NewSpacer(),
		addBtn,
	)

	bottomButtons := container.NewHBox(
		calcAllBtn,
		saveBtn,
	)

	// Кнопка «Сохранить в облако» — только для авторизованных пользователей
	if currentAuth.IsLoggedIn() {
		cloudSaveBtn := widget.NewButtonWithIcon("Сохранить в облако", theme.UploadIcon(), func() {
			// Собираем текущий проект из строк UI
			currentProject := Project{Name: projectName, Equipment: []Equipment{}}
			for _, row := range rows {
				eq, err := row.collectEquipment()
				if err == nil {
					currentProject.Equipment = append(currentProject.Equipment, eq)
				}
			}
			token := currentAuth.GetToken()
			go func() {
				err := saveProjectToCloud(currentProject, token)
				fyne.Do(func() {
					if err != nil {
						dialog.ShowError(fmt.Errorf("Ошибка облачного сохранения: %w", err), w)
					} else {
						dialog.ShowInformation("Облако", "Проект успешно сохранён в облако!", w)
					}
				})
			}()
		})
		bottomButtons.Add(cloudSaveBtn)
	}

	content := container.NewBorder(
		container.NewVBox(toolbarTop, toolbarActions, title, widget.NewSeparator()),
		container.NewVBox(footer, bottomButtons),
		nil, nil,
		scrollable,
	)

	w.SetContent(container.NewPadded(content))
	w.Resize(fyne.NewSize(windowWidth, windowHeight))
}
