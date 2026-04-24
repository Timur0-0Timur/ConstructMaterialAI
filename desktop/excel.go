package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/xuri/excelize/v2"
)

// ─── Константы Excel ─────────────────────────────────────────

const (
	sheetPumps      = "Насосы"
	sheetConveyor   = "Конвейер"
	sheetVessels    = "Вертикальные аппараты"
	sheetDrums      = "Горизонтальные емкости"
	defaultSheetFix = "Sheet1" // excelize создаёт Sheet1 по умолчанию
)

// Порядок листов
var sheetOrder = []string{sheetPumps, sheetConveyor, sheetVessels, sheetDrums}

// Заголовки для каждого листа.
// Флаг required — обязательна ли колонка.
type colDef struct {
	Title    string
	Required bool
}

var pumpColumns = []colDef{
	{"Тэг", true},
	{"Кол-во", true},
	{"Расход (м³/ч)", true},
	{"Напор (м)", true},
	{"Частота (об/мин)", false},
	{"Уд. вес", false},
	{"Мощность (кВт)", false},
	{"Вес (кг)", false},
}

var conveyorColumns = []colDef{
	{"Тэг", true},
	{"Кол-во", true},
	{"Длина (м)", true},
	{"Ширина (мм)", true},
	{"Вес (кг)", false},
}

var vesselColumns = []colDef{
	{"Тэг", true},
	{"Кол-во", true},
	{"Диаметр (мм)", true},
	{"Высота T/T (мм)", true},
	{"Вес (кг)", false},
}

var drumColumns = []colDef{
	{"Тэг", true},
	{"Кол-во", true},
	{"Диаметр (мм)", true},
	{"Длина T/T (мм)", true},
	{"Вес (кг)", false},
}

func columnsForSheet(sheet string) []colDef {
	switch sheet {
	case sheetPumps:
		return pumpColumns
	case sheetConveyor:
		return conveyorColumns
	case sheetVessels:
		return vesselColumns
	case sheetDrums:
		return drumColumns
	}
	return nil
}

// ─── ImportError ─────────────────────────────────────────────

// ImportError описывает одну ошибку валидации при импорте.
type ImportError struct {
	Sheet  string
	Row    int
	Column string
	Msg    string
}

func (e ImportError) String() string {
	return fmt.Sprintf("Лист «%s», строка %d: в колонке «%s» %s", e.Sheet, e.Row, e.Column, e.Msg)
}

// ─── Генерация шаблона ──────────────────────────────────────

// generateTemplate создаёт пустой XLSX-файл с заголовками.
func generateTemplate(filePath string) error {
	f := excelize.NewFile()
	defer f.Close()

	// Стиль для обязательных заголовков — красный шрифт, жирный
	requiredStyle, err := f.NewStyle(&excelize.Style{
		Font: &excelize.Font{
			Bold:  true,
			Color: "#FF0000",
			Size:  11,
		},
	})
	if err != nil {
		return fmt.Errorf("ошибка создания стиля: %w", err)
	}

	// Стиль для обычных заголовков — жирный
	normalStyle, err := f.NewStyle(&excelize.Style{
		Font: &excelize.Font{
			Bold: true,
			Size: 11,
		},
	})
	if err != nil {
		return fmt.Errorf("ошибка создания стиля: %w", err)
	}

	for i, sheet := range sheetOrder {
		if i == 0 {
			// Переименовываем первый лист (Sheet1) вместо создания нового
			if err := f.SetSheetName(defaultSheetFix, sheet); err != nil {
				return fmt.Errorf("ошибка переименования листа: %w", err)
			}
		} else {
			if _, err := f.NewSheet(sheet); err != nil {
				return fmt.Errorf("ошибка создания листа «%s»: %w", sheet, err)
			}
		}

		cols := columnsForSheet(sheet)
		for j, col := range cols {
			cell, _ := excelize.CoordinatesToCellName(j+1, 1)
			if err := f.SetCellValue(sheet, cell, col.Title); err != nil {
				return fmt.Errorf("ошибка записи заголовка: %w", err)
			}
			if col.Required {
				_ = f.SetCellStyle(sheet, cell, cell, requiredStyle)
			} else {
				_ = f.SetCellStyle(sheet, cell, cell, normalStyle)
			}
		}

		// Ширина колонок для читаемости
		for j := range cols {
			colName, _ := excelize.ColumnNumberToName(j + 1)
			_ = f.SetColWidth(sheet, colName, colName, 18)
		}
	}

	return f.SaveAs(filePath)
}

// ─── Экспорт ────────────────────────────────────────────────

// exportProject экспортирует оборудование проекта в XLSX.
func exportProject(filePath string, equipment []Equipment) error {
	f := excelize.NewFile()
	defer f.Close()

	// Стиль для заголовков — жирный
	headerStyle, err := f.NewStyle(&excelize.Style{
		Font: &excelize.Font{
			Bold: true,
			Size: 11,
		},
	})
	if err != nil {
		return fmt.Errorf("ошибка создания стиля: %w", err)
	}

	// Создаём листы и пишем заголовки
	for i, sheet := range sheetOrder {
		if i == 0 {
			if err := f.SetSheetName(defaultSheetFix, sheet); err != nil {
				return fmt.Errorf("ошибка переименования листа: %w", err)
			}
		} else {
			if _, err := f.NewSheet(sheet); err != nil {
				return fmt.Errorf("ошибка создания листа «%s»: %w", sheet, err)
			}
		}

		cols := columnsForSheet(sheet)
		for j, col := range cols {
			cell, _ := excelize.CoordinatesToCellName(j+1, 1)
			_ = f.SetCellValue(sheet, cell, col.Title)
			_ = f.SetCellStyle(sheet, cell, cell, headerStyle)
		}
		for j := range cols {
			colName, _ := excelize.ColumnNumberToName(j + 1)
			_ = f.SetColWidth(sheet, colName, colName, 18)
		}
	}

	// Счётчики строк по листам (начинаем со 2-й строки)
	rowCounters := map[string]int{
		sheetPumps:    2,
		sheetConveyor: 2,
		sheetVessels:  2,
		sheetDrums:    2,
	}

	for _, eq := range equipment {
		var sheet string
		switch eq.Type {
		case "Насосы":
			sheet = sheetPumps
		case "Конвейер":
			sheet = sheetConveyor
		case "Вертикальный аппарат":
			sheet = sheetVessels
		case "Горизонтальная емкость":
			sheet = sheetDrums
		default:
			continue
		}

		row := rowCounters[sheet]
		rowCounters[sheet]++

		// Общие: Тэг (A), Кол-во (B)
		setCell(f, sheet, 1, row, eq.Tag)
		setCell(f, sheet, 2, row, eq.Quantity)

		switch eq.Type {
		case "Насосы":
			setCellOptFloat(f, sheet, 3, row, eq.FlowRate)
			setCellOptFloat(f, sheet, 4, row, eq.FluidHead)
			setCellOptFloat(f, sheet, 5, row, eq.RPM)
			setCellOptFloat(f, sheet, 6, row, eq.SpecGravity)
			setCellOptFloat(f, sheet, 7, row, eq.PowerKW)
			if eq.CalculatedWeight > 0 {
				setCell(f, sheet, 8, row, eq.CalculatedWeight)
			}

		case "Конвейер":
			setCellOptFloat(f, sheet, 3, row, eq.ConveyorLength)
			setCellOptFloat(f, sheet, 4, row, eq.BeltWidth)
			if eq.CalculatedWeight > 0 {
				setCell(f, sheet, 5, row, eq.CalculatedWeight)
			}

		case "Вертикальный аппарат":
			setCellOptFloat(f, sheet, 3, row, eq.VesselDiameter)
			setCellOptFloat(f, sheet, 4, row, eq.VesselTangentToTangentHeight)
			if eq.CalculatedWeight > 0 {
				setCell(f, sheet, 5, row, eq.CalculatedWeight)
			}

		case "Горизонтальная емкость":
			setCellOptFloat(f, sheet, 3, row, eq.VesselDiameter)
			setCellOptFloat(f, sheet, 4, row, eq.DesignTangentToTangentLength)
			if eq.CalculatedWeight > 0 {
				setCell(f, sheet, 5, row, eq.CalculatedWeight)
			}
		}
	}

	return f.SaveAs(filePath)
}

// ─── Импорт ─────────────────────────────────────────────────

// importProject читает XLSX и возвращает оборудование + ошибки валидации.
func importProject(filePath string) ([]Equipment, []ImportError) {
	var equipment []Equipment
	var errors []ImportError

	f, err := excelize.OpenFile(filePath)
	if err != nil {
		errors = append(errors, ImportError{
			Sheet:  "—",
			Row:    0,
			Column: "—",
			Msg:    fmt.Sprintf("невозможно открыть файл: %v", err),
		})
		return nil, errors
	}
	defer f.Close()

	// Обработка каждого листа
	for _, sheet := range sheetOrder {
		rows, err := f.GetRows(sheet)
		if err != nil {
			// Лист может отсутствовать — не ошибка
			continue
		}

		cols := columnsForSheet(sheet)

		// Пропускаем заголовок (строка 0), данные начинаются со строки 1 (Excel-строка 2)
		for rowIdx := 1; rowIdx < len(rows); rowIdx++ {
			excelRow := rowIdx + 1 // номер строки в Excel (1-based)
			cells := rows[rowIdx]

			// Пустая строка — пропускаем
			if isEmptyRow(cells) {
				continue
			}

			eq := Equipment{}
			hasError := false

			// Тэг (колонка A, index 0)
			tag := getCellStr(cells, 0)
			if tag == "" {
				errors = append(errors, ImportError{
					Sheet:  sheet,
					Row:    excelRow,
					Column: cols[0].Title,
					Msg:    "не заполнен обязательный параметр «Тэг»",
				})
				hasError = true
			}
			eq.Tag = tag

			// Кол-во (колонка B, index 1)
			qtyStr := getCellStr(cells, 1)
			qty, qtyErr := strconv.Atoi(strings.TrimSpace(qtyStr))
			if qtyErr != nil || qty < 1 {
				if qtyStr == "" {
					errors = append(errors, ImportError{
						Sheet:  sheet,
						Row:    excelRow,
						Column: cols[1].Title,
						Msg:    "не заполнен обязательный параметр «Кол-во»",
					})
				} else {
					errors = append(errors, ImportError{
						Sheet:  sheet,
						Row:    excelRow,
						Column: cols[1].Title,
						Msg:    fmt.Sprintf("ожидалось целое число ≥ 1, найдено «%s»", qtyStr),
					})
				}
				hasError = true
			} else {
				eq.Quantity = qty
			}

			switch sheet {
			case sheetPumps:
				eq.Type = "Насосы"
				// Расход (C, обязательный)
				eq.FlowRate, hasError = parseImportFloat(cells, 2, cols, sheet, excelRow, true, &errors, hasError)
				// Напор (D, обязательный)
				eq.FluidHead, hasError = parseImportFloat(cells, 3, cols, sheet, excelRow, true, &errors, hasError)
				// Частота (E, опциональный)
				eq.RPM, hasError = parseImportFloat(cells, 4, cols, sheet, excelRow, false, &errors, hasError)
				// Уд. вес (F, опциональный)
				eq.SpecGravity, hasError = parseImportFloat(cells, 5, cols, sheet, excelRow, false, &errors, hasError)
				// Мощность (G, опциональный)
				eq.PowerKW, hasError = parseImportFloat(cells, 6, cols, sheet, excelRow, false, &errors, hasError)

			case sheetConveyor:
				eq.Type = "Конвейер"
				// Длина (C, обязательный)
				eq.ConveyorLength, hasError = parseImportFloat(cells, 2, cols, sheet, excelRow, true, &errors, hasError)
				// Ширина (D, обязательный)
				eq.BeltWidth, hasError = parseImportFloat(cells, 3, cols, sheet, excelRow, true, &errors, hasError)

			case sheetVessels:
				eq.Type = "Вертикальный аппарат"
				// Диаметр (C, обязательный)
				eq.VesselDiameter, hasError = parseImportFloat(cells, 2, cols, sheet, excelRow, true, &errors, hasError)
				// Высота (D, обязательный)
				eq.VesselTangentToTangentHeight, hasError = parseImportFloat(cells, 3, cols, sheet, excelRow, true, &errors, hasError)

			case sheetDrums:
				eq.Type = "Горизонтальная емкость"
				// Диаметр (C, обязательный)
				eq.VesselDiameter, hasError = parseImportFloat(cells, 2, cols, sheet, excelRow, true, &errors, hasError)
				// Длина (D, обязательный)
				eq.DesignTangentToTangentLength, hasError = parseImportFloat(cells, 3, cols, sheet, excelRow, true, &errors, hasError)
			}

			if !hasError {
				equipment = append(equipment, eq)
			}
		}
	}

	return equipment, errors
}

// ─── Хелперы Excel ──────────────────────────────────────────

func setCell(f *excelize.File, sheet string, col, row int, value interface{}) {
	cell, _ := excelize.CoordinatesToCellName(col, row)
	_ = f.SetCellValue(sheet, cell, value)
}

func setCellOptFloat(f *excelize.File, sheet string, col, row int, val *float64) {
	if val != nil {
		setCell(f, sheet, col, row, *val)
	}
}

func getCellStr(cells []string, idx int) string {
	if idx >= len(cells) {
		return ""
	}
	return strings.TrimSpace(cells[idx])
}

func isEmptyRow(cells []string) bool {
	for _, c := range cells {
		if strings.TrimSpace(c) != "" {
			return false
		}
	}
	return true
}

// parseImportFloat — парсит float из ячейки с валидацией.
// Возвращает указатель на значение и обновлённый флаг hasError.
func parseImportFloat(
	cells []string,
	colIdx int,
	cols []colDef,
	sheet string,
	excelRow int,
	required bool,
	errors *[]ImportError,
	currentHasError bool,
) (*float64, bool) {
	raw := getCellStr(cells, colIdx)
	colTitle := "?"
	if colIdx < len(cols) {
		colTitle = cols[colIdx].Title
	}

	if raw == "" {
		if required {
			*errors = append(*errors, ImportError{
				Sheet:  sheet,
				Row:    excelRow,
				Column: colTitle,
				Msg:    fmt.Sprintf("не заполнен обязательный параметр «%s»", colTitle),
			})
			return nil, true
		}
		return nil, currentHasError
	}

	// Заменяем запятую на точку
	raw = strings.ReplaceAll(raw, ",", ".")
	val, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		*errors = append(*errors, ImportError{
			Sheet:  sheet,
			Row:    excelRow,
			Column: colTitle,
			Msg:    fmt.Sprintf("ожидалось число, найдено «%s»", getCellStr(cells, colIdx)),
		})
		return nil, true
	}

	return &val, currentHasError
}
