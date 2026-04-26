package main

import (
	"image/color"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/theme"
)

const ColorNameCardBackground fyne.ThemeColorName = "cardBackground"

type modernTheme struct{}

var _ fyne.Theme = (*modernTheme)(nil)

func (m *modernTheme) Color(name fyne.ThemeColorName, variant fyne.ThemeVariant) color.Color {
	// Игнорируем variant и всегда используем темную схему для "Modern Dark"
	variant = theme.VariantDark

	switch name {
	case theme.ColorNameBackground:
		return color.NRGBA{R: 15, G: 23, B: 42, A: 255} // Slate 900
	case theme.ColorNameInputBackground:
		return color.NRGBA{R: 51, G: 65, B: 85, A: 255} // Slate 700 (светлее карточки)
	case ColorNameCardBackground:
		return color.NRGBA{R: 30, G: 41, B: 59, A: 255} // Slate 800 (теперь непрозрачный)
	case theme.ColorNamePrimary:
		return color.NRGBA{R: 56, G: 189, B: 248, A: 255} // Sky 400 (Cyan)
	case theme.ColorNameForeground:
		return color.NRGBA{R: 248, G: 250, B: 252, A: 255} // Slate 50
	case theme.ColorNameMenuBackground:
		return color.NRGBA{R: 15, G: 23, B: 42, A: 255}
	case theme.ColorNameSeparator:
		return color.NRGBA{R: 51, G: 65, B: 85, A: 255} // Slate 700
	case theme.ColorNameButton:
		return color.NRGBA{R: 30, G: 41, B: 59, A: 255}
	case theme.ColorNameFocus:
		return color.NRGBA{R: 56, G: 189, B: 248, A: 255} // Cyan focus
	case theme.ColorNameError:
		return color.NRGBA{R: 239, G: 68, B: 68, A: 255} // Red 500
	default:
		return theme.DefaultTheme().Color(name, variant)
	}
}

func (m *modernTheme) Font(style fyne.TextStyle) fyne.Resource {
	return theme.DefaultTheme().Font(style)
}

func (m *modernTheme) Icon(name fyne.ThemeIconName) fyne.Resource {
	return theme.DefaultTheme().Icon(name)
}

func (m *modernTheme) Size(name fyne.ThemeSizeName) float32 {
	switch name {
	case theme.SizeNameText:
		return 16 // Было 14, увеличиваем для лучшей видимости
	case theme.SizeNameHeadingText:
		return 20
	case theme.SizeNameSubHeadingText:
		return 16
	case theme.SizeNameCaptionText:
		return 11
	case theme.SizeNamePadding:
		return 6 // Уменьшаем с 8 до 6, чтобы тексту было просторнее в инпутах
	case theme.SizeNameInputRadius:
		return 8
	case theme.SizeNameSelectionRadius:
		return 6
	case theme.SizeNameScrollBar:
		return 10
	default:
		return theme.DefaultTheme().Size(name)
	}
}

func newModernTheme() fyne.Theme {
	return &modernTheme{}
}
