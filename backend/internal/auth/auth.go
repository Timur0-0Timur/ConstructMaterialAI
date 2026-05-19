package auth

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
)

const bcryptCost = 12

// HashPassword хэширует пароль через bcrypt
func HashPassword(password string) (string, error) {
	bytes, err := bcrypt.GenerateFromPassword([]byte(password), bcryptCost)
	return string(bytes), err
}

// CheckPassword сравнивает открытый пароль с bcrypt-хэшем
func CheckPassword(password, hash string) bool {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) == nil
}

// GenerateToken выдаёт подписанный HS256 JWT токен
func GenerateToken(userID uint) (string, error) {
	secret := os.Getenv("JWT_SECRET")
	if secret == "" {
		return "", fmt.Errorf("JWT_SECRET не задан в переменных окружения")
	}

	claims := jwt.MapClaims{
		"user_id": userID,
		"exp":     time.Now().Add(72 * time.Hour).Unix(),
		"iat":     time.Now().Unix(),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString([]byte(secret))
}

// contextKey — приватный тип для ключей контекста (избегает коллизий).
type contextKey string

// UserIDKey — ключ для хранения userID в контексте запроса.
const UserIDKey contextKey = "userID"

// AuthMiddleware проверяет Bearer токен и добавляет userID в контекст
func AuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if !strings.HasPrefix(authHeader, "Bearer ") {
			http.Error(w, `{"error":"Требуется авторизация"}`, http.StatusUnauthorized)
			return
		}

		tokenStr := strings.TrimPrefix(authHeader, "Bearer ")
		secret := os.Getenv("JWT_SECRET")

		token, err := jwt.Parse(tokenStr, func(t *jwt.Token) (interface{}, error) {
			if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("неожиданный метод подписи: %v", t.Header["alg"])
			}
			return []byte(secret), nil
		})

		if err != nil || !token.Valid {
			http.Error(w, `{"error":"Недействительный или просроченный токен"}`, http.StatusUnauthorized)
			return
		}

		claims, ok := token.Claims.(jwt.MapClaims)
		if !ok {
			http.Error(w, `{"error":"Недействительный токен"}`, http.StatusUnauthorized)
			return
		}

		// JWT хранит числа как float64
		userIDFloat, ok := claims["user_id"].(float64)
		if !ok {
			http.Error(w, `{"error":"Недействительный токен: user_id"}`, http.StatusUnauthorized)
			return
		}

		ctx := context.WithValue(r.Context(), UserIDKey, uint(userIDFloat))
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// GetUserID извлекает userID из контекста запроса
func GetUserID(r *http.Request) (uint, bool) {
	id, ok := r.Context().Value(UserIDKey).(uint)
	return id, ok
}
