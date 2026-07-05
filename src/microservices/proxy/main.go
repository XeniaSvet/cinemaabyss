package main

import (
	"hash/fnv"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Port                   string
	MonolithURL            *url.URL
	MoviesServiceURL       *url.URL
	EventsServiceURL       *url.URL
	GradualMigration       bool
	MoviesMigrationPercent int
}

func main() {
	// Инициализируем генератор случайных чисел для фолбека
	rand.Seed(time.Now().UnixNano())

	config := loadConfig()

	// Создаем обратные прокси (Reverse Proxies) для каждого целевого сервиса
	monolithProxy := httputil.NewSingleHostReverseProxy(config.MonolithURL)
	moviesProxy := httputil.NewSingleHostReverseProxy(config.MoviesServiceURL)
	eventsProxy := httputil.NewSingleHostReverseProxy(config.EventsServiceURL)

	// Настраиваем логирование ошибок проксирования
	setupProxyErrorHandler(monolithProxy, "Monolith")
	setupProxyErrorHandler(moviesProxy, "Movies-Service")
	setupProxyErrorHandler(eventsProxy, "Events-Service")

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Обязательно обновляем заголовок Host, чтобы целевой сервер корректно обработал запрос
		path := r.URL.Path

		// Маршрутизация на сервис событий (Events)
		if strings.HasPrefix(path, "/api/v1/events") {
			r.Host = config.EventsServiceURL.Host
			eventsProxy.ServeHTTP(w, r)
			return
		}

		// Логика Strangler Fig для сервиса метаданных фильмов (Movies)
		if strings.HasPrefix(path, "/api/v1/movies") {
			if config.GradualMigration && shouldRouteToNewService(r, config.MoviesMigrationPercent) {
				log.Printf("[Strangler] Routing request %s %s to MOVIES-SERVICE", r.Method, path)
				r.Host = config.MoviesServiceURL.Host
				moviesProxy.ServeHTTP(w, r)
				return
			}
			
			// Если фиче-флаг выключен или процент не пройден — отдаем монолиту
			log.Printf("[Strangler] Routing request %s %s to MONOLITH", r.Method, path)
			r.Host = config.MonolithURL.Host
			monolithProxy.ServeHTTP(w, r)
			return
		}

		// Все остальные эндпоинты по умолчанию уходят на Монолит
		r.Host = config.MonolithURL.Host
		monolithProxy.ServeHTTP(w, r)
	})

	log.Printf("Proxy Gateway started on port %s", config.Port)
	log.Printf("Migration flag Status: %t, Target Percent: %d%%", config.GradualMigration, config.MoviesMigrationPercent)
	if err := http.ListenAndServe(":" + config.Port, nil); err != nil {
		log.Fatalf("Failed to start proxy server: %v", err)
	}
}

// shouldRouteToNewService определяет, пойдет ли запрос в новый микросервис
func shouldRouteToNewService(r *http.Request, percent int) bool {
	if percent <= 0 {
		return false
	}
	if percent >= 100 {
		return true
	}

	// Ищем идентификатор пользователя для обеспечения липкости (Sticky Sessions)
	// Пытаемся достать из кастомного заголовка или Cookie
	userID := r.Header.Get("X-User-ID")
	if userID == "" {
		if cookie, err := r.Cookie("user_id"); err == nil {
			userID = cookie.Value
		}
	}

	// Если пользователь не авторизован, привязываемся к IP-адресу, чтобы избежать мерцания UI
	if userID == "" {
		userID = r.RemoteAddr
	}

	// Хешируем строку для получения стабильного распределения от 0 до 99
	hasher := fnv.New32a()
	hasher.Write([]byte(userID))
	hashValue := hasher.Sum32()
	userBucket := int(hashValue % 100)

	return userBucket < percent
}

func setupProxyErrorHandler(proxy *httputil.ReverseProxy, serviceName string) {
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("[ERROR] Proxying to %s failed: %v", serviceName, err)
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(fmt.Sprintf("Gateway Error: Unable to reach %s", serviceName)))
	}
}

func loadConfig() Config {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8000"
	}

	return Config{
		Port:                   port,
		MonolithURL:            parseURL(os.Getenv("MONOLITH_URL")),
		MoviesServiceURL:       parseURL(os.Getenv("MOVIES_SERVICE_URL")),
		EventsServiceURL:       parseURL(os.Getenv("EVENTS_SERVICE_URL")),
		GradualMigration:       os.Getenv("GRADUAL_MIGRATION") == "true",
		MoviesMigrationPercent: parseInt(os.Getenv("MOVIES_MIGRATION_PERCENT"), 0),
	}
}

func parseURL(envValue string) *url.URL {
	u, err := url.Parse(envValue)
	if err != nil {
		log.Fatalf("Invalid URL config value %s: %v", envValue, err)
	}
	return u
}

func parseInt(envValue string, defaultValue int) int {
	if envValue == "" {
		return defaultValue
	}
	val, err := strconv.Atoi(envValue)
	if err != nil {
		return defaultValue
	}
	return val
}
