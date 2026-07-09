package main

import (
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

	// Настройка для Monolith
	origMonolithDir := monolithProxy.Director
	monolithProxy.Director = func(req *http.Request) {
		origMonolithDir(req)
		req.URL.Scheme = config.MonolithURL.Scheme
		req.URL.Host = config.MonolithURL.Host
		req.Host = config.MonolithURL.Host
	}

	// Настройка для Movies
	origMoviesDir := moviesProxy.Director
	moviesProxy.Director = func(req *http.Request) {
		origMoviesDir(req)
		req.URL.Scheme = config.MoviesServiceURL.Scheme
		req.URL.Host = config.MoviesServiceURL.Host
		req.Host = config.MoviesServiceURL.Host
	}

	// Настройка для Events
	origEventsDir := eventsProxy.Director
	eventsProxy.Director = func(req *http.Request) {
		origEventsDir(req)
		req.URL.Scheme = config.EventsServiceURL.Scheme
		req.URL.Host = config.EventsServiceURL.Host
		req.Host = config.EventsServiceURL.Host
	}

	// Настраиваем логирование ошибок проксирования
	setupProxyErrorHandler(monolithProxy, "Monolith")
	setupProxyErrorHandler(moviesProxy, "Movies-Service")
	setupProxyErrorHandler(eventsProxy, "Events-Service")

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Обязательно обновляем заголовок Host, чтобы целевой сервер корректно обработал запрос
		path := r.URL.Path

		// Маршрутизация на сервис событий (Events)
		if strings.HasPrefix(path, "/api/events") {
			eventsProxy.ServeHTTP(w, r)
			return
		}

		// Логика Strangler Fig для сервиса метаданных фильмов (Movies)
		if strings.HasPrefix(path, "/api/movies") {
			if config.GradualMigration && shouldRouteToNewService(r, config.MoviesMigrationPercent) {
				log.Printf("[Strangler] Routing request %s %s to MOVIES-SERVICE", r.Method, path)
				moviesProxy.ServeHTTP(w, r)
				return
			}

			// Если фиче-флаг выключен или процент не пройден — отдаем монолиту
			log.Printf("[Strangler] Routing request %s %s to MONOLITH", r.Method, path)
			monolithProxy.ServeHTTP(w, r)
			return
		}

		// Все остальные эндпоинты по умолчанию уходят на Монолит
		monolithProxy.ServeHTTP(w, r)
	})

	log.Printf("Proxy Gateway started on port %s", config.Port)
	log.Printf("Migration flag Status: %t, Target Percent: %d%%", config.GradualMigration, config.MoviesMigrationPercent)
	if err := http.ListenAndServe(":"+config.Port, nil); err != nil {
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

	return rand.Intn(100) < percent
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
