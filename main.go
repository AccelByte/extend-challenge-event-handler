// Copyright (c) 2023 AccelByte Inc. All Rights Reserved.
// This is licensed software from AccelByte Inc, for limitations
// and restrictions contact your company contract manager.

package main

import (
	"context"
	"database/sql"
	"extend-challenge-event-handler/pkg/buffered"
	"extend-challenge-event-handler/pkg/common"
	"extend-challenge-event-handler/pkg/processor"
	"extend-challenge-event-handler/pkg/service"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/AccelByte/extend-challenge-common/pkg/cache"
	"github.com/AccelByte/extend-challenge-common/pkg/config"
	"github.com/AccelByte/extend-challenge-common/pkg/repository"

	agsRepository "github.com/AccelByte/accelbyte-go-sdk/services-api/pkg/repository"

	"github.com/AccelByte/accelbyte-go-sdk/services-api/pkg/factory"
	"github.com/AccelByte/accelbyte-go-sdk/services-api/pkg/service/iam"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/propagators/b3"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	pb "extend-challenge-event-handler/pkg/pb/accelbyte-asyncapi/iam/account/v1"
	statpb "extend-challenge-event-handler/pkg/pb/accelbyte-asyncapi/social/statistic/v1"

	sdkAuth "github.com/AccelByte/accelbyte-go-sdk/services-api/pkg/utils/auth"
	prometheusGrpc "github.com/grpc-ecosystem/go-grpc-prometheus"
	prometheusCollectors "github.com/prometheus/client_golang/prometheus/collectors"

	_ "github.com/lib/pq" // PostgreSQL driver
)

const (
	environment     = "production"
	id              = int64(1)
	metricsEndpoint = "/metrics"
	metricsPort     = 8080
	grpcServerPort  = 6565
)

var (
	serviceName = common.GetEnv("OTEL_SERVICE_NAME", "ExtendEventHandlerGoServerDocker")
	logLevelStr = common.GetEnv("LOG_LEVEL", "info")
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create zerolog logger
	zerologLogger := common.NewZerologLogger(logLevelStr)
	zerologLogger.Info().Msg("starting app server")

	// Create slog logger for config and cache (convert from zerolog level)
	var slogLevel slog.Level
	switch zerolog.GlobalLevel() {
	case zerolog.DebugLevel:
		slogLevel = slog.LevelDebug
	case zerolog.InfoLevel:
		slogLevel = slog.LevelInfo
	case zerolog.WarnLevel:
		slogLevel = slog.LevelWarn
	case zerolog.ErrorLevel, zerolog.FatalLevel, zerolog.PanicLevel:
		slogLevel = slog.LevelError
	default:
		slogLevel = slog.LevelInfo
	}
	slogLogger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slogLevel}))

	loggingOptions := []logging.Option{
		logging.WithLogOnEvents(logging.StartCall, logging.FinishCall, logging.PayloadReceived, logging.PayloadSent),
		logging.WithFieldsFromContext(func(ctx context.Context) logging.Fields {
			if span := trace.SpanContextFromContext(ctx); span.IsSampled() {
				return logging.Fields{"traceID", span.TraceID().String()}
			}

			return nil
		}),
		logging.WithLevels(logging.DefaultClientCodeToLevel),
		logging.WithDurationField(logging.DurationToDurationField),
	}
	unaryServerInterceptors := []grpc.UnaryServerInterceptor{
		prometheusGrpc.UnaryServerInterceptor,
		logging.UnaryServerInterceptor(common.InterceptorLogger(&zerologLogger), loggingOptions...),
	}
	streamServerInterceptors := []grpc.StreamServerInterceptor{
		prometheusGrpc.StreamServerInterceptor,
		logging.StreamServerInterceptor(common.InterceptorLogger(&zerologLogger), loggingOptions...),
	}

	// Preparing the IAM authorization
	var tokenRepo agsRepository.TokenRepository = sdkAuth.DefaultTokenRepositoryImpl()
	var configRepo agsRepository.ConfigRepository = sdkAuth.DefaultConfigRepositoryImpl()
	var refreshRepo agsRepository.RefreshTokenRepository = &sdkAuth.RefreshTokenImpl{AutoRefresh: true, RefreshRate: 0.8}

	// Create gRPC Server
	s := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(unaryServerInterceptors...),
		grpc.ChainStreamInterceptor(streamServerInterceptors...),
	)

	// Configure IAM authorization
	oauthService := iam.OAuth20Service{
		Client:                 factory.NewIamClient(configRepo),
		ConfigRepository:       configRepo,
		TokenRepository:        tokenRepo,
		RefreshTokenRepository: refreshRepo,
	}
	// Check if OAuth login is required (only when auth is enabled)
	authEnabled := strings.ToLower(common.GetEnv("PLUGIN_GRPC_SERVER_AUTH_ENABLED", "true")) == "true"

	if authEnabled {
		clientId := configRepo.GetClientId()
		clientSecret := configRepo.GetClientSecret()
		err := oauthService.LoginClient(&clientId, &clientSecret)
		if err != nil {
			zerologLogger.Fatal().Err(err).Msg("Error unable to login using clientId and clientSecret")
		}
		zerologLogger.Info().Msg("Successfully logged in to AGS IAM")
	} else {
		zerologLogger.Info().Msg("Skipping AGS OAuth login (auth disabled)")
	}

	// Initialize database connection
	db, err := initDatabase(zerologLogger)
	if err != nil {
		zerologLogger.Fatal().Err(err).Msg("Failed to initialize database")
	}
	defer func() {
		if err := db.Close(); err != nil {
			zerologLogger.Error().Err(err).Msg("Failed to close database connection")
		}
	}()
	zerologLogger.Info().Msg("Database connection established")

	// Load challenge configuration
	configPath := common.GetEnv("CONFIG_PATH", "/app/config/challenges.json")
	configLoader := config.NewConfigLoader(configPath, slogLogger)
	challengeConfig, err := configLoader.LoadConfig()
	if err != nil {
		zerologLogger.Fatal().Err(err).Msg("Failed to load challenge config")
	}
	zerologLogger.Info().Msg("Challenge configuration loaded successfully")

	// Initialize goal cache
	goalCache := cache.NewInMemoryGoalCache(challengeConfig, configPath, slogLogger)
	zerologLogger.Info().Msg("Goal cache initialized")

	// Get namespace from environment
	namespace := common.GetEnv("AB_NAMESPACE", "")
	if namespace == "" {
		zerologLogger.Fatal().Msg("AB_NAMESPACE environment variable is required")
	}

	// Create repository with buffering
	// PHASE 4 OPTIMIZATION: Parallel flush workers for 500+ EPS throughput
	// - Phase 3 achieved 475 EPS with single-threaded flush (PostgreSQL bottleneck eliminated)
	// - Phase 4 uses N parallel flush workers to parallelize database writes
	// - Each worker has independent flush goroutine (no contention)
	// - Recommended: 6-8 workers for 500 EPS (16,000-21,000 updates/sec capacity)
	postgresRepo := repository.NewPostgresGoalRepository(db)

	// Read FLUSH_WORKERS from environment (default: 8 for 500 EPS target)
	flushWorkersStr := common.GetEnv("FLUSH_WORKERS", "8")
	flushWorkers := 8 // default
	if n, err := fmt.Sscanf(flushWorkersStr, "%d", &flushWorkers); err != nil || n != 1 {
		zerologLogger.Warn().
			Str("value", flushWorkersStr).
			Msg("Invalid FLUSH_WORKERS value, using default: 8")
		flushWorkers = 8
	}

	// Validate workers count
	if flushWorkers < 1 {
		zerologLogger.Warn().
			Int("value", flushWorkers).
			Msg("FLUSH_WORKERS must be >= 1, using default: 8")
		flushWorkers = 8
	}

	if flushWorkers > 32 {
		zerologLogger.Warn().
			Int("value", flushWorkers).
			Msg("FLUSH_WORKERS must be <= 32, using maximum: 32")
		flushWorkers = 32
	}

	// Create buffered repository (partitioned or single-threaded)
	var bufferedRepo buffered.Repository
	if flushWorkers > 1 {
		// Use partitioned repository for parallel flush workers
		partitionedRepo, err := buffered.NewPartitionedBufferedRepository(
			postgresRepo,
			goalCache,
			namespace,
			100*time.Millisecond, // 10x more frequent flushes
			3000,                 // 3x larger buffer
			flushWorkers,
			zerologLogger,
		)
		if err != nil {
			zerologLogger.Fatal().Err(err).Msg("Failed to create partitioned buffered repository")
		}
		bufferedRepo = partitionedRepo
	} else {
		// Use single-threaded repository for backwards compatibility
		bufferedRepo = buffered.NewBufferedRepository(
			postgresRepo,
			goalCache,
			namespace,
			100*time.Millisecond, // 10x more frequent flushes
			3000,                 // 3x larger buffer
			zerologLogger,
		)
	}

	defer func() {
		if err := bufferedRepo.Close(); err != nil {
			zerologLogger.Error().Err(err).Msg("Failed to close buffered repository")
		}
	}()
	zerologLogger.Info().
		Int("flush_workers", flushWorkers).
		Msg("Buffered repository initialized")

	// Create EventProcessor for routing events by goal type
	eventProcessor := processor.NewEventProcessor(bufferedRepo, goalCache, namespace, zerologLogger)
	zerologLogger.Info().Msg("Event processor initialized")

	// Register IAM Handler
	loginHandler := service.NewLoginHandler(eventProcessor, goalCache, namespace, zerologLogger)
	pb.RegisterUserAuthenticationUserLoggedInServiceServer(s, loginHandler)
	zerologLogger.Info().Msg("Login handler registered")

	// Register Statistic Handler
	statisticHandler := service.NewStatisticHandler(eventProcessor, goalCache, namespace, zerologLogger)
	statpb.RegisterStatisticStatItemUpdatedServiceServer(s, statisticHandler)
	zerologLogger.Info().Msg("Statistic handler registered")

	// Enable gRPC Reflection
	reflection.Register(s)
	zerologLogger.Info().Msg("gRPC reflection enabled")

	// Enable gRPC Health GetEventInfo
	grpc_health_v1.RegisterHealthServer(s, health.NewServer())

	prometheusGrpc.Register(s)

	// Register Prometheus Metrics
	prometheusRegistry := prometheus.NewRegistry()
	prometheusRegistry.MustRegister(
		prometheusCollectors.NewGoCollector(),
		prometheusCollectors.NewProcessCollector(prometheusCollectors.ProcessCollectorOpts{}),
		prometheusGrpc.DefaultServerMetrics,
	)

	go func() {
		mux := http.NewServeMux()
		mux.Handle(metricsEndpoint, promhttp.HandlerFor(prometheusRegistry, promhttp.HandlerOpts{}))

		// Register pprof handlers
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

		metricsServer := &http.Server{
			Addr:              fmt.Sprintf(":%d", metricsPort),
			Handler:           mux,
			ReadHeaderTimeout: 10 * time.Second,
			ReadTimeout:       30 * time.Second,
			WriteTimeout:      30 * time.Second,
			IdleTimeout:       60 * time.Second,
		}
		log.Fatal(metricsServer.ListenAndServe())
	}()
	zerologLogger.Info().Int("port", metricsPort).Str("endpoint", metricsEndpoint).Msg("serving prometheus metrics")
	zerologLogger.Info().Int("port", metricsPort).Msg("pprof endpoints available at: (:port/debug/pprof/*)")

	// Save Tracer Provider
	tracerProvider, err := common.NewTracerProvider(serviceName, environment, id)
	if err != nil {
		zerologLogger.Fatal().Err(err).Msg("failed to create tracer provider")

		return
	}
	otel.SetTracerProvider(tracerProvider)
	defer func(ctx context.Context) {
		if err := tracerProvider.Shutdown(ctx); err != nil {
			zerologLogger.Fatal().Err(err).Msg("tracer provider shutdown failed")
		}
	}(ctx)
	zerologLogger.Info().Str("service_name", serviceName).Str("environment", environment).Int64("id", id).Msg("set tracer provider")

	// Save Text Map Propagator
	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			b3.New(),
			propagation.TraceContext{},
			propagation.Baggage{},
		),
	)
	zerologLogger.Info().Msg("set text map propagator")

	// Start gRPC Server
	zerologLogger.Info().Msg("starting gRPC server")
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", grpcServerPort))
	if err != nil {
		zerologLogger.Fatal().Err(err).Int("port", grpcServerPort).Msg("failed to listen to tcp")

		return
	}
	go func() {
		if err = s.Serve(lis); err != nil {
			zerologLogger.Fatal().Err(err).Msg("failed to run gRPC server")

			return
		}
	}()
	zerologLogger.Info().Msg("gRPC server started")
	zerologLogger.Info().Msg("app server started")

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	zerologLogger.Info().Msg("signal received")
}

// initDatabase initializes the PostgreSQL database connection with connection pooling.
func initDatabase(logger zerolog.Logger) (*sql.DB, error) {
	// Get database configuration from environment variables
	dbHost := common.GetEnv("DB_HOST", "localhost")
	dbPort := common.GetEnv("DB_PORT", "5432")
	dbName := common.GetEnv("DB_NAME", "challenge_db")
	dbUser := common.GetEnv("DB_USER", "postgres")
	dbPassword := common.GetEnv("DB_PASSWORD", "postgres")

	// Build connection string
	connStr := fmt.Sprintf(
		"host=%s port=%s dbname=%s user=%s password=%s sslmode=disable",
		dbHost, dbPort, dbName, dbUser, dbPassword,
	)

	// Open database connection
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Configure connection pool
	repository.ConfigureDB(db)

	logger.Info().
		Str("host", dbHost).
		Str("port", dbPort).
		Str("database", dbName).
		Msg("Database connection configured")

	return db, nil
}
