package mantle

import (
	"fmt"
	"github.com/getsentry/sentry-go"
	sentryhttp "github.com/getsentry/sentry-go/http"
	"github.com/minio/minio/cmd/mantle/gateway"
)

var SentryHttpHandler *sentryhttp.Handler

func init() {
	// To initialize Sentry's handler, you need to initialize Sentry itself beforehand
	if err := sentry.Init(sentry.ClientOptions{
		Dsn:         gateway.MantleConfig.SentryDNS,
		Environment: gateway.MantleConfig.SentryEnv,
	}); err != nil {
		fmt.Printf("Sentry initialization failed: %v\n", err)
	}

	SentryHttpHandler = sentryhttp.New(sentryhttp.Options{Repanic: true})
}
