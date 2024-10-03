//go:build wireinject
// +build wireinject

// The build tag makes sure the stub is not built in the final build.

package main

import (
	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/wire"
	"review-job/internal/conf"
	"review-job/internal/job"
)

// wireApp init kratos application.
func wireApp(*conf.Server, *conf.Data, *conf.Kafka, *conf.ES, log.Logger) (*kratos.App, func(), error) {
	panic(wire.Build(job.ProviderSet, newApp))
}
