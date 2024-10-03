package job

import "github.com/google/wire"

// ProviderSet is data providers.
var ProviderSet = wire.NewSet(NewJobServer, NewKafkaReader, NewESCli)
