package main

import (
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/ozontech/seq-db/storeapi"
)

const (
	appModeProxy  = "proxy"
	appModeStore  = "store"
	appModeSingle = "single"
)

var (
	flagConfig = kingpin.Flag("config", "path to config file").
			Default("config.yaml").
			ExistingFile()

	flagMode = kingpin.Flag("mode", `operation mode`).
			Default(appModeSingle).
			HintOptions(appModeSingle, appModeProxy, appModeStore).
			String()

	flagStoreMode = kingpin.Flag("store-mode", `store operation mode`).
			HintOptions("", storeapi.StoreModeCold, storeapi.StoreModeHot).
			Default("").
			String()
)
