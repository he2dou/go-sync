package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/littlerivercc/go-sync/internal/config"
	"github.com/littlerivercc/go-sync/internal/node"
	"github.com/littlerivercc/go-sync/internal/svc"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/logc"
	"strings"
)

var configFile = flag.String("f", "etc/config.yaml", "the config file")
var tableList = flag.String("n", "mb_opr_account", "table list")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)
	logc.MustSetup(c.Log)

	ctx := svc.NewServiceContext(c)
	s := node.NewServer(ctx)
	//defer s.Stop()
	fmt.Printf("Starting node server ...\n")
	tables := strings.Split(*tableList, ",")
	s.Run(context.Background(), tables)
}
