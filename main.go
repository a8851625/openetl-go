package main

import (
	_ "github.com/a8851625/openetl-go/internal/packed"

	_ "github.com/a8851625/openetl-go/internal/logic"

	"github.com/gogf/gf/v2/os/gctx"

	"github.com/a8851625/openetl-go/internal/cmd"
)

func main() {
	cmd.Main.Run(gctx.GetInitCtx())
}
