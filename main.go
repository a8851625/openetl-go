package main

import (
	_ "openetl-go/internal/packed"

	_ "openetl-go/internal/logic"

	"github.com/gogf/gf/v2/os/gctx"

	"openetl-go/internal/cmd"
)

func main() {
	cmd.Main.Run(gctx.GetInitCtx())
}
