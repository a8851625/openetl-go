// ================================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// You can delete these comments if you wish manually maintain this interface file.
// ================================================================================

package service

import (
	"context"
)

type (
	IApp interface {
		// SetupStaticFiles 配置静态文件服务（前端 UI）
		SetupStaticFiles()
		// StartETLAsync 异步启动 ETL 管道服务
		StartETLAsync(ctx context.Context)
		// Stop 优雅停止服务
		Stop()
		// WaitForShutdown 等待关闭信号并优雅关闭
		WaitForShutdown()
	}
)

var (
	localApp IApp
)

func App() IApp {
	if localApp == nil {
		panic("implement not found for interface IApp, forgot register?")
	}
	return localApp
}

func RegisterApp(i IApp) {
	localApp = i
}
