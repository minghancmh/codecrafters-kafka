package logger

import (
	"fmt"
	"runtime"
	"strings"
)

func Log(format string, a ...interface{}) {
	pc, _, _, _ := runtime.Caller(1)
	funcName := runtime.FuncForPC(pc).Name()
	shortName := funcName[strings.LastIndex(funcName, ".")+1:]
	logLine := fmt.Sprintf("[%s]: %s", shortName, fmt.Sprintf(format, a...))
	fmt.Println(logLine)
}
