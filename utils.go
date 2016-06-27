package bolt_wrap

import (
	"github.com/ivahaev/go-logger"
)

func ErrorHandler(v interface{}) {
	if v != nil {
		logger.Error(v)
	}
}
