package jjdiscovery

type LogLevel int

const (
	LEVEL_DEBUG LogLevel = iota
	LEVEL_INFO
	LEVEL_ERROR
)

func (l LogLevel) String() string {
	switch l {
	case LEVEL_DEBUG:
		return "DEBUG"
	case LEVEL_INFO:
		return "INFO"
	case LEVEL_ERROR:
		return "ERROR"
	}
	return ""
}

type LogFunc func(LogLevel, string)
