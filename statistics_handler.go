package redismq

import (
	"fmt"
	"net/http"
)

type StatisticsHandler struct {
	overseer *Overseer
}

func NewStatisticsHandler(overseer *Overseer) *StatisticsHandler {
	handler := &StatisticsHandler{
		overseer: overseer,
	}
	return handler
}

func (self *StatisticsHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	fmt.Fprintln(writer, self.overseer.OutputToString())
}
