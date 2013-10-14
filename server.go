package redismq

import (
	"log"
	"net/http"
)

type Server struct {
	port     string
	Overseer *Overseer
}

func NewServer(port string, overseer *Overseer) *Server {
	s := &Server{
		port:     port,
		Overseer: overseer,
	}
	return s
}

func (self *Server) SetUpRoutes() {
	http.Handle("/stats", NewStatisticsHandler(self.Overseer))
}

func (self *Server) Start() {
	self.SetUpRoutes()
	log.Printf("STARTING REDISMQ SERVER ON PORT %s", self.port)
	err := http.ListenAndServe(":"+self.port, nil)
	if err != nil {
		log.Fatalf("REDISMQ SERVER SHUTTING DOWN [%s]\n\n", err.Error())
	}
}
