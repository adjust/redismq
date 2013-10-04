package redismq

import (
	"github.com/adeven/goenv"
	"log"
	"net/http"
)

type Server struct {
	goenv    *goenv.Goenv
	port     string
	Overseer *Overseer
}

func NewServer(goenv *goenv.Goenv, overseer *Overseer) *Server {
	s := &Server{
		port:     goenv.GetPort(),
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
