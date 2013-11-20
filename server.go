package redismq

import (
	"fmt"
	"log"
	"net/http"
)

// Server is the web server API for monitoring via JSON
type Server struct {
	port     string
	observer *Observer
}

// NewServer returns a Server that can be started with Start()
func NewServer(redisHost, redisPort, redisPassword string, redisDb int64, port string) *Server {
	observer := NewObserver(redisHost, redisPort, redisPassword, redisDb)
	s := &Server{
		port:     port,
		observer: observer,
	}
	return s
}

func (server *Server) setUpRoutes() {
	http.Handle("/stats", newStatisticsHandler(server.observer))
}

// Start enables the Server to listen on his port
func (server *Server) Start() {
	go func() {
		server.setUpRoutes()
		log.Printf("STARTING REDISMQ SERVER ON PORT %s", server.port)
		err := http.ListenAndServe(":"+server.port, nil)
		if err != nil {
			log.Fatalf("REDISMQ SERVER SHUTTING DOWN [%s]\n\n", err.Error())
		}
	}()
}

type statisticsHandler struct {
	*Observer
}

func newStatisticsHandler(observer *Observer) *statisticsHandler {
	handler := &statisticsHandler{
		Observer: observer,
	}
	return handler
}

func (handler *statisticsHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	handler.Observer.UpdateAllStats()
	fmt.Fprintln(writer, handler.Observer.ToJSON())
}
