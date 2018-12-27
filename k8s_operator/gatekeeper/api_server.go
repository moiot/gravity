package gatekeeper

import (
	"context"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	log "github.com/sirupsen/logrus"
)

type ApiServer struct {
	srv        *http.Server
	repository *Repository
}

func (s *ApiServer) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	s.srv.Handler.ServeHTTP(resp, req)
}

func NewApiServer(repo *Repository) *ApiServer {
	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()

	srv := &http.Server{
		Addr:    ":8080",
		Handler: router,
	}
	apiServer := &ApiServer{
		srv:        srv,
		repository: repo,
	}

	router.Any("/metrics", gin.WrapH(promhttp.Handler()))

	router.GET("/debug/pprof/", gin.WrapF(pprof.Index))
	router.GET("/debug/pprof/cmdline", gin.WrapF(pprof.Cmdline))
	router.GET("/debug/pprof/profile", gin.WrapF(pprof.Profile))
	router.GET("/debug/pprof/symbol", gin.WrapF(pprof.Symbol))
	router.GET("/debug/pprof/trace", gin.WrapF(pprof.Trace))
	router.GET("/debug/pprof/block", gin.WrapH(pprof.Handler("block")))
	router.GET("/debug/pprof/goroutine", gin.WrapH(pprof.Handler("goroutine")))
	router.GET("/debug/pprof/heap", gin.WrapH(pprof.Handler("heap")))
	router.GET("/debug/pprof/mutex", gin.WrapH(pprof.Handler("mutex")))
	router.GET("/debug/pprof/threadcreate", gin.WrapH(pprof.Handler("threadcreate")))

	router.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"msg": "ok"})
	})

	router.GET("/version", apiServer.list)
	router.POST("/version/:name", apiServer.create)
	router.GET("/version/:name", apiServer.get)
	router.DELETE("/version/:name", apiServer.delete)

	return apiServer
}

func (s *ApiServer) Start() {
	go func() {
		if err := s.srv.ListenAndServe(); err != nil {
			log.Infof("[ApiServer] closed with %s", err)
		}
	}()
	log.Info("[ApiServer] Started")
}

func (s *ApiServer) Stop() {
	log.Info("[ApiServer] Stopping")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.srv.Shutdown(ctx); err != nil {
		log.Error("[ApiServer] error: ", err)
	}
	log.Info("[ApiServer] Stopped")
}

func (s *ApiServer) create(c *gin.Context) {
	name := c.Param("name")
	version := Version{Name: name}
	err := s.repository.insert(&version)
	if err != nil {
		log.Errorf("[ApiServer.create] insert %#v error. %s", version, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err})
		return
	}

	c.JSON(http.StatusCreated, gin.H{"msg": "created"})
}

func (s *ApiServer) get(c *gin.Context) {
	name := c.Param("name")
	c.JSON(http.StatusOK, s.repository.get(name))
}

func (s *ApiServer) list(c *gin.Context) {
	c.JSON(http.StatusOK, s.repository.list())
}

func (s *ApiServer) delete(c *gin.Context) {
	name := c.Param("name")
	err := s.repository.delete(name)
	if err != nil {
		log.Errorf("[ApiServer.delete] delete %s error. %s", name, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err})
	} else {
		c.JSON(http.StatusOK, gin.H{"msg": "deleted version " + name})
	}
}
