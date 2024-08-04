package router

import (
	"encoding/json"
	"errors"
	"github.com/yousuf64/chord-kv/errs"
	"github.com/yousuf64/chord-kv/kv"
	"github.com/yousuf64/shift"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"google.golang.org/grpc"
	"net/http"
	"strings"
)

type Router struct {
	HttpHandler http.Handler
	GrpcHandler http.Handler
}

func New(grpcs *grpc.Server, kvs kv.KV) *Router {
	r := shift.New()
	r.Use(ConsumeJson, ErrorHandler, OTelTrace)
	r.Group("/api", func(g *shift.Group) {
		g.POST("/set", func(w http.ResponseWriter, r *http.Request, route shift.Route) error {
			req := SetRequest{}
			err := json.NewDecoder(r.Body).Decode(&req)
			if err != nil {
				return errors.Join(err, &ErrorReply{
					Status: http.StatusBadRequest,
				})
			}

			err = kvs.Insert(r.Context(), req.Key, req.Value)
			if err != nil {
				if errors.Is(err, errs.AlreadyExistsError) {
					return &ErrorReply{
						Status: http.StatusBadRequest,
					}
				}

				return err
			}

			w.WriteHeader(http.StatusCreated)
			return nil
		})

		g.GET("/get/:key", func(w http.ResponseWriter, r *http.Request, route shift.Route) error {
			value, err := kvs.Get(r.Context(), route.Params.Get("key"))
			if err != nil {
				if errors.Is(err, errs.NotFoundError) {
					return &ErrorReply{
						Status: http.StatusNotFound,
					}
				} else {
					return err
				}
			}

			err = json.NewEncoder(w).Encode(&GetReply{Value: value})
			if err != nil {
				return err
			}
			return nil
		})

		g.GET("/dump", func(w http.ResponseWriter, r *http.Request, route shift.Route) error {
			_, err := w.Write([]byte(kvs.Dump()))
			if err != nil {
				return err
			}

			return nil
		})
	})

	return &Router{
		HttpHandler: otelhttp.NewHandler(
			r.Serve(),
			"http-server",
			otelhttp.WithMessageEvents(otelhttp.ReadEvents, otelhttp.WriteEvents)),
		GrpcHandler: grpcs,
	}
}

func (router *Router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.ProtoMajor == 2 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
		router.GrpcHandler.ServeHTTP(w, r)
		return
	}

	router.HttpHandler.ServeHTTP(w, r)
}
