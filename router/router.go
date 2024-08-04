package router

import (
	"encoding/json"
	"github.com/yousuf64/chord-kv/kv"
	"github.com/yousuf64/shift"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"google.golang.org/grpc"
	"net/http"
	"strings"
)

type Router struct {
	http.Handler
}

func New(grpcs *grpc.Server, kvs kv.KV) *Router {
	router := shift.New()

	router.With(ErrorHandler)

	router.With(GrpcFilter).All("/*any", func(w http.ResponseWriter, r *http.Request, route shift.Route) error {
		grpcs.ServeHTTP(w, r)
		return nil
	})

	setHandler := otelhttp.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := SetRequest{}
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			return
		}

		err = kvs.Insert(r.Context(), req.Key, req.Value)
		if err != nil {
			return
		}
		return

	}), "Set Endpoint")

	getHandler := otelhttp.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, ok := shift.FromContext(r.Context())
		if !ok {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		value, err := kvs.Get(r.Context(), ctx.Params.Get("key"))
		if err != nil {
			return
		}

		err = json.NewEncoder(w).Encode(&GetReply{Value: value})
		if err != nil {
			return
		}
	}), "Get Endpoint")

	router.With(shift.RouteContext(), JsonResponse).Group("/api", func(g *shift.Group) {
		g.POST("/set", func(w http.ResponseWriter, r *http.Request, route shift.Route) error {
			setHandler.ServeHTTP(w, r)
			return nil
		})

		g.GET("/get/:key", func(w http.ResponseWriter, r *http.Request, route shift.Route) error {
			getHandler.ServeHTTP(w, r)
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

	return &Router{router.Serve()}
}

func ErrorHandler(next shift.HandlerFunc) shift.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request, route shift.Route) error {
		err := next(w, r, route)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(err.Error()))
		}

		return nil
	}
}

func JsonResponse(next shift.HandlerFunc) shift.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request, route shift.Route) error {
		w.Header().Set("Content-Type", "application/json")
		return next(w, r, route)
	}
}

func GrpcFilter(next shift.HandlerFunc) shift.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request, route shift.Route) error {
		if r.ProtoMajor == 2 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
			return next(w, r, route)
		}

		http.NotFoundHandler().ServeHTTP(w, r)
		return nil
	}
}
