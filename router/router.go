package router

import (
	"context"
	"encoding/json"
	"github.com/yousuf64/chord-kv/kv"
	"github.com/yousuf64/shift"
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

	router.With(JsonResponse).Group("/api", func(g *shift.Group) {
		g.POST("/set", func(w http.ResponseWriter, r *http.Request, route shift.Route) error {
			req := SetRequest{}
			err := json.NewDecoder(r.Body).Decode(&req)
			if err != nil {
				return err
			}

			err = kvs.Insert(context.Background(), req.Key, req.Value)
			if err != nil {
				return err
			}
			return nil
		})

		g.GET("/get/:key", func(w http.ResponseWriter, r *http.Request, route shift.Route) error {
			value, err := kvs.Get(context.Background(), route.Params.Get("key"))
			if err != nil {
				return err
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
