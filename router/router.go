package router

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/yousuf64/chord-kv/errs"
	"github.com/yousuf64/chord-kv/kv"
	"github.com/yousuf64/shift"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"google.golang.org/grpc"
	"log"
	"math/big"
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

			err = kvs.Insert(r.Context(), req.Key, req.Content)
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
			_, err := kvs.Get(r.Context(), route.Params.Get("key"))
			if err != nil {
				if errors.Is(err, errs.NotFoundError) {
					return &ErrorReply{
						Status: http.StatusNotFound,
					}
				} else {
					return err
				}
			}

			size, hash := generateContent()

			err = json.NewEncoder(w).Encode(&GetReply{Size: size, Hash: hash})
			if err != nil {
				return err
			}
			return nil
		})

		g.GET("/debug", func(w http.ResponseWriter, r *http.Request, route shift.Route) error {
			_, err := w.Write([]byte(kvs.Debug()))
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

func generateContent() (int64, string) {
	// Generate a random integer between 2MB and 10MB
	minSize := 2 * 1024 * 1024  // 2MB
	maxSize := 10 * 1024 * 1024 // 10MB
	size, err := rand.Int(rand.Reader, big.NewInt(int64(maxSize-minSize)))
	if err != nil {
		log.Fatalf("Failed to generate random size: %v", err)
	}
	size = size.Add(size, big.NewInt(int64(minSize)))

	// Create a byte slice of the generated size and fill it with random data
	data := make([]byte, size.Int64())
	_, err = rand.Read(data)
	if err != nil {
		log.Fatalf("Failed to generate random data: %v", err)
	}

	// Calculate the SHA-256 hash of the data
	hash := sha256.Sum256(data)
	hashString := hex.EncodeToString(hash[:])

	return size.Int64(), hashString
}

func (router *Router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.ProtoMajor == 2 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
		router.GrpcHandler.ServeHTTP(w, r)
		return
	}

	router.HttpHandler.ServeHTTP(w, r)
}
