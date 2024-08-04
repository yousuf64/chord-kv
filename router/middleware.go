package router

import (
	"errors"
	"fmt"
	"github.com/yousuf64/shift"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"net/http"
)

func ErrorHandler(next shift.HandlerFunc) shift.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request, route shift.Route) error {
		err := next(w, r, route)
		var errorReply *ErrorReply
		if err != nil {
			if errors.As(err, &errorReply) {
				w.WriteHeader(errorReply.Status)
			} else {
				w.WriteHeader(http.StatusInternalServerError)
			}

			unwrap := errors.Unwrap(err)
			if unwrap != nil {
				_, _ = w.Write([]byte(unwrap.Error()))
			}
		}

		return nil
	}
}

func ConsumeJson(next shift.HandlerFunc) shift.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request, route shift.Route) error {
		w.Header().Set("Content-Type", "application/json")
		return next(w, r, route)
	}
}

func OTelTrace(next shift.HandlerFunc) shift.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request, route shift.Route) error {
		ctx := r.Context()
		span := trace.SpanFromContext(ctx)
		attr := semconv.HTTPRoute(route.Path)
		span.SetAttributes(attr)

		labeler, _ := otelhttp.LabelerFromContext(ctx)
		labeler.Add(attr)

		// Set params
		route.Params.ForEach(func(k, v string) {
			span.SetAttributes(attribute.String("param."+k, v))
		})

		// Set traceparent and tracestate headers
		spanCtx := trace.SpanContextFromContext(ctx)
		traceParent := fmt.Sprintf("00-%s-%s-01", spanCtx.TraceID().String(), spanCtx.SpanID().String())
		w.Header().Set("traceparent", traceParent)
		w.Header().Set("tracestate", spanCtx.TraceState().String())

		err := next(w, r, route)
		if err != nil {
			labeler.Add(attribute.Bool("error", true))
		}

		return err
	}
}
