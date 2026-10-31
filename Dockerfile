# One image per service: docker build --build-arg SERVICE=router -t notif-router .
ARG GO_IMAGE=golang:1.25.6-alpine3.22
FROM ${GO_IMAGE} AS build
ENV GOTOOLCHAIN=auto CGO_ENABLED=0
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG SERVICE
RUN test -n "$SERVICE" && go build -trimpath -ldflags="-s -w" -o /out/service ./cmd/${SERVICE}

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/service /service
USER nonroot:nonroot
ENTRYPOINT ["/service"]
