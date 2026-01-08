run:
	go run cmd/main.go

test:
	go test -p=1 -count=1 -failfast -timeout 10m ./internal/...

lint:
	golangci-lint fmt ./internal/... ./cmd/... && golangci-lint run ./internal/... ./cmd/...

migration-create:
	goose create $(name) sql

migration-up:
	goose up

migration-down:
	goose down

swagger:
	swag init -g ./cmd/main.go -o swagger
