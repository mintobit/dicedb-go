GOLANGCI_LINT_VERSION := 1.60.1

lint:
	gofmt -w .
	golangci-lint run ./...

generate:
	protoc --go_out=. --go-grpc_out=. protos/cmd.proto

test:
	go test -v ./...
