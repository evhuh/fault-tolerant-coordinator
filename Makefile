.PHONY: proto test race vet build

# Regenerate both schemas. Run from proto/ so `source:` in the generated headers stays stable; --go_opt=module strips the package prefix so files land in proto/commandpb and proto/schedulerpb rather than a nested tree
PROTO_MODULE := github.com/evazheng/fault-tolerant-coordinator/proto

proto:
	cd proto && protoc \
		--go_out=. --go_opt=module=$(PROTO_MODULE) \
		--go-grpc_out=. --go-grpc_opt=module=$(PROTO_MODULE) \
		command.proto scheduler.proto snapshot.proto

build:
	go build ./...

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...
