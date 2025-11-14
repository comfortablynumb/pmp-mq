.PHONY: help build run test clean docker-up docker-down docker-logs fmt check

help:
	@echo "PMP-MQ Development Commands"
	@echo ""
	@echo "  make build         - Build the project"
	@echo "  make run           - Run the server (postgres backend)"
	@echo "  make test          - Run tests"
	@echo "  make fmt           - Format code"
	@echo "  make check         - Run clippy linter"
	@echo "  make clean         - Clean build artifacts"
	@echo "  make docker-up     - Start all services with docker-compose"
	@echo "  make docker-down   - Stop all docker services"
	@echo "  make docker-logs   - Show docker logs"
	@echo ""

build:
	cargo build --release

run:
	@export BACKEND_TYPE=postgres && \
	export DATABASE_URL=postgres://postgres:postgres@localhost:5432/pmp_mq && \
	export RUST_LOG=pmp_mq_server=debug,pmp_mq_core=debug,pmp_mq_backends=debug && \
	cargo run --release --bin pmp-mq-server

test:
	cargo test --all

fmt:
	cargo fmt --all

check:
	cargo clippy --all-targets --all-features

clean:
	cargo clean

docker-up:
	docker-compose up -d postgres
	@echo "Waiting for Postgres to be ready..."
	@sleep 5
	docker-compose up -d pmp-mq-postgres

docker-up-kafka:
	docker-compose up -d

docker-down:
	docker-compose down

docker-logs:
	docker-compose logs -f

docker-clean:
	docker-compose down -v
