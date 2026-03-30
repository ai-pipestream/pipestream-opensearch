# OpenSearch Manager Service

The OpenSearch Manager service is responsible for indexing repository data (drives, nodes, modules, etc.) into OpenSearch for search functionality. It consumes Kafka messages about repository updates and maintains the search index.

## Features

- **Kafka Integration**: Consumes repository update notifications from multiple Kafka topics
- **OpenSearch Indexing**: Indexes drives, nodes, modules, pipedocs, process requests/responses, and graphs
- **Service Discovery**: Automatically registers with Consul for service discovery
- **Health Checks**: Provides health check endpoints for monitoring
- **Dev Services**: Automatically starts required infrastructure in development mode

## Quick Start

### Prerequisites
- Java 21
- Docker and Docker Compose (for dev services)

### Development Mode

Use the provided start script to run in development mode with all dev services:

```bash
# From project root
./scripts/start-opensearch-manager.sh
```

**Note**: Scripts must be run from the project root directory, not from the opensearch-manager directory.

This will:
1. Build the application
2. Start all required dev services (Consul, OpenSearch, Kafka, Apicurio Registry, etc.)
3. Register the service with Consul
4. Start consuming Kafka messages

### Testing the Service

Test the service endpoints:

```bash
# From project root
./scripts/test-opensearch-manager.sh
```

### Manual Start

Alternatively, you can start manually from the project root:

```bash
# From project root
./gradlew :applications:opensearch-manager:quarkusDev
```

## Service Endpoints

When running, the service provides:

- **HTTP API**: http://localhost:38002
- **Health Check**: http://localhost:38002/q/health
- **OpenAPI/Swagger**: http://localhost:38002/q/swagger-ui
- **Dev UI**: http://localhost:38002/q/dev

For the current service defaults in this repository, see:
- **HTTP API (dev default)**: http://localhost:18103/opensearch-manager
- **Health Check**: http://localhost:18103/opensearch-manager/q/health
- **OpenAPI/Swagger**: http://localhost:18103/opensearch-manager/q/swagger-ui

## Dev Services

The following services are automatically started in development mode:

- **Consul UI**: http://localhost:8500 (Service discovery)
- **OpenSearch**: http://localhost:9200 (Search engine)
- **OpenSearch Dashboards**: http://localhost:5601 (Search UI)
- **Kafka UI**: http://localhost:8889 (Kafka management)
- **Apicurio Registry**: http://localhost:8081 (Schema registry)
- **Apicurio Registry UI**: http://localhost:8888 (Schema registry UI)
- **MinIO Console**: http://localhost:9001 (Object storage)

## Kafka Topics Consumed

The service consumes the following Kafka topics:

- `drive-updates` - Drive creation/update/deletion notifications
- `node-updates` - Node (file/folder) creation/update/deletion notifications
- `module-updates` - Module registration/update notifications
- `pipedoc-updates` - Pipeline document updates
- `process-request-updates` - Process request updates
- `process-response-updates` - Process response updates
- `graph-updates` - Pipeline graph updates

## Configuration

Key configuration properties:

### Service Registration
```properties
service.registration.enabled=true
service.registration.service-name=opensearch-manager
service.registration.host=localhost
service.registration.port=38002
```

### OpenSearch
```properties
opensearch.hosts=http://localhost:9200
opensearch.username=
opensearch.password=
```

### Kafka
```properties
kafka.bootstrap.servers=localhost:9092
kafka.group.id=opensearch-manager
```

### Apicurio Registry
```properties
mp.messaging.connector.smallrye-kafka.apicurio.registry.url=http://localhost:8081/apis/registry/v3
```

## Environment Variables

For production deployment, you can override configuration using environment variables:

- `OPENSEARCH_MANAGER_HOST` - Service hostname for registration
- `CONSUL_HOST` - Consul server hostname
- `CONSUL_PORT` - Consul server port
- `OPENSEARCH_HOST` - OpenSearch hostname
- `OPENSEARCH_PORT` - OpenSearch port
- `OPENSEARCH_USERNAME` - OpenSearch username
- `OPENSEARCH_PASSWORD` - OpenSearch password
- `KAFKA_BOOTSTRAP_SERVERS` - Kafka bootstrap servers
- `APICURIO_REGISTRY_URL` - Apicurio Registry URL
- `REDIS_HOST` - Redis hostname for distributed locking
- `REDIS_PORT` - Redis port

## Testing

Run tests with:

```bash
./gradlew test
```

The tests use Testcontainers to spin up real Kafka and OpenSearch instances for integration testing.

## Monitoring

The service provides health checks at:
- `/q/health` - Overall health
- `/q/health/live` - Liveness probe
- `/q/health/ready` - Readiness probe

## Semantic Layer Documentation

For semantic-layer onboarding and LLM-assisted edits, use:
- `docs/SEMANTIC_LAYER_END_TO_END_FOR_LLM.md` (single-page end-to-end guide)
- `docs/IMPLEMENTATION_PLAN.md` (current implementation status and open work)
- `pipestream-engine/docs/architecture/12-semantic-config-vectorset.md` (cross-repo semantic contract)

## Architecture

The service follows this flow:

1. **Kafka Consumer** - Receives repository update notifications
2. **Message Processing** - Deserializes protobuf messages using Apicurio Registry
3. **OpenSearch Indexing** - Indexes/updates/deletes documents in OpenSearch
4. **Service Registration** - Maintains registration with Consul for service discovery

## Development

### Adding New Kafka Topics

1. Add the topic configuration to `application.properties`
2. Create a consumer method in the appropriate service class
3. Update the protobuf schema in Apicurio Registry if needed

### Modifying OpenSearch Indexes

1. Update the indexing logic in `OpenSearchIndexingService`
2. Consider index migration strategies for production
3. Update health checks if needed

