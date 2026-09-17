# Legacy Inventory Modernization

Real-time stock availability for a retailer whose inventory lives in a legacy
SQL Server ERP that cannot be changed, built with Change Data Capture, Kafka
and the strangler fig pattern, and tested against infrastructure failures.

> **Status: in progress.** The design is in [docs/design.md](docs/design.md).
> This README becomes the case study as milestones land.

## Requirements

| To | You need |
|---|---|
| Run the system | Docker with Compose v2. Nothing else: services build inside containers |
| Develop | JDK 25. Maven is provided by `./mvnw` |

With [SDKMAN](https://sdkman.io), run `sdk env install` in the repo root. It
reads the pinned JDK from `.sdkmanrc`, the same file CI uses.

## Run it

```bash
docker compose up -d --build
```

That starts a three-node Kafka cluster, Schema Registry, SQL Server with the
legacy ERP schema and seed data, Kafka Connect with the Debezium CDC connector,
and a simulator writing stock movements. First start takes a few minutes while
images download and SQL Server initialises.

Watch change events arriving from the ERP:

```bash
docker compose exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:29092 \
  --topic erp.raw.ERPDB.dbo.INV_MOV --from-beginning --max-messages 5
```

See the translated domain events, in Avro:

```bash
docker compose run --rm --no-deps schema-registry kafka-avro-console-consumer \
  --bootstrap-server kafka-1:29092 \
  --property schema.registry.url=http://schema-registry:8081 \
  --topic inventory.stock-levels.v1 --from-beginning --max-messages 5 \
  --property print.key=true \
  --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
```

Quantities print as raw bytes there because they are Avro `decimal`, which keeps
the exact value the ERP holds; a real consumer reads them as `BigDecimal`.

Check the connector:

```bash
curl -s localhost:8083/connectors/erp-cdc/status
```

Stop everything, discarding all data:

```bash
docker compose down -v
```

Roughly 6 GB of RAM while running. See [docs/legacy-erp.md](docs/legacy-erp.md)
for how to read the legacy schema.

## Build

```bash
./mvnw verify
```

On Windows use `mvnw.cmd verify`, or work inside WSL.

The build is isolated from machine-level Maven configuration. `.mvn/settings.xml`
replaces any user settings (corporate mirrors, proxies), and the enforcer
rejects the wrong JDK or Maven version up front instead of failing later with
obscure errors.

## License

[MIT](LICENSE)
