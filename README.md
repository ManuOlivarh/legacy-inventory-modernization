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
