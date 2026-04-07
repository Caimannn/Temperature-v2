# Data Contracts

Keep the foundation small and explicit.

- `CityConfig`: city key plus display label.
- `MarketRecord`: one logged market snapshot for a city and horizon.
- `PositionAdvice`: one text-only recommendation for a market.
- `OperationMode`: manual-only or collect-only.

Storage should log every market seen during the day so later tuning can use full context.
