# Data Universe - 2026-04-29

## Collector Universe

Symbols:

- `BTCUSDT`
- `ETHUSDT`
- `BNBUSDT`
- `SOLUSDT`
- `XRPUSDT`
- `LINKUSDT`
- `ADAUSDT`
- `AVAXUSDT`
- `LTCUSDT`
- `MATICUSDT`

Exchanges:

- `binance`
- `bybit`
- `okx`

Streams:

- `bbo`
- `trade`
- `mark_price`
- `funding`
- `open_interest`

## MATIC Caveat

MATIC is problematic for the first executable research/trading universe:

- OKX has compact available data.
- Binance has `funding` and `mark_price`, but no `bbo`/`trade`.
- Bybit currently has no available MATIC partition.

The first executable research/trading universe should likely exclude MATIC
unless a separate data-quality fix is planned. This document is a caveat only;
it does not change `configs/dev.yaml`, `configs/prod.yaml`, scanning code, or
collector scope.
