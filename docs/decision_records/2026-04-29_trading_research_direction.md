# Decision Record: Trading Research Direction

Date: 2026-04-29

## Decision

QuantLab Event Scanner remains a research/data-discovery layer. It should not be
replaced by an ML system or live trading service.

The trading research target moves from event-centric descriptive analysis to
decision-time prediction:

- Old/current shape: past `%1` event -> pre-event window -> normal-time
  comparison -> metric/top-diff inspection.
- Target shape: every symbol / every decision timestamp -> as-of-safe features
  -> future 60s executable long/short label -> rule or ML scoring -> executable
  backtest -> shadow live -> controlled production.

ML is a tool for later scoring/benchmarking, not the goal of the project.

## Rationale

The current Event Scanner is useful for finding and describing market behavior,
but trading decisions need as-of-safe features, executable labels, cost-aware
expected value, and backtests that model fee, spread, slippage, and latency.
Phase 3B/3C should therefore use existing event outputs to decide metric policy
and inventory scope before Phase 4A begins.

## Consequences

- Phase 3B is descriptive content review only.
- Phase 4A decision-time labels are future work, after Phase 3B/3C.
- Live trading must be a separate service later with market-data WebSocket
  ingest, ring-buffer feature engine, scorer, order manager, Binance Futures API
  integration, user data stream reconciliation, risk manager, kill switch, and
  durable event/order logs.
- None of those live trading components are implemented now.

## Non-Claims

This decision does not validate any metric as a trading signal, does not claim
statistical significance, does not approve production readiness, and does not
authorize ML training or live order execution.
