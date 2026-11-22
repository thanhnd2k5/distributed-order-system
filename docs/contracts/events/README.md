# Kafka Event Contracts (v1.5)

All events use the common `EventEnvelope<T>` JSON schema (`vn.edu.orders.contracts.events.EventEnvelope`) with `schemaVersion = 1`.

## Topic Topology

| Topic | Partitions | RF | Min ISR | Retention | Producer -> Consumer | Partition Key |
|---|---:|---:|---:|---:|---|---|
| `order-events.<branch>` | 2 | 3 | 2 | 7 days | Branch -> Inventory (`order-events\..*`) | `orderId` |
| `cancel-events.<branch>` | 2 | 3 | 2 | 7 days | Branch -> Kitchen (`cancel-events\..*`) | `orderId` |
| `status-events.<branch>` | 1 | 3 | 2 | 7 days | Inventory / Kitchen -> Branch | `orderId` |
| `kitchen-orders` | 2 | 3 | 2 | 7 days | Inventory -> Kitchen | `orderId` |
| `stock-commands` | 2 | 3 | 2 | 7 days | Kitchen -> Inventory | `orderId` |
| `menu-events` | 1 | 3 | 2 | 7 days | Inventory -> Branches + Kitchen | `dishId` / `ingredientId` / `heartbeat` |
| `dlq` | 1 | 3 | 2 | 7 days | Any consumer -> Operator | `orderId` (or Kafka key) |

## 13 Event Fixtures (`v1.5/`)

- `order-created.json` (`OrderCreated`)
- `cancel-requested.json` (`CancelRequested`)
- `order-reserved.kitchen.json` & `order-reserved.branch.json` (`OrderReserved`)
- `order-failed.json` (`OrderFailed`)
- `stock-consumed.json` (`StockConsumed`)
- `release-requested.json` (`ReleaseRequested`)
- `cancel-pending.json` (`CancelPending`)
- `cancel-rejected.json` (`CancelRejected`)
- `order-cancelled.json` (`OrderCancelled`)
- `order-completed.json` (`OrderCompleted`)
- `dish-status-changed.json` (`DishStatusChanged`)
- `ingredient-check-changed.json` (`IngredientCheckChanged`)
- `heartbeat.json` (`Heartbeat`)
