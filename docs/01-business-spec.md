# Business specification v1.5 – Order cancellation, ticket rejection and sold-out reporting

v1.5 · 21 Nov 2026 · @Naul

> From v1.5 this Markdown file is the master copy of the specification, replacing the v1.4 PDF of 19 Nov 2026. It was written in Vietnamese up to v1.5 and translated into English on 21 Nov 2026 with no change in content. The architecture diagram is an image in the PDF, so it is described here in text.

**v1.5 (21 Nov 2026), revised after the third review round:** adds a rule on when the sender starts attempting to send, fixes three places where the specification contradicted itself, adds the rows that were missing from table 3.2, table 3.3 and Flow D, and adds 8 scenarios (48–55). No BR rule changes direction.

| Section | Change in v1.5 | Reason |
|---|---|---|
| 2 (BR5, BR9), 9 | The sender only starts attempting to send an entry when a connectivity probe to Kafka has just succeeded. The label "Not sent, kitchen has not received it" is used only for entries that have not been attempted | Without this rule, an order created while offline still gets the flag, and scenario 10 fails |
| 1 (Terminology), 9, 12 | Two-layer deduplication; definition of "return the stored result" | The old sentence "deduplicate by order ID and message type" made the origin branch's cancel request count as a duplicate of a wrong-branch request that arrived first, contradicting scenario 35 |
| 1, 14 | States the model of one central Kitchen and one central Inventory; fixes the sentence "the physical ingredients are at another branch" | The two old sentences contradicted each other |
| 15 | A stuck reservation cannot be detected by the invariant queries | When a release command is lost, all three invariants still hold |
| 3.2 | Adds the order of precedence when several rows of the table match | A Completed ticket matched two rows that gave two different results |
| 3.3 | Adds the cell "Held, duplicate order"; splits the duplicate-message row; a command reaching a Failed record is logged | The table was missing one cell and had one cell that could be read two ways |
| 3.4 | Removing a dish means deleting its recipe | There was no Failed reason for a removed dish |
| 8 | Adds four rows to the Flow D table | These alerts exist in sections 9, 10 and 13.2 but had no viewer |
| 12 | Adds scenarios 48–55; adds a precondition to scenarios 10 and 27; rewrites scenario 12; states that the stock-check flag messages are built in phase 1 | These behaviours had no scenario testing them |
| Diagram, 10, 13.5, 15 | Partition key of `menu-events`; three parameters named N; Inventory down for longer than X; orders with a message in `dlq` | Wording clarified |

**v1.4 (20 Nov 2026), lean edition for the course project:** keeps all three business flows A, B and C; drops the production-grade infrastructure and operations parts so that the work fits implementation and a demo. Dropped: synchronous replicas of the Inventory DB and Kitchen DB, authentication and ACLs on Kafka, the quarantine queue, TTL-based record cleanup, reconciliation after G hours, and the operation that closes an order with a message in `dlq`. Section 15 records each dropped part, its consequence and the path to extend it; v1.3 is the full edition for reference. The diagram was redrawn, section 13.6 states which problem each component serves, and section 12 has 36 scenarios left.

**Earlier versions:** v1.1 added deployment assumptions for Kafka and PostgreSQL. v1.2 and v1.3 were revised after two review rounds: local cancel of an order that was never attempted, the Kitchen recording the Failed result and the origin branch, Inventory refusing to reserve an ingredient awaiting stock check, and the stock accounting table.

## Architecture and message flows

v1.4 architecture (lean edition): message flows between the branches, Inventory, the Kitchen and Management. The table below lists the components on the original diagram; a component marked "new" was added on top of the initial architecture.

| Block | Component | Note on the diagram |
|---|---|---|
| Order sources: POS site × N (POS01 … POSN) | Local Order Service, 1 per site | Accepts orders even when offline |
| Order sources: POS site × N | Local Order DB | Orders, send queue (outbox), dish status cache (new) |
| Order sources: Online site (web, app) | Online Order Service, 1 instance | Shares the same flow as POS |
| Order sources: Online site | Online Order DB | Orders, send queue (outbox), dish status cache (new) |
| Data centre (reporting) | Central Order Replica | Copy of the order DBs, fed by asynchronous replication from the Local Order DB and the Online Order DB |
| Data centre (reporting) | Order Query Service | |
| Data centre (reporting) | Management dashboard | Shows the "data as of" marker (new) |
| Kafka, 3 brokers | Common configuration | `acks=all`, 3 replicas per message, `min.insync` 2, 7-day message retention |
| Kafka, per-branch topics | `order-events.<branch>` | New orders |
| Kafka, per-branch topics | `cancel-events.<branch>` (new) | Cancel requests |
| Kafka, per-branch topics | `status-events.<branch>` | Order statuses of the branch |
| Kafka, internal topics | `kitchen-orders` (new) | Reserved orders, failed orders |
| Kafka, internal topics | `stock-commands` (new) | Ingredient release and consumption |
| Kafka, internal topics | `menu-events` (new) | Dish status, heartbeat |
| Kafka, internal topics | `dlq` (new) | Unreadable messages; does not block the consumer |
| Inventory, shared by the whole chain | Inventory Service, 2 instances | Dish status snapshot API (new) |
| Inventory | Inventory DB | Chain-wide stock, reservations by order ID; stock-check flag, version seq, `processed_events` (new); `outbox_events` |
| Kitchen | Kitchen Service, 1 instance | The only party that decides cancellation (new) |
| Kitchen | Kitchen DB | Kitchen tickets, one record per order ID; Cancel-pending, origin branch, `processed_events` (new); `outbox_events` |

The message flows on the diagram, all of which go through Kafka:

- A branch sends new orders and cancel requests; a branch receives order statuses and dish statuses.
- Inventory receives new orders and release commands or consumption reports; Inventory sends order statuses, orders for the Kitchen and dish statuses.
- The Kitchen receives cancel requests and orders from Inventory; the Kitchen sends order statuses and release commands or consumption reports.

Every message between sites goes through Kafka and is written through an outbox. An order travels `order-events.<branch>` → Inventory → `kitchen-orders` → Kitchen, while a cancel request travels `cancel-events.<branch>` straight to the Kitchen, so it can arrive before the order (BR7). A branch receives statuses only through its own `status-events.<branch>`.

The partition key is the order ID (only `menu-events` uses the dish ID or ingredient ID), so ordering is preserved only within one topic, not across two topics. Topics are split per branch so that each branch reads only its own status messages and so that the Kitchen knows which branch sent a cancel request; the lean edition has no authentication and no ACLs (section 13.2). POS and Online fetch the dish snapshot through the Inventory Service API when they reconnect (BR10).

## 1. Purpose and scope

Version v1.0 added three business flows (the Kitchen rejects a ticket, cancelling an order on request, sold-out reporting) and one monitoring flow. The goal is to close a gap in the base flow: once Inventory has reserved ingredients, they cannot be taken back if the order is not cooked. This document is the only source for implementation; every value marked "proposed" is one the team can still change.

**Scope assumptions:** each order has exactly one kitchen ticket; cancellation and rejection always apply to the whole order, and there is no partial cancellation. Inventory is shared by the whole chain: stock and dish status are computed chain-wide and are the same at every branch. The system has one physical Kitchen and one physical Inventory at the centre, serving every branch; a branch is only a place that takes orders.

### Parties

| Party | Role | Decision authority |
|---|---|---|
| Branch (POS, Online) | Creates orders, sends cancel requests, displays order and dish status | Decides a status on its own only for an order whose create-order entry was never attempted (BR9) |
| Inventory | Reserves, releases and writes off ingredients as waste; publishes dish status | Final decision on ingredients and on accepting an order |
| Kitchen | Receives tickets, cooks, rejects tickets | The only party that decides order cancellation |
| Management | Views chain-wide aggregate figures | Does not change data; figures may lag |

### Terminology

- **Reserve:** Inventory temporarily locks ingredients for an order and records the quantity of each.
- **Release:** return the reserved quantity to available stock.
- **Waste:** write off the reserved quantity without returning it to available stock.
- **Consumed:** the reserved quantity counts as used because the dish has started cooking.
- **Cancel-pending:** the record the Kitchen creates when a cancel request arrives before the order. The record stores every branch that has sent a cancel request.
- **Send queue:** where POS durably stores the entries (create order, cancel request) that Kafka has not yet acknowledged.
- **Send attempted:** a durable flag on each entry in the send queue, written before the first send. An entry without this flag has certainly not reached Kafka. The sender writes the flag only when a connectivity probe to Kafka has just succeeded (BR9).
- **Return the stored result:** publish a new message carrying the result that corresponds to the order's stored status, without repeating any effect: no second reservation, no second release, no second ticket.
- **Version (epoch, seq):** a pair of numbers used to compare how recent a dish status is. `epoch` is the generation number of Inventory's data, `seq` is a durable sequence number in Inventory.
- **Snapshot:** all dish statuses at one point in time, with the version of each dish.
- **Heartbeat:** a periodic signal Inventory sends to show that it is still reachable, carrying the current `epoch`.

## 2. Business rules

BR1–BR5 are unchanged from the base flow (BR3 and BR4 are stated more precisely). BR6–BR12 are the new rules of v1.0, revised through two review rounds. The lean edition v1.4 shortens BR5, BR8, BR9, BR11 and BR12.

| ID | Rule |
|---|---|
| BR1 | Inventory reserves all ingredients of an order or none. |
| BR2 | The Kitchen cooks only orders for which Inventory has reserved ingredients. |
| BR3 | Order status never goes backwards. A branch accepts only a message whose rank is higher than the current rank (section 3.1); a terminal status is never replaced. The "Cancel requested" label is an auxiliary flag, not a status. |
| BR4 | Resending the same message produces no second effect. This applies to every receiver (Inventory, the Kitchen, the branch) and every message type (create order, cancel request, release command, consumption report, result). |
| BR5 | A branch that is offline still records new orders and cancel requests in the send queue, but cannot serve them yet: the Kitchen is central, so an order created while offline is not cooked until the network returns. A new order is subject to the X-hour threshold (BR11); past it, the order is Failed. For a create-order entry that has not been attempted, POS shows "Not sent, kitchen has not received it"; for an entry that has been attempted but not yet acknowledged by Kafka, POS shows "Sending, unknown whether the kitchen has received it". A cancel request has two cases. Create-order entry never attempted: POS cancels locally and the order is certainly not cooked (BR9). Create-order entry attempted: the cancel request is sent to the Kitchen and retried indefinitely on network errors, but success is not certain because the order may already have been cooked; POS shows "Cancel request not sent yet, cancellation not guaranteed". |
| BR6 | Inventory handles a release command according to its reason. An ingredient reported as spoiled is written off as waste, flagged for stock check and temporarily treated as Sold out; the other ingredients of the same order are released. Inventory does not reserve a flagged ingredient for new orders, even if available stock is sufficient: an order that needs that ingredient gets Failed (ingredient awaiting stock check), and the check happens in the same transaction as the reservation. Inventory releases the quantity recorded at reservation time and does not recompute it from the recipe. A ticket carries the list of reserved ingredients; the Kitchen shows a warning on Queued tickets that contain a flagged ingredient. |
| BR7 | The Kitchen is the only party that decides cancellation for an order that has been attempted. The Kitchen does not finalise Cancelled for an order that has not arrived: it creates a Cancel-pending record and returns the commitment "kitchen will not cook". The commitment holds only when the branch requesting cancellation really is the order's origin branch (section 13.2). Between Cancel and Start cooking, whichever operation the Kitchen records first wins. |
| BR8 | The Kitchen keeps exactly one record per order ID, including orders that Inventory reports as Failed. Records are never deleted, so a Cancel-pending record is valid indefinitely and a duplicate message always gets the stored result back. |
| BR9 | POS validates order data before accepting it: an order with invalid data is rejected immediately (HTTP 400) and does not enter the send queue. POS preserves send order per order ID, not across the whole queue, and durably writes the "send attempted" flag before the first send of each entry. The sender only starts attempting to send an entry, that is, writes the flag and then sends, when a connectivity probe to Kafka has just succeeded; if it cannot connect, the entry stays not attempted and can still be cancelled locally. The connectivity probe only decides when attempting starts; whether a local cancel is allowed is still decided by the flag. POS decides a status on its own in one case only: a cancel request arrives while the create-order entry has never been attempted, in which case POS closes the order as Cancelled and discards the entry. An entry that has been attempted is never cancelled locally and never discarded, even if that send reported an error: every send error is retried with increasing backoff. Kafka acknowledging an entry does not mean Inventory has received the order. Send order at POS does **not** guarantee the order in which the Kitchen receives messages. |
| BR10 | Inventory publishes a message only when a dish changes between Available, Sold out and Hidden. Each message carries a version (epoch, seq) taken from a durable sequence number, not from the system clock. Inventory sends a periodic heartbeat carrying the epoch. POS merges each dish by version and does not clear the cache to reload it; when it sees a new epoch, POS fetches the snapshot and then moves the dishes still carrying the old generation to Hidden. |
| BR11 | Inventory returns Failed and reserves no ingredients for an order created more than X hours ago, or whose creation time is in the future beyond the tolerance. |
| BR12 | Inventory keeps exactly one result record per order ID, including orders that Failed because of invalid data, so that a resent order is never evaluated again. Records are never deleted. Inventory moves a reservation to Consumed when the Kitchen reports Cooking; it releases only while the reservation is still Held. Each reservation leaves Held exactly once (section 3.3). A release command that matches no reservation record is ignored and logged, and is never added to stock. |

BR12 was added when this version was compiled; see section 11.

## 3. State machines

There are four independent state machines: the order at the branch, the order record at the Kitchen (which includes the ticket), the order record at Inventory, and dish status. Any transition that is not in the tables is rejected.

### 3.1 Order at the branch

Ranks: Awaiting Inventory = 0, Reserved = 1, Completed / Cancelled / Failed = 2 (terminal statuses). A message whose rank is lower than or equal to the current rank is ignored. An order can skip a rank, for example from Awaiting Inventory to Cancelled when the Reserved message arrives later or is lost.

| From | Event | To | Sender |
|---|---|---|---|
| (none) | Cashier or customer submits an order | Awaiting Inventory | POS, Online |
| Awaiting Inventory | Inventory reserves all ingredients | Reserved | Inventory |
| Awaiting Inventory | Inventory rejects (reasons in section 10) | Failed | Inventory |
| Awaiting Inventory, Reserved | Kitchen confirms the cancellation or rejects the ticket | Cancelled | Kitchen |
| Awaiting Inventory, Reserved | Kitchen reports completion | Completed | Kitchen |
| Awaiting Inventory | Cancel request while the create-order entry has never been attempted | Cancelled (customer cancel) | POS |

The auxiliary "Cancel requested" flag:

- Turned on when the cashier or customer sends a cancel request.
- Gains the line "kitchen will not cook" when the Kitchen confirms that it has created a Cancel-pending record.
- Turned off when the Kitchen refuses the cancellation (showing the message "Cannot cancel: the kitchen has started cooking") or when the order reaches a terminal status.

### 3.2 Order record at the Kitchen

Each order ID has exactly one record (BR8). A "ticket" is a record in the Queued, Cooking or Completed status. Terminal statuses: Completed, Cancelled, Failed. Every transition must be a conditional write on the current status. The `CHUA_CO` row in section 13.4 is treated as "(none)". The "origin branch" is the branch that created the order, passed along by Inventory; a Cancel-pending record stores the list of branches that have sent a cancel request.

| From | Event | To | Kitchen also does |
|---|---|---|---|
| (none) | Order arrives from Inventory | Queued | Shows the ticket with the list of reserved ingredients |
| (none) | Cancel request arrives | Cancel-pending | Records the sending branch; returns the commitment "kitchen will not cook" to the branch |
| (none) | Inventory reports Failed | Failed | Records the final result, so that a cancel request arriving later does not create a Cancel-pending record |
| Cancel-pending | Cancel request from a branch not yet in the list | (unchanged) | Adds the branch to the list; returns the commitment "kitchen will not cook" |
| Cancel-pending | Order arrives from Inventory, origin branch is in the list | Cancelled (customer cancel) | Sends Inventory a release command; reports Cancelled to the branch |
| Cancel-pending | Order arrives from Inventory, origin branch is not in the list | Queued | Dismisses the recorded cancel requests and raises a warning (section 13.2); handles the order as if there were no record |
| Cancel-pending | Inventory reports Failed | Failed | Nothing more |
| Queued | Staff press Start cooking | Cooking | Reports Consumed to Inventory |
| Queued | Cancel request arrives from the origin branch | Cancelled (customer cancel) | Sends Inventory a release command; reports Cancelled to the branch |
| Queued | Staff press Reject with a reason | Cancelled (Kitchen rejection) | Sends Inventory a release command with the reason (BR6); reports Cancelled to the branch |
| Cooking | Staff press Complete | Completed | Reports Completed to the branch |
| Cooking, Completed | Cancel request arrives from the origin branch | (unchanged) | Returns "Cannot cancel: the kitchen has started cooking" to the branch |
| Queued, Cooking or a terminal status | Cancel request from a branch other than the origin branch | (unchanged) | Ignores the request and raises a warning (section 13.2) |
| Queued, Cooking | Duplicate order arrives from Inventory | (unchanged) | Ignores it; no second ticket is created |
| Cancel-pending or a terminal status | Duplicate message (order, cancel request, Failed) | (unchanged) | Returns the stored result; no second release command is sent |

When several rows match one message, apply them in this order:

1. A cancel request from a branch other than the origin branch: ignore it and raise a warning, in every status where the origin branch is known.
2. The row that names the status and the event explicitly. For example, a cancel request from the origin branch reaching a Completed ticket gets "Cannot cancel: the kitchen has started cooking", not the Completed result again.
3. The last row of the table, for every remaining message reaching a Cancel-pending record or a terminal status. This row also applies to the first cancel request from the origin branch reaching a Cancelled or Failed record, even though that is not a duplicate message.

### 3.3 Order record at Inventory

Each order ID has exactly one record (BR12). The record stores the reserved quantity of each ingredient and the outcome of each line.

| From | Event | To | Inventory also does |
|---|---|---|---|
| (none) | Order arrives, within the time limit, with enough ingredients and no ingredient flagged for stock check | Held | Reports Reserved to the branch; forwards the order with the list of reserved ingredients to the Kitchen |
| (none) | Order arrives with insufficient ingredients, an ingredient flagged for stock check, a dish that has no recipe, invalid order data, past the time limit or with a creation time in the future | Failed | Reports Failed with the reason to both the branch and the Kitchen |
| Held | Release command from the Kitchen | Released | For each line: release, or write off as waste if it is an ingredient reported as spoiled (BR6) |
| Held | Kitchen reports Consumed | Consumed | Moves the reserved quantity to Consumed; does not deduct available stock a second time |
| Held, Failed, Released, Consumed | Duplicate order arrives | (unchanged) | Returns the stored result to the branch; no second reservation, and the order is not forwarded to the Kitchen a second time |
| Released, Consumed | Release command or Consumed report | (unchanged) | Ignored; no second release and no second consumption |
| Failed or (no record) | Release command or Consumed report | (unchanged or not created) | Ignored and logged; nothing is added to stock |

#### Stock accounting

Each ingredient has four quantities: **available stock** (the `available` column, still reservable for new orders), **reserved**, **consumed** and **waste** (the last two are cumulative totals). On-hand stock equals available stock plus reserved. Each operation changes the quantities only as shown in the table below, where q is the quantity recorded at reservation time.

| Operation | Available stock | Reserved | Consumed | Waste |
|---|---|---|---|---|
| Reserve for an order | − q | + q | unchanged | unchanged |
| Release | + q | − q | unchanged | unchanged |
| Kitchen reports Consumed | unchanged | − q | + q | unchanged |
| Write off as waste (spoiled ingredient) | unchanged | − q | unchanged | + q |
| Stock receipt, adjustment after a stock check | add or subtract the adjustment quantity | unchanged | unchanged | unchanged |

Example: Inventory has 10 kg of meat available and an order needs 2 kg. After the reservation, 8 kg is available and 2 kg is reserved. When the Kitchen starts cooking, available stays at 8 kg and the 2 kg moves to consumed. If the customer cancels before cooking, available goes back to 10 kg.

Invariants that must be checkable by query: available stock is never negative; the reserved quantity of an ingredient equals the sum of the recorded quantities of the records still Held; each reservation leaves Held exactly once, to exactly one of three destinations: released, consumed or waste. Flagging for stock check changes no quantity, but blocks every new reservation of that ingredient (BR6).

### 3.4 Dish status

Each dish has one of three statuses, always with a version (epoch, seq). Versions are compared lexicographically: epoch first, then seq if the epochs are equal.

| Status | Condition at Inventory | What POS and Online show |
|---|---|---|
| Available | Enough available ingredients for at least 1 portion and no ingredient flagged for stock check | Selectable |
| Sold out | Not enough for 1 portion, or an ingredient is flagged for stock check | Blocked or warned (section 7) |
| Hidden | The dish has no recipe or has been removed | Not shown |

Removing a dish means deleting its recipe. An order containing a removed dish therefore gets Failed with the reason "Dish has no recipe", even if POS still shows that dish because of stale data.

A dish that is Sold out because an ingredient is flagged is not just a hint for the user interface: Inventory refuses to reserve that ingredient for every new order, including orders placed while POS was offline or using stale data (BR6).

## 4. Base flow: placing an order

The base flow is unchanged; only the BR11 and BR12 checks at Inventory are added.

1. The cashier or customer selects dishes and submits the order. POS validates the order data and rejects it immediately if it is invalid (HTTP 400). For a valid order, POS generates a unique order ID, records the creation time and stores the order in the Awaiting Inventory status in the send queue, even while offline.
2. Inventory receives the order. If the order ID already has a record, Inventory returns the stored result and stops (BR12).
3. Inventory checks the time limit: an order created more than X hours ago, or with a creation time in the future beyond the tolerance, is Failed (BR11).
4. Inventory computes the total ingredients from the current recipes. If all are sufficient and no ingredient is flagged for stock check, it reserves them, records the quantity of each and reports Reserved. If even one is insufficient, an ingredient is flagged (BR6), or a dish has no recipe, it reserves nothing and reports Failed (BR1). An order that is readable but has invalid data (a missing required field, a non-positive quantity) also gets Failed and does not go to `dlq` (section 9).
5. Inventory sends the result back to the branch that created the order. A Failed result is also sent to the Kitchen. A Reserved order is forwarded to the Kitchen with the list of reserved ingredients.
6. The Kitchen creates a Queued ticket (BR2). When staff press Start cooking, the ticket moves to Cooking and the Kitchen reports Consumed to Inventory.
7. When staff press Complete, the ticket and the order move to Completed.
8. Management views chain-wide aggregate figures, which may lag behind the data at the branches.

## 5. Flow A: Kitchen rejects a ticket

The Kitchen can reject only a ticket that is Queued. Inventory releases or writes off as waste depending on the reason.

| Item | Content |
|---|---|
| Actor | Kitchen staff |
| Condition | The ticket is Queued |
| Situations | Equipment failure, short-staffed, an ingredient spoiled after it was reserved |

### Steps

1. Staff select the ticket, press Reject and choose a reason from the fixed list (section 10). For the reason "Spoiled ingredient", staff must also choose which ingredient is spoiled, from the ticket's list of reserved ingredients.
2. The Kitchen moves the ticket to Cancelled (Kitchen rejection) with a conditional write. If the ticket is no longer Queued, the operation is rejected.
3. The Kitchen reports the order as Cancelled, with the reason, to the branch. The branch shows it immediately, without waiting for Inventory.
4. The Kitchen sends Inventory a release command with the reason and the spoiled ingredient (if any).
5. Inventory handles it according to BR6:
   - Equipment failure, short-staffed: release the whole reserved quantity.
   - Spoiled ingredient: write off the reserved quantity of that ingredient as waste, release the remaining ingredients, and flag the spoiled ingredient for stock check.
6. If an ingredient is flagged: every dish that uses that ingredient moves to Sold out (Flow C), and the Kitchen shows a red warning on the Queued tickets that contain that ingredient. From this point on, Inventory refuses to reserve that ingredient for new orders (BR6).

### Rules

- A ticket that is already Cooking cannot be rejected; it must be completed. Consumed ingredients are not released.
- Inventory releases the quantity recorded at reservation time and does not recompute it from the current recipe.
- Each order is released only once, even if the release command is resent (BR4, BR12).
- If Inventory is down, the order still shows Cancelled immediately. The release command is stored durably and is certain to be processed when Inventory comes back.
- A warning on a ticket is only a warning: the system does not cancel the ticket by itself. If staff check and find that the ingredient really is spoiled, they reject the ticket through this same flow.
- The warning and the Sold out status are cleared when Inventory confirms that the stock check is done. The lean edition does this through an Inventory Service API; clearing the flag also allows Inventory to reserve that ingredient again.

## 6. Flow B: Cancelling an order on request

A branch only sends a cancel request; the Kitchen is the only party that decides (BR7). The single exception is an order whose create-order entry has never been attempted: that order has certainly not entered Kafka, so POS cancels it on the spot (BR9).

| Item | Content |
|---|---|
| Requester | Cashier (POS) or customer (Online), sharing the same flow |
| Decider | The Kitchen, because only the Kitchen knows whether the dish has started cooking |
| Reason | Mandatory, chosen from the fixed list (section 10) |
| Path | The cancel request goes straight from the branch to the Kitchen, not through Inventory |

### Branch side

1. The cashier or customer presses Cancel and chooses a reason. If the order is already in a terminal status, the Cancel button is hidden.
2. If the create-order entry is still in the send queue and does not have the "send attempted" flag: POS closes the order as Cancelled (customer cancel), discards that entry and stops. Nothing is sent. Checking the flag and discarding the entry happen in the same local transaction as the sender writing the flag; whichever writes first wins.
3. Otherwise, the create-order entry has been attempted: POS writes the cancel request to the send queue, after the create-order entry of the same order ID, and then turns on the "Cancel requested" flag. The order's main status does not change yet.
4. For an order that has been attempted, POS always sends both the create-order entry and the cancel entry and does not cancel locally, even if the earlier send reported an error: POS does not know whether Kafka received the entry.
5. The branch updates its display from the Kitchen's reply and the status messages, following the rank rule in section 3.1.

### Kitchen decision table

The Kitchen decides from its own record, not from the status the branch currently sees. A branch seeing Reserved does not mean the Kitchen has received the order.

| What the Kitchen sees when the cancel request arrives | What the Kitchen does | What the branch sees |
|---|---|---|
| No record | Creates Cancel-pending, returns the commitment "kitchen will not cook", does not finalise a terminal status yet | "Cancel requested: kitchen will not cook" |
| Queued | Moves to Cancelled, sends Inventory a release command | Cancelled |
| Cooking or Completed | Refuses the cancellation | Status unchanged, with the message "Cannot cancel: the kitchen has started cooking" |
| Cancel-pending, Cancelled, Failed | Returns the stored result | Unchanged |

This table applies to a cancel request from the order's actual origin branch. A request from another branch is handled according to sections 3.2 and 13.2.

Once a Cancel-pending record exists, the final result depends on Inventory:

| How Inventory handles the order | Result |
|---|---|
| Reserves the ingredients and forwards the order to the Kitchen | The Kitchen creates no ticket, moves to Cancelled and sends Inventory a release command. The branch sees Cancelled |
| Rejects (including past the time limit) | Inventory sends Failed to the branch and the Kitchen. The branch sees Failed and the cancel flag turns off; the Kitchen moves the record to Failed |
| Inventory is down | The branch keeps "Cancel requested: kitchen will not cook" until Inventory comes back. The Kitchen must not finalise Cancelled on its own |

### Races and network loss

- **Cancel and Start cooking at almost the same time:** whichever operation the Kitchen records first wins; the other gets a rejection. Both operations are conditional writes on the Queued status.
- **A cancel request reaches the Kitchen before the order:** this is a normal case, not an error, because the order goes around through Inventory while the cancel request goes straight.
- **Branch offline:** an order that was never attempted is cancelled locally and is certainly not cooked. For an order that has been attempted, the cancel request is stored and sent later; this request is not a commitment: by the time the network returns, the Kitchen may have finished cooking. A network drop between sending the create-order entry and the cancel entry gives the same result. The user interface must say this clearly.
- **Kitchen down:** the "Cancel requested" flag is shown with the time waited so far and is included in the Flow D alerts.

## 7. Flow C: Sold-out reporting

Inventory publishes dish status to POS and Online as a hint; Inventory still makes the final decision, so an order can still be Failed even though POS shows the dish as Available.

### Inventory side

1. After every stock change (reserve, release, waste, stock receipt, flagging or clearing the stock-check flag, a recipe change), Inventory recomputes the status of the affected dishes using the conditions in section 3.4. This recomputation happens only inside Inventory.
2. Inventory publishes a message only when a dish's status changes between Available, Sold out and Hidden. If stock quantities change but the dish status does not, nothing is published.
3. Each message carries the dish ID, the status and the version (epoch, seq). `seq` comes from a durable sequence number in the Inventory database, shared by all dishes, and is not reset when Inventory restarts. `epoch` increases only when Inventory is restored from a backup.
4. Inventory sends a heartbeat at a fixed interval (section 10), carrying the current `epoch`.
5. Inventory provides a snapshot: the status and version of every dish, including Hidden dishes.
6. Inventory also publishes messages to the Kitchen when an ingredient is flagged for stock check and when the flag is cleared, versioned in the same way.

### POS and Online side

1. On a status-change message: overwrite that dish only if the new version is greater than the version held.
2. On reconnecting: subscribe to new messages, fetch the snapshot, then merge each dish using the same version comparison rule. Do not clear the cache to reload it.
3. When a heartbeat or any message carries an `epoch` greater than the epoch held: fetch the snapshot, merge, then move every dish still carrying the old epoch to Hidden.
4. Data counts as fresh if a heartbeat or a message from Inventory was received within the last N minutes.

| Situation | Available dish | Sold-out dish | Hidden dish |
|---|---|---|---|
| Online, fresh data | Selectable | Blocked, not selectable | Not shown |
| Offline or data older than N minutes | Selectable; the screen shows one general line "Dish availability may have changed" | Shows "May be sold out", can still be ordered | Not shown |

Two dishes that share an ingredient can both show Available even though there is not enough to make both. This is accepted because the status is only a hint.

## 8. Flow D: Monitoring

Flow D adds no order status and has no operator action that changes an order's status. The system only raises alerts; it does not cancel an order by itself because the order has waited a long time.

| Viewer | Display | Action |
|---|---|---|
| Branch | Connection status; number of entries waiting to be sent | None |
| Branch | Alert for an order in Awaiting Inventory for more than N minutes | Request cancellation button right on the alert |
| Branch | Alert for an order in the send queue that is about to pass the X-hour limit | None |
| Branch | Orders with the "Cancel requested" flag, with the time waited so far | None |
| Kitchen | Red warning on Queued tickets that contain an ingredient flagged for stock check | Reject through Flow A if it really is spoiled |
| Kitchen | Alert for a cancel request from a branch other than the origin branch (section 13.2) | The operator checks the configuration of the sending branch |
| Inventory | Log of release commands that match no reservation record | The operator checks manually |
| Inventory | List of ingredients flagged for stock check | Clear the flag once the stock check is done (Flow A) |
| Inventory, Kitchen | Orders that "need review" because a message is in `dlq` (section 9) | None. The lean edition has no close-order operation (section 15) |
| Inventory, Kitchen | Slow-consumer alert: an unread message older than the threshold in section 10 | None |
| Management | Each branch has a line "data as of hh:mm:ss"; order counts by status and by cancel, rejection and failure reason | None |

The Management report counts only order statuses at the branches. A Cancel-pending record at the Kitchen is not a terminal status and so is not counted.

## 9. Mandatory technical requirements

Message delivery between the parties is "at least once" (at-least-once), and ordering across different paths is not guaranteed. Every requirement below follows from this assumption.

| Rule | Mechanism that must be used |
|---|---|
| BR4, BR8, BR12 | The order ID is generated by POS (UUID) at creation. Every receiver deduplicates in two layers (idempotent consumer). A message whose `event_id` has already been processed is ignored and nothing is published (section 13.3). A message with a new `event_id` for an order ID that already has a record goes through the state table in section 3, that is, the stored result is returned. Do not deduplicate by the pair of order ID and message type: two cancel requests for the same order from two branches are two different messages |
| BR5, BR9 | A durable send queue at POS (outbox pattern). Ordering is kept per order ID; each entry has a "send attempted" flag written durably before the first send, and the sender writes the flag only when a connectivity probe to Kafka has just succeeded; writing the flag and cancelling locally are two conditional writes on the same row. A send error is retried with increasing backoff, and the entry is not discarded |
| BR7 | Every transition at the Kitchen is a conditional write (compare-and-set) on the current status |
| BR3 | The branch merges statuses by rank and does not use "last message wins" |
| Flow A, B | Releasing ingredients is a compensating transaction (saga compensation). The release command is stored durably by the Kitchen before it is sent |
| BR7, BR8 | A Cancel-pending record is a row in the Kitchen's order table (tombstone), not a separate table |
| BR10 | The version (epoch, seq) is durable; each dish is merged by version; the heartbeat carries the epoch |
| BR11 | POS clocks are NTP-synchronised; Inventory checks both expired orders and orders with a creation time in the future |

### What must not be done

- Do not delete order records at the Kitchen or Inventory.
- Do not keep the version counter in RAM, and do not use the system clock as the version.
- Do not assign the snapshot directly to the cache (as in `menuCache = snapshot`).
- Do not assume the Kitchen receives the order before the cancel request, even if POS sends them in the right order.
- Do not let the Kitchen return "Cancelled" for an order that has not arrived.
- Do not recompute the release quantity from the current recipe.
- Do not block the whole send queue because of one failing entry; do not discard an entry because of a send error.
- Do not add to stock when a release command matches no reservation record.
- Do not cancel locally an order whose create-order entry has been attempted, even if that send reported an error.
- Do not treat a Kafka acknowledgement as Inventory having received the order. Business results arrive only through `status-events.<branch>`.
- Do not let the Kitchen ignore a Failed message for an order that has no record; the result must be recorded.
- Do not reserve an ingredient flagged for stock check, even if available stock is sufficient.

### Common message envelope

Every event on every topic uses one common envelope. When the envelope changes, fields may only be added, never renamed or removed; on deployment, update the readers first and the senders afterwards.

```json
{ "event_id": "uuid", "event_type": "CancelRequested", "schema_version": 1,
  "order_id": "uuid", "branch_id": "POS01", "occurred_at": "2026-11-20T09:15:00Z",
  "causation_id": "event_id of the message that caused this one", "payload": { } }
```

- `event_id` is used for deduplication (the `processed_events` table, section 13).
- `order_id` is the partition key and appears in every log line of every service, so that the whole path of an order can be filtered.
- `branch_id` in the message body is for reference only. The actual sending branch is derived from the topic name.

### Handling bad messages and unknown events

| Situation | Display-only side (POS, Online) | Deciding side (Inventory, Kitchen) |
|---|---|---|
| Transient error (lost DB connection, timeout) | Retry indefinitely with increasing backoff | Retry indefinitely with increasing backoff |
| Unreadable message | Try N times, move to `dlq`, raise an alert | Try N times, move to `dlq`, mark the order as "needs review", raise an alert |
| Create-order message that is readable but has invalid order data | Not applicable | Inventory records a Failed result (invalid order data) according to BR12 and notifies the branch and the Kitchen. Not moved to `dlq` |
| Unknown `event_type` | Log, raise an alert, ignore (a later message will overwrite by rank, BR3) | Move to `dlq`, mark the order as "needs review", raise an alert. Never ignored silently |
| `schema_version` higher than the supported level | Same as unknown `event_type` | Same as unknown `event_type` |

A message moved to `dlq` does not block the partition: other orders are still processed. An order marked "needs review" appears in Flow D. Because both POS and Inventory validate order data, `dlq` receives only messages that are unreadable or of an unknown type, which means it appears only in testing. The lean edition has no close-order operation: if a real order ends up in `dlq`, the order stays in Awaiting Inventory (section 15).

## 10. Reasons and parameters

Every reason comes from a fixed list; there is no free-text entry. Values marked "proposed" need the team's confirmation.

### Reasons

| Group | Reason | Ingredient handling |
|---|---|---|
| Failed (Inventory) | Insufficient ingredients | Not reserved |
| Failed (Inventory) | Ingredient awaiting stock check | Not reserved |
| Failed (Inventory) | Dish has no recipe | Not reserved |
| Failed (Inventory) | Order expired | Not reserved |
| Failed (Inventory) | Invalid creation time (in the future) | Not reserved |
| Failed (Inventory) | Invalid order data | Not reserved |
| Kitchen rejection | Equipment failure | Release everything |
| Kitchen rejection | Short-staffed | Release everything |
| Kitchen rejection | Spoiled ingredient (the ingredient must be chosen) | Write off that ingredient as waste, release the rest, flag for stock check |
| Customer cancel (proposed) | Customer changed their mind | Release everything if reserved |
| Customer cancel (proposed) | Customer waited too long | Release everything if reserved |
| Customer cancel (proposed) | Cashier entered the wrong order | Release everything if reserved |

### Parameters

| Parameter | Value | Note |
|---|---|---|
| X: expiry threshold for accepting an order at Inventory | 4 hours (proposed) | Set by the business: for an order older than this, the customer has left |
| Tolerance for a creation time in the future | 5 minutes (proposed) | Covers small clock differences between POS and Inventory |
| N: alert for an order in Awaiting Inventory | 10 minutes (proposed) | Flow D |
| Alert for an order in the send queue that is about to expire | X minus 1 hour (proposed) | Flow D |
| Inventory heartbeat interval | 60 seconds (proposed) | |
| N: dish data counts as stale | 5 minutes (proposed) | Must be greater than at least two heartbeat intervals |
| Kafka message retention | 7 days | Must be longer than the longest downtime of a consumer |
| Number of attempts for a permanently failing message | 5 attempts (proposed) | After that, moved to dlq |
| Slow-consumer alert | An unread message older than 5 minutes (proposed) | Flow D |

The three parameters named N (alert for an order in Awaiting Inventory, dish data counts as stale, number of attempts for a failing message in section 9) are three independent values, configured separately.

## 11. Decisions, limitations and scope

Eight questions Q1–Q8 have been decided. Of the five points that needed the team's confirmation, the three that change behaviour were confirmed on 21 Nov 2026; the two still open are configuration values and do not block coding.

### Decisions made

| # | Question | Decision |
|---|---|---|
| Q1 | Add a Cooking status for tickets? | Yes. It is the point that blocks cancellation and the point at which Inventory counts Consumed |
| Q2 | Allow cancellation while the order is still Awaiting Inventory? | Yes, through the Cancel-pending record at the Kitchen |
| Q3 | Can Online customers cancel by themselves? | Yes, sharing the same flow as the cashier |
| Q4 | Is choosing a reason mandatory? | Yes, from a fixed list. The reason also decides between release and waste |
| Q5 | When does an order show Cancelled? | When the Kitchen accepts, without waiting for Inventory to release. With Cancel-pending: when the order reaches the Kitchen |
| Q6 | Is an order that waits too long for Inventory cancelled automatically? | No, there is only an alert with a Request cancellation button. An order older than X hours is rejected by Inventory under BR11 |
| Q7 | If a dish that is already Cooking is abandoned, are its ingredients released? | No |
| Q8 | For a Sold-out dish, block or warn? | Block when data is fresh, warn when data is stale or the branch is offline |

### Needs team confirmation

- [ ] The value of X (proposed 4 hours) and the proposed parameters in section 10.
- [ ] The list of customer cancel reasons.
- [x] BR12 and the "Consumed" message from the Kitchen to Inventory. These two points were added when this version was compiled. If Inventory does not store the Failed result, an order resent after Inventory receives more stock would be reserved and cooked even though the branch has already seen Failed. Without the Consumed message, the reserved quantity would stay in Held forever. Confirmed on 21 Nov 2026: @Naul agreed as proposed.
- [x] Local cancel of an order that was never attempted (BR9). If this is not accepted, the phrase "never cooked" in scenario 10 must be removed and every cancel request made while offline must be treated as uncertain. Confirmed on 21 Nov 2026: @Naul agreed as proposed.
- [x] Inventory refusing to reserve an ingredient flagged for stock check (BR6). Consequence: Inventory is shared, so one spoilage report blocks every order that uses that ingredient across the whole chain until the stock check is done. Confirmed on 21 Nov 2026: @Naul agreed as proposed, with this consequence accepted.

### Known limitations

- **Spurious failure:** in the short interval between Inventory reserving ingredients for an order that already has a Cancel-pending record and Inventory receiving the release command, another order can be Failed even though stock is actually there.
- **Inaccurate dish hints:** two dishes that share an ingredient can both show Available; a dish near the threshold can flip back and forth between Available and Sold out.
- **A cancel request for an order that has been attempted is not a commitment,** even when the request is recorded while offline. Only an order that was never attempted is certain to be cancelled.
- **The "kitchen will not cook" commitment is conditional:** it holds only when the branch requesting cancellation is the order's origin branch. An honest branch always meets this condition.
- **If staff forget to press Start cooking**, a dish that is being cooked can still be cancelled and its ingredients are wrongly released. This is handled through training and the user interface, not by the system.

### Out of scope for v1.0

- Restoring Inventory from a backup: handled manually, with a mandatory physical stock check before Inventory accepts orders again. The system only guarantees that the epoch increases and that unmatched release commands are ignored.
- Cancelling or rejecting part of an order.
- Payment and refunds, including prepaid Online orders.
- Automatically cancelling tickets that contain an ingredient flagged for stock check.
- Preventing flipping between Available and Sold out at the threshold.
- The infrastructure and operations parts dropped from the lean edition: see section 15.

## 12. Mandatory test scenarios

Every scenario must have an automated test. Scenario numbers are kept as in v1.3 for easy cross-reference; 11 scenarios that belong to the dropped parts (11, 13, 14, 17, 32, 38, 40, 41, 45, 46, 47) are no longer in the table, which leaves 36 scenarios from v1.3. Version v1.5 adds 8 scenarios numbered 48–55, so the table has 44 scenarios.

Scenarios 18–21 belong to Flow C and are done in phase 2; the other scenarios are done in phase 1. Two parts are checked in phase 2 even though their scenarios belong to phase 1: the "dish moves to Sold out" part of scenario 6 and the "dish returns to Available" part of scenario 48. The messages Inventory sends to the Kitchen when an ingredient is flagged for stock check and when the flag is cleared (step 6 on the Inventory side of Flow C) are built in phase 1, because the ticket-warning part of scenario 6 needs them.

In the scenarios, "duplicate" and "resent" cover two cases, and the test must cover both: a message arriving again with the same `event_id` (the receiver ignores it and publishes no new message), and a message carrying a new `event_id` for an order ID that already has a record (the receiver returns the stored result in a new message).

Scenarios 1, 3, 4, 10 and 36 should be used for the demo; 10 and 36 go as a pair to show the boundary of the guarantee when cancelling while offline.

| # | Scenario | Expected result | Rule |
|---|---|---|---|
| 1 | A cancel request reaches the Kitchen before the order; Inventory then reserves the ingredients and forwards the order to the Kitchen | No ticket; the order is Cancelled; Inventory releases in full, once | BR7 |
| 2 | As scenario 1, then Inventory sends the order a second time | No ticket is created and no second release command is sent | BR8 |
| 3 | The cashier presses cancel and the Kitchen presses Start cooking at the same time | Exactly one operation wins; the statuses at the Kitchen, Inventory and the branch agree | BR7 |
| 4 | The Kitchen rejects a ticket while Inventory is down, then Inventory comes back | The branch sees Cancelled immediately; Inventory releases exactly once when it comes back | BR4 |
| 5 | The release command for the same order reaches Inventory three times | Stock increases exactly once | BR12 |
| 6 | The Kitchen rejects with the reason spoiled ingredient (meat) | The meat is written off as waste and the other ingredients are released; dishes that use meat move to Sold out; Queued tickets that contain meat show a warning | BR6 |
| 7 | The Cancelled message reaches the branch before the Reserved message | The branch keeps Cancelled and ignores Reserved | BR3 |
| 8 | An order that has a Cancel-pending record is rejected by Inventory | The branch sees Failed and the cancel flag turns off; the Kitchen record moves to Failed | BR7 |
| 9 | Inventory returns Failed for lack of stock, more stock is received, then the order is resent | Inventory returns Failed again and reserves nothing | BR12 |
| 10 | POS is offline, so the sender does not write the flag; the cashier creates an order and then cancels it while the create-order entry has never been attempted; the network then returns | POS closes the order as Cancelled immediately, discards the entry and sends nothing; Inventory and the Kitchen have no record; the order is never cooked | BR5, BR9 |
| 12 | The network is unstable for a long time: entries that have been attempted fail to send many times | No entry is discarded; when the network returns every entry is sent, in the right order per order ID | BR9 |
| 15 | POS is offline for longer than X and then sends the old order | Inventory returns Failed (order expired) and reserves no ingredients | BR11 |
| 16 | An order has a creation time 1 hour in the future | Inventory returns Failed | BR11 |
| 18 | Inventory restarts, then a dish changes status | The version keeps increasing; POS receives the update immediately | BR10 |
| 19 | POS receives a Sold out message (v6), then a snapshot with Available (v5) arrives later | POS keeps Sold out (v6) | BR10 |
| 20 | Inventory is restored from a backup and increases the epoch; dish A is Sold out in the old generation, dish B is not in the new snapshot | A follows the new snapshot; B moves to Hidden | BR10 |
| 21 | Inventory has no stock change for 10 minutes and heartbeats are regular | POS does not show "May be sold out" | BR10 |
| 22 | Inventory receives a release command for an order that has no reservation record | Stock is unchanged and a log line is written | BR12 |
| 23 | A cancel request arrives while the ticket is Cooking | The branch sees "Cannot cancel: the kitchen has started cooking" and the status is unchanged | BR7 |
| 24 | Inventory reserves an order from POS01 | The Kitchen receives the order through kitchen-orders; POS01 receives Reserved through its own status-events; other branches receive nothing | Section 13 |
| 25 | The Kitchen is shut down for 2 hours while orders and cancel requests keep coming, then started again | The Kitchen processes every message still in Kafka, with none lost and none duplicated | Section 13 |
| 26 | The consumer process is stopped after the DB commit but before the offset commit | Kafka redelivers the message; processed_events blocks the duplicate and there is no second effect | BR4 |
| 27 | POS01 sends a cancel request for an order of POS02 that already has a ticket at the Kitchen | The Kitchen ignores the request and raises a warning; the POS02 order does not change status | Section 13.2 |
| 28 | Two Kitchen instances run; both receive the order and the cancel request for the same order ID at the same time | Only one record exists; the result matches table 3.2 whichever message arrives first | BR7 |
| 29 | Inventory receives a message with an unknown event_type | The message goes to dlq, the order is marked as needing review, and other orders are still processed | Section 9 |
| 30 | Inventory is short of one ingredient when reserving an order, then the message for that order is resent | Stock is unchanged; the Failed record, the processed_events row and the Failed message are all stored; the resend gets the same stored result | BR12 |
| 31 | One Kafka broker is shut down while the branches are sending orders | No message acknowledged by Kafka is lost; sending continues | Section 13 |
| 33 | Inventory's Failed report reaches the Kitchen first, and the cancel request for that order arrives later | The Kitchen has a Failed record; no Cancel-pending record is created; the branch sees Failed | BR8 |
| 34 | POS02 sends a cancel request for an order ID of POS01 before the order reaches the Kitchen | The POS01 order still gets a Queued ticket; the POS02 request is dismissed; a warning is raised | Section 13.2 |
| 35 | As scenario 34, but POS01 also sends a cancel request before the order arrives | The order is Cancelled; Inventory releases exactly once | BR7 |
| 36 | POS sends the order and then goes offline, and the cashier presses cancel; when the network returns, the Kitchen has already pressed Start cooking before receiving the cancel request | The branch sees "Cannot cancel: the kitchen has started cooking"; the order continues to Completed; the ingredients are not released | BR5, BR7 |
| 37 | The cashier presses cancel at the very moment the sender picks up a create-order entry that has never been attempted | Exactly one side wins: either the order is cancelled locally and no entry is sent, or the entry is sent and the cancel request goes through the Kitchen | BR9 |
| 39 | Inventory receives an order that is readable but has a negative quantity | Inventory writes a Failed record (invalid order data) and reserves nothing; the branch sees Failed; the message does not go to dlq | BR12 |
| 42 | Two orders arrive at the same time at two Inventory instances, both needing the same ingredient, with total demand exceeding available stock | Exactly one order is Reserved and the other is Failed; available stock is not negative; no order is partially reserved | BR1 |
| 43 | Inventory has 10 kg of meat; an order reserves 2 kg, then the Kitchen presses Start cooking | Available stock is 8 kg before and after cooking; reserved returns to 0; consumed increases by 2 kg | Section 3.3 |
| 44 | Meat is flagged for stock check and available stock is sufficient; POS, while offline, still accepts an order with a dish that uses meat and then sends it | Inventory returns Failed (ingredient awaiting stock check); no ingredient of the order is reserved | BR6 |
| 48 | Meat is flagged for stock check and a Queued ticket that contains meat is showing a warning; Inventory confirms that the stock check is done | The flag is cleared; the warning on the ticket disappears; a new order that uses meat is reserved; dishes that use meat return to Available | BR6 |
| 49 | An order reserves 2 kg of meat; the dish's recipe changes to 3 kg; the Kitchen then cancels or rejects the order | Inventory releases exactly the 2 kg recorded at reservation time; available stock returns to what it was before the reservation | BR6 |
| 50 | Staff press Reject on a ticket that is already Cooking | The operation is rejected; the ticket stays Cooking; Inventory receives no release command | Flow A |
| 51 | Staff press Reject with the reason spoiled ingredient at the very moment the origin branch's cancel request reaches a Queued ticket | Exactly one operation wins; Inventory processes exactly one release command, according to the winner's reason: either release everything, or write off as waste and flag for stock check | BR6, BR7 |
| 52 | Two staff members press Reject and Start cooking on the same Queued ticket at the same time | Exactly one operation wins; the reservation leaves Held exactly once, to consumed or to released | BR7, BR12 |
| 53 | Inventory is down; an order stays in Awaiting Inventory for more than N minutes | The branch shows an alert with a Request cancellation button; the order does not change status by itself | Flow D |
| 54 | POS is offline; an order in the send queue is less than 1 hour away from passing the X limit | The branch shows the lost connection, the number of entries waiting to be sent and the alert that the order is about to expire | Flow D |
| 55 | Replication from POS01 to the Central Order Replica is cut while POS01 and POS02 keep taking orders, then reconnected | While it is cut, the report can still be viewed, the "data as of" marker of POS01 stops while that of POS02 keeps moving; after reconnection, the POS01 figures match the branch's DB | Flow D, section 13.5 |

## 13. Deployment assumptions

The rules BR1–BR12 hold only when the system is deployed according to the assumptions below. Lean edition architecture: Kafka with 3 brokers, PostgreSQL for every DB, 2 Inventory instances, and 1 instance each for the Kitchen and Online (section 13.6).

### 13.1 Kafka topics

| Topic | Written by | Read by | Partition key |
|---|---|---|---|
| `order-events.<branch>` | That branch | Inventory (subscribes by the pattern `order-events.*`) | Order ID |
| `cancel-events.<branch>` | That branch | Kitchen (subscribes by the pattern `cancel-events.*`) | Order ID |
| `status-events.<branch>` | Inventory, Kitchen | That branch | Order ID |
| `kitchen-orders` | Inventory | Kitchen | Order ID |
| `stock-commands` | Kitchen | Inventory | Order ID |
| `menu-events` | Inventory | All branches, Kitchen | Dish ID or ingredient ID |
| `dlq` | Every consumer | Operator | Order ID |

When it finishes processing an order, Inventory writes messages to both `kitchen-orders` and `status-events.<branch>` through the outbox, in the same transaction.

That each branch writes and reads only its own topics is a convention in the source code and is not enforced by the broker (section 13.2).

Mandatory configuration:

- Producer (outbox relay): `acks=all`, `enable.idempotence=true`. An outbox row is marked "sent" only when Kafka has acknowledged it.
- Topic: `replication.factor=3`, `min.insync.replicas=2`, `retention.ms` = 7 days.
- Broker: `unclean.leader.election.enable=false`.

### 13.2 Origin branch of an order

- The Kitchen derives the branch that sent a cancel request from the topic name, then compares it with the order's origin branch (passed along by Inventory through `kitchen-orders`, including in a Failed message). If they differ, the cancel request is ignored and a warning is raised.
- A Cancel-pending record stores every branch that has sent a cancel request, not only the first one; if only one were stored, a wrong-branch request arriving first would swallow a valid request arriving later. When the order arrives: if the origin branch is in the list, the order is Cancelled; if not, the Kitchen dismisses the recorded requests, creates the ticket as normal and raises a warning (table 3.2).
- The lean edition has no authentication and no authorisation on Kafka. This check stops cross-branch cancellation caused by a configuration error or a bug; it does not protect against a party that deliberately writes to another branch's topic (section 15).

### 13.3 How a consumer handles each message

Automatic offset commit is turned off. Each message is processed in exactly this order:

1. Open a DB transaction.
2. Insert the `event_id` into `processed_events`. If the ID already exists, the message is a duplicate: roll back and jump to step 5.
3. Apply the state transition table in section 3.
4. Write the outbox, then commit.
5. Commit the Kafka offset.

If the process dies between step 4 and step 5, Kafka redelivers the message and step 2 blocks the duplicate. The lean edition does not clean up the `processed_events` table. Errors during processing follow the table "Handling bad messages and unknown events" in section 9.

### 13.4 DB writes under contention

**Kitchen:** every message that concerns an order ID, from any topic, and every staff action on a ticket is handled in one transaction with a row lock. Whichever processing path gets the lock first runs first, whether on the same instance or on a different one; this is how the DB guarantees BR7.

```sql
INSERT INTO kitchen_orders (order_id, status)
VALUES (:id, 'CHUA_CO') ON CONFLICT (order_id) DO NOTHING;
SELECT status FROM kitchen_orders WHERE order_id = :id FOR UPDATE;
-- apply table 3.2 → UPDATE status → write outbox → COMMIT
```

The `CHUA_CO` row is only a place to hold the lock: table 3.2 treats it as "(none)" and it is not a record in the sense of BR8. The origin branch (`origin_branch`) is written only from an Inventory message; the branches that send cancel requests are written to a separate list (`cancel_branches`), not to the same column.

**Inventory:** a savepoint is used so that when an ingredient is short only the stock deduction is undone, while the Failed result and the deduplication row are still written (BR12).

```sql
BEGIN;
INSERT INTO processed_events ...;
INSERT INTO inventory_orders (order_id, ...) ...;
SAVEPOINT reserve;
UPDATE stock SET available = available - :q   -- each ingredient, in ascending ID order
 WHERE ingredient_id = :i AND available >= :q AND NOT needs_check;
-- if any row could not be deducted:
ROLLBACK TO SAVEPOINT reserve;
UPDATE inventory_orders SET status = 'THAT_BAI', reason = :ly_do ...;
INSERT INTO outbox ...;   -- Failed for kitchen-orders and status-events.<branch>
COMMIT;
```

- Always lock ingredients in ascending ID order so that two orders cannot deadlock each other.
- The condition `NOT needs_check` is inside the UPDATE statement itself, so checking the flag and reserving are one operation. The command that flags an ingredient for stock check locks the same stock row, so there is no gap between flagging and reserving. The Failed reason is "Ingredient awaiting stock check" if the row could not be deducted because of the flag, and "Insufficient ingredients" otherwise.
- Keep transactions short, and do not call external services while holding locks.

### 13.5 Databases

- The Inventory DB and the Kitchen DB are each a single PostgreSQL instance, with no replica. Inventory and the Kitchen favour consistency: every state change and outgoing message is in the same transaction (outbox).
- When Inventory or the Kitchen is down, messages wait in Kafka and are processed when the service comes back (scenarios 4, 25, 26). New orders are still subject to the X-hour limit: if Inventory is down for longer than X, the orders that have waited past the limit get Failed (order expired) when Inventory comes back (BR11); release commands and Consumed reports have no time limit. Losing a DB's data entirely is out of scope for the lean edition (section 15).
- The branches' order DBs replicate asynchronously to the Central Order Replica to serve reporting; this is the data replication part of the system.

### 13.6 Deployment size

Each component on the diagram is there to answer one specific problem.

| Component | Count | Which problem it serves |
|---|---|---|
| Kafka | 3 brokers | Acknowledged messages are not lost when one broker dies (scenario 31); orders and release commands can wait while Inventory or the Kitchen is down (scenarios 4, 25) |
| Local Order Service and DB | 1 per POS site | A branch that is offline still records orders and resends them when the network returns (BR5, scenarios 10, 36) |
| Online Order Service | 1 instance | A second order source, independent of POS, sharing the same flow |
| Inventory Service | 2 instances | Two orders compete for the same ingredient on two instances; row locks in the DB arbitrate (BR1, scenario 42) |
| Kitchen Service | 1 instance; 2 when running scenario 28 | The race between Cancel and Start cooking exists within a single instance: the cancel message arrives through the consumer, the button press through HTTP (BR7, scenario 3) |
| Inventory DB, Kitchen DB | 1 per service, no replica | Single source of truth for stock and tickets; consistent through transactions and row locks |
| Central Order Replica | 1 | Asynchronous replication; chain-wide reports do not read the branches' DBs directly |

## 14. Guarantees and limits

The system promises only what is listed below, and only while the accompanying condition holds. Anything outside the list is not guaranteed.

### The system guarantees

| Guarantee | Under the condition |
|---|---|
| Ingredients are not reserved or released twice for one order | The Inventory DB does not lose committed data |
| An order that has a Cancel-pending record is never cooked | The Kitchen DB does not lose committed data; the cancel request was sent by the order's origin branch |
| An order cancelled while its create-order entry had never been attempted is never cooked | The POS local DB does not lose committed data |
| An order that is Failed is never evaluated again | The Inventory DB does not lose committed data |
| An ingredient flagged for stock check is not reserved for new orders | The Inventory DB does not lose committed data |
| A reservation still Held always has a record to release or to count as consumed | The Inventory DB does not lose committed data |
| No message acknowledged by Kafka is lost | At most 1 of the 3 brokers dies |
| Every message is eventually processed exactly once in terms of effect | No consumer stays down longer than Kafka's message retention |
| Order status at the branch does not go backwards | Always true (BR3) |

### The system does not guarantee

- That a cancel request for an order that has been attempted will certainly succeed, even when the request is recorded while offline.
- That Inventory receives every order Kafka has acknowledged. A Kafka acknowledgement only means that the entry has entered the queue; business results arrive only through status messages.
- That dish status on POS is always accurate. Dish status is only a hint: Inventory is shared, so another branch may have just ordered the last of an ingredient, and two dishes that share an ingredient can both show Available.
- That an order is processed if a consumer stays down longer than Kafka's message retention. This case is handled manually.
- That data survives the loss of the Inventory DB or the Kitchen DB. The lean edition has no replica for these two DBs.
- That a branch cannot send messages on behalf of another branch. The lean edition has no authentication; the Kitchen only compares the origin branch to stop mistakes.
- That an order with a message in `dlq`, or a reservation whose release command was lost, is closed automatically. The lean edition has no reconciliation tool (section 15).
- Ordering between two different topics, even for the same order ID.
- High throughput when many orders use the same common ingredient (ice, milk, meat): these orders must wait for each other's lock on the same stock row. This is the first bottleneck under high load.

## 15. Limitations and path to production

The parts below exist in the full edition v1.3 but are not implemented in the course project. Each row states the consequence that must be accepted in the lean edition and how the production edition handles it; the section numbers in the last column are section numbers of v1.3.

| Dropped part | Consequence in the lean edition | Path to extend (per v1.3) |
|---|---|---|
| Synchronous replicas for the Inventory DB and Kitchen DB | When a DB is down the service is down, and messages wait in Kafka. Losing a DB entirely means losing the stock state or the tickets | 1 primary and 1 synchronous replica; fence the old primary before switching; increase the epoch if the switch happens while replication is running asynchronously (section 13.5) |
| Authentication and authorisation: mTLS or SASL, Kafka ACLs, checking Online customer accounts | Any process can write to every topic. The origin-branch check only stops mistakes, not forgery | One Kafka identity per branch, ACLs on the branch's topics; Online checks that the order belongs to the customer's account (section 13.2) |
| Quarantine queue for entries rejected by the broker | An entry rejected by the broker is retried indefinitely and counts among the entries waiting to be sent; POS does not close that order by itself | A quarantine queue, a "certainly not in Kafka" rule based on sends whose outcome is unknown, and a discard-entry operation (BR9) |
| TTL-based record cleanup | The order tables at the Kitchen and at Inventory and the `processed_events` table grow over time | 7-day TTL, cleaning up only records with no unfinished work; the Kitchen rejects orders and cancel requests older than the TTL (BR8, section 10) |
| Reconciliation of reservations older than G hours | If a release command or Consumed report is lost, the reservation stays in Held. The invariants in section 3.3 still hold, so nothing is reported; it can be seen only by manually comparing Inventory's Held records with the ticket statuses at the Kitchen | An alert after G hours and a Reconcile operation with the Kitchen to republish the release command or Consumed report (section 8) |
| Closing an order with a message in `dlq` | A real order with a message in `dlq` stays in Awaiting Inventory forever. The cashier can still send a cancel request and get the commitment "kitchen will not cook", but the order cannot reach a terminal status | A Close order operation at Inventory: record Failed, notify the branch and the Kitchen (section 8) |
| Multiple instances for the Kitchen and Online | When the Kitchen or Online is down, its part of the work stops until it comes back; no message is lost | Run 2 instances. The source code already uses row locks and deduplication, so the logic needs no change (scenario 28) |
