# Kết quả kiểm chứng Milestone B4 (11/11/2026)

Chạy local trên Windows, múi giờ Asia/Bangkok (`+07:00`), khoảng `22:45–23:30` ngày `11/11/2026`.
Kiểm chứng toàn bộ module `kitchen-service` (quản lý vé bếp, consumer `status-events`, API `/api/kitchen/tickets`, hoàn thành vé `/complete` kèm outbox `DoneEvent`) và đồng bộ ngược trạng thái về `order-service` (`PENDING_INVENTORY -> RESERVED -> DONE`, ma trận D13, chống revert khi trễ sự kiện, cách ly bán kính ảnh hưởng đa chi nhánh).

## Cấu hình hệ thống

- Maven Wrapper 3.3.4, Maven 3.10.0, Spring Boot 4.1.2, Spring Kafka (`kafka-clients` 4.2.2), Jackson 3.
- JDK build/runtime: Temurin 25.0.4.1; compiler `release 21`.
- PostgreSQL `postgres:18.6`:
  - `pos01-db` (`127.0.0.1:5433`, DB `pos01_orders`, schema `pos01`).
  - `inventory-db` (`127.0.0.1:5435`, DB `inventory_db`, schema `inventory`).
  - `kitchen-db` (`127.0.0.1:5436`, DB `kitchen_db`, schema `kitchen`).
- Cụm Kafka 3 node KRaft (`apache/kafka:4.1.2`): `kafka-1` (`19092`), `kafka-2` (`19093`), `kafka-3` (`19094`).
- Topic `order-events` và `status-events`: `PartitionCount=3`, `ReplicationFactor=3`, `min.insync.replicas=2`.
- Consumer Groups:
  - `inventory-v1` trên topic `order-events`
  - `kitchen-v1` trên topic `status-events` (2 instance `kitchen-1` và `kitchen-2` chia partition 0,1 và 2)
  - `order-status-POS01-v1` trên topic `status-events`
- Tất cả consumer đều dùng `AckMode.RECORD`, `enable-auto-commit: false`, `auto-offset-reset: earliest`.

## Bảng tổng hợp kịch bản kiểm thử

| Kịch bản / Hạng mục | Kết quả | Bằng chứng |
|---|---|---|
| `mvnw.cmd -B -ntp clean verify` toàn backend | `BUILD SUCCESS`; 56 tests (`event-contracts` 4, `order-service` 20, `inventory-service` 14, `kitchen-service` 18), 0 failures/errors/skipped | [build.txt](build.txt) |
| Khởi tạo hạ tầng & Reset Kafka Topics | 2 topic `order-events`, `status-events` đủ 3 partition / ISR `1,2,3`; 3 consumer group (`inventory-v1`, `kitchen-v1`, `order-status-POS01-v1`) kết nối ổn định | [topics-reset.txt](topics-reset.txt), [consumer-groups-initial.txt](consumer-groups-initial.txt) |
| Trạng thái ban đầu các DB & Health check | `pos01.orders`: 0 dòng, `inventory.ingredients`: GAO=1000, THIT_GA=1500, `kitchen.kitchen_tickets`: 0 dòng; cả 4 cổng `8081`, `8084`, `8085`, `18085` đều `UP` | [initial-state.txt](initial-state.txt) |
| D01 — Luồng đầy đủ (`NORMAL`) | Đơn `1c2db13f...` tạo ở `8081` (`PENDING_INVENTORY`) -> Kho giữ hàng -> Đơn cập nhật `RESERVED` -> Bếp tạo vé `WAITING` -> Bếp bấm xong `POST /complete` -> Vé thành `DONE` -> Đơn đồng bộ thành `DONE`; mọi consumer group `LAG = 0` | [scenario-d01-normal.txt](scenario-d01-normal.txt) |
| D02 — Kho từ chối (`FAILED`) | Hết gạo (`GAO=0`) -> Kho phát `Failed` -> Đơn `95708c6e...` cập nhật `FAILED` kèm `failure_reason = "INSUFFICIENT_INGREDIENT: Không đủ nguyên liệu"` -> Bếp bỏ qua sự kiện, 0 vé sinh ra | [scenario-d02-failed.txt](scenario-d02-failed.txt) |
| D04 & D05 — Idempotent & Chống Revert | Gọi lại `POST /complete` trên vé đã `DONE` -> trả 200 OK, outbox chỉ giữ đúng 1 sự kiện `Done`; Replay sự kiện `Reserved` muộn trên đơn đã `DONE` -> lưu `TERMINAL_STATE_IGNORED`, đơn giữ nguyên `DONE` | [scenario-d04-d05-replay.txt](scenario-d04-d05-replay.txt) |
| D12 — Cách ly bán kính ảnh hưởng đa chi nhánh | Phát sự kiện của `POS02` lên `status-events` -> `order-service` POS01 bỏ qua ngay tại bước kiểm tra envelope; sự kiện của `POS02` mang `schemaVersion=99` không làm crash POS01 | [scenario-d12-isolation.txt](scenario-d12-isolation.txt) |
| D13 — Hội tụ thứ tự ngược & Tiến triển đơn điệu | Phát sự kiện `Done` đến trước khi đơn đang ở `PENDING_INVENTORY` -> Đơn hội tụ trực tiếp về `DONE`; sự kiện `Reserved` đến sau không làm lùi trạng thái về `RESERVED` | [scenario-d13-convergence.txt](scenario-d13-convergence.txt) |
| Kafka Outage — Khả năng chịu lỗi khi sập Kafka | Dừng cụm Kafka -> `POST /api/orders` vẫn thành công 201 Created (lưu order + outbox `sent_at IS NULL`); Bật lại Kafka -> outbox tự động drain sau retry, đơn chuyển thành `RESERVED` | [scenario-kafka-outage.txt](scenario-kafka-outage.txt) |
| Trạng thái SQL cuối cùng | Toàn bộ dữ liệu các bảng `orders`, `order_items`, `processed_events`, `kitchen_tickets`, `kitchen_ticket_items`, `ingredients`, `inventory_order_decisions` đối chứng nhất quán | [sql-final.txt](sql-final.txt) |

## Ghi chú & Đóng mốc B4

- Đã hoàn tất toàn bộ yêu cầu của Milestone B4.
- Đã cung cấp bộ script reset hoàn chỉnh: `reset-pos01.ps1`, `reset-kitchen.ps1`, `reset-inventory.ps1`, `reset-kafka-topics.ps1`.
- Dữ liệu và trạng thái sau kiểm thử đã được dọn sạch hoàn toàn, các tiến trình test đã được tắt, cổng mạng giải phóng.
