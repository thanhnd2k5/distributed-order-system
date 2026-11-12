# Kết quả kiểm chứng Milestone B5 (12/11/2026)

Chạy local trên Windows, múi giờ Asia/Bangkok (`+07:00`), khoảng `18:08–18:25` ngày `12/11/2026`.
Kiểm chứng toàn bộ hạ tầng đa chi nhánh (`POS01`, `POS02`, `ONLINE`), cấu hình PostgreSQL Logical Replication (`wal_level=logical`, publication theo từng mảnh, subscription độc lập tại `central-db`, view `reporting.all_orders` / `reporting.all_order_items`), microservice đọc trung tâm `order-query-service` (role chỉ đọc `query_reader`), và trọn vẹn kịch bản **D11** (tái thiết đa nguồn, mô phỏng lag khi `DISABLE`/`ENABLE` subscription, tính độc lập khi tắt Query Service).

## Cấu hình hệ thống

- Maven Wrapper 3.3.4, Maven 3.10.0, Spring Boot 4.1.2, Spring Kafka (`kafka-clients` 4.2.2), Jackson 3.
- JDK build/runtime: Temurin 25.0.4.1; compiler `release 21`.
- PostgreSQL `postgres:18.6` (các publisher DB đều bật `wal_level=logical`, nối mạng `order-replication-net`):
  - `pos01-db` (`127.0.0.1:5433`, DB `pos01_orders`, schema `pos01`, publication `pos01_pub`)
  - `pos02-db` (`127.0.0.1:5437`, DB `pos02_orders`, schema `pos02`, publication `pos02_pub`)
  - `online-db` (`127.0.0.1:5438`, DB `online_orders`, schema `online`, publication `online_pub`)
  - `central-db` (`127.0.0.1:5434`, DB `central_orders_db`, schemas `pos01`, `pos02`, `online`, `reporting`, subscriptions `pos01_sub`, `pos02_sub`, `online_sub`)
  - `inventory-db` (`127.0.0.1:5435`, DB `inventory_db`, schema `inventory`)
  - `kitchen-db` (`127.0.0.1:5436`, DB `kitchen_db`, schema `kitchen`)
- Cụm Kafka 3 node KRaft (`apache/kafka:4.1.2`): `kafka-1` (`19092`), `kafka-2` (`19093`), `kafka-3` (`19094`).
- Các microservice chạy đồng thời:
  - `order-query-service`: cổng `8080`, runtime DB user `query_reader` (chỉ có `SELECT`)
  - `order-service` (`POS01`): cổng `8081`, consumer group `order-status-POS01-v1`
  - `order-service` (`POS02`): cổng `8082`, consumer group `order-status-POS02-v1`
  - `order-service` (`ONLINE`): cổng `8083`, consumer group `order-status-ONLINE-v1`
  - `inventory-service`: cổng `8084`, consumer group `inventory-v1`
  - `kitchen-service`: cổng `8085`, consumer group `kitchen-v1`

## Bảng tổng hợp kịch bản kiểm thử

| Kịch bản / Hạng mục | Kết quả | Bằng chứng |
|---|---|---|
| `mvnw.cmd -B -ntp clean verify` toàn backend (5 module) | `BUILD SUCCESS`; **59 tests** (`event-contracts` 4, `order-service` 20, `inventory-service` 14, `kitchen-service` 18, `order-query-service` 3), 0 failures/errors/skipped. Bao gồm test chứng minh `query_reader` bị chặn mọi ghi `INSERT` và constraint phân mảnh `chk_pos01_fragment_site` chặn ghi sai chi nhánh | [build.txt](build.txt) |
| Khởi tạo Kafka Topics & Logical Replication | 2 topic `order-events`, `status-events` đủ 3 partition / ISR `1,2,3`; 3 publication (`pos01_pub`, `pos02_pub`, `online_pub`) và 3 subscription (`pos01_sub`, `pos02_sub`, `online_sub`) hoàn tất initial sync với toàn bộ 6 bảng ở trạng thái `r` (`ready`) | [topics-reset.txt](topics-reset.txt), [replication-setup.txt](replication-setup.txt) |
| Trạng thái ban đầu các DB, Consumer Group & Health check | Cả 6 cổng `8080`, `8081`, `8082`, `8083`, `8084`, `8085` đều `UP`; 5 consumer group kết nối ổn định; `reporting.all_orders` có `0` đơn | [initial-state.txt](initial-state.txt) |
| D11 — Tái thiết đa nguồn (`POS01`, `POS02`, `ONLINE` $\to$ `central-db`) | Tạo đơn ở `POS01` (`COM_GA x2`), `POS02` (`CHAO_GA x1`), `ONLINE` (`COM_GA x1 + CHAO_GA x1`); hoàn tất vé `POS01` và `ONLINE` tại Bếp (`DONE`), giữ `POS02` ở `RESERVED`. Trung tâm hội tụ đủ 3 đơn (3 mã duy nhất) qua `GET /api/reporting/orders`, `GET /api/reporting/orders/{id}`, `GET /api/reporting/summary` trong `77.7 ms` | [scenario-d11-multi-source.txt](scenario-d11-multi-source.txt) |
| D11 — Mô phỏng Replication Lag (`DISABLE` / `ENABLE` subscription) | Chạy `ALTER SUBSCRIPTION pos01_sub DISABLE`; tạo và hoàn tất đơn mới tại `POS01` (`772ee275...`, trạng thái `DONE` ở nguồn) đồng thời tạo đơn tại `ONLINE` (`03c8992c...`). Trung tâm trả `404` với đơn `POS01` mới (bị trễ, `POS01` vẫn ở 1) trong khi `ONLINE` vẫn replicate lên 2 bình thường. Sau khi `ENABLE` lại `pos01_sub`, trung tâm bắt kịp hoàn toàn sau `299.1 ms` (`totalOrders = 5`) | [scenario-d11-lag-recovery.txt](scenario-d11-lag-recovery.txt) |
| D11 — Tính độc lập khi dừng `order-query-service` | Tắt `order-query-service` (`8080`), tạo đơn mới tại `POS02` (`512aa874...`) $\to$ đơn vẫn qua Kho chuyển `RESERVED` và tạo vé Bếp bình thường; bật lại `8080` $\to$ Query Service đọc được ngay bản ghi đã replicate | [scenario-d11-query-downtime.txt](scenario-d11-query-downtime.txt) |
| Đối chứng SQL xuyên 6 DB sau toàn bộ lượt chạy | `pos01`: 2 đơn, `pos02`: 2 đơn, `online`: 2 đơn; `reporting.all_orders`: 6 đơn (`distinct_order_ids = 6`), không trùng lặp/mất dòng; tồn Kho khớp tuyệt đối (`GAO = 350`, `THIT_GA = 450` sau tổng tiêu thụ 650 g gạo / 1050 g thịt gà); 6 quyết định Kho và 6 vé Bếp khớp 1:1 | [sql-final.txt](sql-final.txt) |

## Ghi chú & Đóng mốc B5

- Đã hoàn tất toàn bộ yêu cầu backend của Milestone B5 và kịch bản **D11**. Dashboard React và D12 (nhiều instance) không thuộc bằng chứng này.
- Đã bổ sung đầy đủ script tự động hóa: [up-all-infra.ps1](../../../scripts/up-all-infra.ps1), [setup-replication.ps1](../../../scripts/setup-replication.ps1), [reset-pos02.ps1](../../../scripts/reset-pos02.ps1), [reset-online.ps1](../../../scripts/reset-online.ps1), [reset-central.ps1](../../../scripts/reset-central.ps1) và cập nhật [reset-pos01.ps1](../../../scripts/reset-pos01.ps1), [reset-kafka-topics.ps1](../../../scripts/reset-kafka-topics.ps1).
- Dữ liệu test sau kiểm chứng đã được reset sạch trên cả 6 DB và cụm Kafka; các tiến trình Java kiểm thử đã được tắt và cổng mạng được giải phóng hoàn toàn.
