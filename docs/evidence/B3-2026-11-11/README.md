# Kết quả B3 ngày 11/11/2026

Chạy local trên Windows, giờ Asia/Bangkok (`+07:00`), khoảng `10:07–10:28` ngày `11/11/2026`.
Kiểm chứng toàn bộ module `event-contracts`, refactor `order-service`, và `inventory-service` (migration Flyway V1–V2, khóa giữ hàng nguyên tử PostgreSQL, chống trùng 2 lớp, outbox `status-events`, 2 instance song song, và failpoint D08).

## Cấu hình đã chạy

- Maven Wrapper 3.3.4, Maven 3.10.0, Spring Boot 4.1.2, Spring Kafka (`kafka-clients` 4.2.2), Jackson 3.
- JDK build/runtime: Temurin 25.0.4.1; compiler `release 21`.
- PostgreSQL `postgres:18.6`:
  - `distributed-order-base-pos01-db-1` (`127.0.0.1:5433`, DB `pos01_orders`, schema smoke `b3_smoke`).
  - `distributed-order-inventory-inventory-db-1` (`127.0.0.1:5435`, DB `inventory_db`, schema smoke `inventory`).
- Cụm Kafka 3 node KRaft (`apache/kafka:4.1.2`): `kafka-1` (`19092`), `kafka-2` (`19093`), `kafka-3` (`19094`).
- Topic `order-events` và `status-events`: `PartitionCount=3`, `ReplicationFactor=3`, `min.insync.replicas=2`.
- Consumer `inventory-service`: group `inventory-v1`, `StringDeserializer`, `enable.auto.commit=false`, `AckMode.RECORD`, `auto.offset.reset=earliest`, `concurrency=1` mỗi instance.
- Các tiến trình smoke test: `order-service` (`SERVER_PORT=8081`, `SITE_ID=POS01`, `SOURCE_TYPE=POS`), `inventory-1` (`SERVER_PORT=8084`), `inventory-2` (`SERVER_PORT=18084`), đều bật `OUTBOX_ENABLED=true`.
- Integration/contract tests trong `clean verify` chạy trên `b0_test`, `b1_test`, `b3_test`, `b3_kafka_test` (`@EmbeddedKafka` cho `InventoryKafkaTests`).

## Kết quả kiểm tra

| Kịch bản / Hạng mục | Kết quả | Bằng chứng |
|---|---|---|
| `mvnw.cmd -B -ntp clean verify` toàn backend | `BUILD SUCCESS`; 24 tests (`event-contracts` 4, `order-service` 6, `inventory-service` 14), 0 failures/errors/skipped | [build.txt](build.txt) |
| Reset Kafka dev topics & trạng thái khởi đầu (`NORMAL`) | 2 topic `order-events`, `status-events` đủ 3 partition / ISR `1,2,3`; API `/api/ingredients`, `/api/recipes` trả seed `GAO=1000`, `THIT_GA=1500`; `/api/reservations/{id}` trả `404`; `reset-inventory.ps1` tự từ chối (`ExitCode=1`) khi `8084/18084` đang mở; `inventory-v1` chia 2 member (`Stable`) | [topics-reset.txt](topics-reset.txt), [initial-state.txt](initial-state.txt), [consumer-group-initial.txt](consumer-group-initial.txt) |
| D01 — Luồng bình thường (`NORMAL`) | Hai đơn `COM_GA ×1` (`b3fe0e06-6f7c-4745-9926-5f1d7d968fcb`, p=2 → `inventory-2`) và `CHAO_GA ×1` (`4b29ac1a-5fa1-4e8e-95d7-ace4b5c78a25`, p=1 → `inventory-1`) đều `RESERVED`; tồn còn `GAO=850`, `THIT_GA=1250`; 2 event `Reserved` lên `status-events`; `LAG=0` | [scenario-d01-normal.txt](scenario-d01-normal.txt), [sql-final.txt](sql-final.txt) |
| D02 — Thiếu 1 nguyên liệu (`MISSING_RICE`: `GAO=50`, `THIT_GA=1500`) | Đơn `945115d3-968f-4a6e-be7e-65a44163007a` (`COM_GA ×1`, p=2 → `inventory-2`) nhận `FAILED / INSUFFICIENT_INGREDIENT`; `GAO=50`, `THIT_GA=1500` không bị trừ một phần; `0` dòng reservation; 1 event `Failed` lên `status-events`; không retry lặp (`LAG=0`) | [scenario-d02-missing-rice.txt](scenario-d02-missing-rice.txt) |
| D03 — Hai instance tranh phần cuối (`LAST_PORTION`: `GAO=1000`, `THIT_GA=150`) | Barrier phát đồng thời `3580d1e8-b8a0-419a-813e-e953b9c5809a` (p=0 → `inventory-1`) và `4cd6c05d-a667-44fa-9635-0a7bf8d82297` (p=2 → `inventory-2`). Cửa sổ transaction chồng nhau thực tế (`inv1=[03:18:58.247755Z..03:18:58.335Z]`, `inv2=[03:18:58.248822Z..03:18:58.362Z]`). Kết quả 1 `RESERVED` + 1 `FAILED / INSUFFICIENT_INGREDIENT`; tồn cuối `GAO=900`, `THIT_GA=0` | [scenario-d03-last-portion.txt](scenario-d03-last-portion.txt) |
| D04 & D05 — Replay cùng `eventId` (×3) và cùng `orderId` đổi `eventId` / xung đột payload | Đơn `0cfa440c-3ccd-46cd-9ac3-052cd87dc1c9` (p=2) `RESERVED` (`GAO=900`, `THIT_GA=1350`). Replay nguyên payload 3 lần → 3× `DUPLICATE_EVENT`; cùng payload + `eventId` mới → `DUPLICATE_ORDER`; payload xung đột (`CHAO_GA ×2`) + `eventId` mới → log `ERROR` & lưu `PAYLOAD_CONFLICT` trong `processed_events`. Tồn giữ nguyên `900/1350`, chỉ 1 `Reserved` trên `status-events`, offset `6/6` (`LAG=0`) | [scenario-d04-d05-replay.txt](scenario-d04-d05-replay.txt) |
| D08 — Consumer halt sau DB commit và trước commit offset Kafka | Bật `haltAfterCommitOnce=true` trên `inventory-1` (p=2). Đơn `3a3134e3-ab34-4f8e-b49b-29bda6655dcd` khiến `inventory-1` commit `RESERVED` (`900/1350`) rồi gọi `Runtime.halt(137)` (`ExitCode=137`, tạo marker file) trước ack Kafka (`CURRENT-OFFSET=-`, `LAG=1`). `inventory-2` phát outbox `Reserved`, chờ rebalance chiếm p=2, nhận lại offset `0`, dedup với `DUPLICATE_EVENT`, commit offset `1` (`LAG=0`), tồn vẫn `900/1350` | [scenario-d08-failpoint.txt](scenario-d08-failpoint.txt), [kafka-consumer-status-events.txt](kafka-consumer-status-events.txt), [sql-final.txt](sql-final.txt) |

## Ghi chú và phạm vi

- Trong [scenario-d01-normal.txt](scenario-d01-normal.txt), câu truy vấn phụ vào `b3_smoke.orders` ghi nhầm tên cột `source_site_id` (cột đúng của Order Service là `site_id`); hai đơn D01 trong `b3_smoke.orders` vẫn được truy vấn đầy đủ ở [sql-final.txt](sql-final.txt), và `b3_smoke.outbox_events` cùng toàn bộ bảng Inventory ở D01 đều đối chứng đầy đủ.
- Ở kịch bản D03, hai đơn tranh tồn cùng xuất phát từ `POS01` với payload `OrderCreated` thật qua barrier Producer vì kênh `Online` và `POS02` thuộc mốc B5; phiên bản hai nguồn khác nhau sẽ được kiểm tra lại ở B5.
- Sau khi hoàn tất các lượt đo và lưu bằng chứng, các app test trên `8081/8084/18084` đã được dừng, tệp marker D08 đã xóa, schema `inventory` được reset sạch về `NORMAL`, và hai dev topic Kafka được tạo lại sạch sẽ.
- Ngoài phạm vi B3 (chưa triển khai/kiểm thử trong lượt này): Kitchen Service và đồng bộ ngược trạng thái về Order Service (B4), POS02/Online và PostgreSQL logical replication (B5–B6), failpoint D07 phía `order-service`, và cơ chế bù trừ hoàn nguyên liệu khi Bếp lỗi (D15).
