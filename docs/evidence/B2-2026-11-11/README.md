# Kết quả B2 ngày 11/11/2026

Chạy local trên Windows, giờ Asia/Bangkok (`+07:00`), khoảng `00:17–00:25` ngày `11/11/2026`.
Kiểm tra lại build, integration test và toàn bộ smoke test messaging B2 trước khi đóng gói commit.

## Cấu hình đã chạy

- Maven Wrapper 3.3.4, Maven 3.10.0, Spring Boot 4.1.2, Spring Kafka (`kafka-clients` 4.2.2).
- JDK build/runtime: Temurin 25.0.4.1; compiler `release 21`.
- PostgreSQL image `postgres:18.6`, container `distributed-order-base-pos01-db-1`, cổng `127.0.0.1:5433`.
- Kafka cluster 3 node KRaft (`apache/kafka:4.1.2`): `kafka-1` (`19092`), `kafka-2` (`19093`), `kafka-3` (`19094`).
- Topic `order-events` và `status-events`: `PartitionCount=3`, `ReplicationFactor=3`, `min.insync.replicas=2`.
- Producer config: `acks=all`, `enable.idempotence=true`, `max.block.ms=2000`, `request.timeout.ms=2000`, `delivery.timeout.ms=5000`.
- App smoke test chạy bằng JAR đóng gói với `SERVER_PORT=18081`, `DB_SCHEMA=b2_smoke`, `OUTBOX_ENABLED=true`, `SITE_ID=POS01`, `SOURCE_TYPE=POS`.
- Integration test chạy trên `b0_test` và `b1_test` (`outbox.enabled=false`).

## Kết quả kiểm tra

| Kịch bản / Hạng mục | Kết quả | Bằng chứng |
|---|---|---|
| `mvnw.cmd -B -ntp -pl order-service -am clean verify` | `BUILD SUCCESS`; 6 tests (`BackendBaseTests` 2, `OrderIntegrationTests` 4), 0 failures/errors/skipped | [build.txt](build.txt) |
| Khởi tạo và cấu hình topic Kafka | 3 broker healthy; `order-events` và `status-events` có 3 partition, RF=3, `min.insync.replicas=2`, ISR đủ `1,3,2` | [topics-initial.txt](topics-initial.txt) |
| Luồng thường + retry HTTP (`clientRequestId`) | `POST #1` trả `201`, `POST #2` trả `200` cùng `orderId` (`ed1c302c-8871-4461-bbf6-5114c5883fd9`); outbox ghi `sent=t`, `attempt_count=0` | [scenario-1-normal.txt](scenario-1-normal.txt) |
| Ngắt toàn bộ 3 broker Kafka rồi phục hồi (D06 core) | Khi tắt `kafka-1/2/3`, `POST` vẫn trả `201` (`2ca946ff-675c-4213-a550-1d2cae9ded80`), `sent=f`, worker backoff `1s -> 2s -> 4s -> 8s` (`attempt_count=4`). Sau khi bật lại cụm, outbox tự gửi thành công (`sent=t` lúc `17:23:21Z`) | [scenario-2-kafka-outage.txt](scenario-2-kafka-outage.txt), [app-smoke.txt](app-smoke.txt) |
| Tắt 1 broker follower (`kafka-3`) khi đang gửi | ISR thu còn `2,1`; đơn `a7cdff29-154f-47c2-a4a9-45222ad030de` vẫn trả `201` và gửi thành công (`sent=t`, `attempt_count=0`) ngay khi `kafka-3` dừng; bật lại thì ISR đủ `2,1,3` | [scenario-3-one-broker-down.txt](scenario-3-one-broker-down.txt) |
| Tắt 1 broker đang làm leader (`kafka-2`) | Leader chuyển sang broker `3` (partition 0, 1) và `1` (partition 2), ISR `1,3`; đơn `e62b5409-d937-4cba-ab2a-a8844f468150` vẫn trả `201` và gửi `sent=t` (`attempt_count=0`) ngay khi `kafka-2` dừng; bật lại thì ISR đủ `1,3,2` | [scenario-3-one-broker-down.txt](scenario-3-one-broker-down.txt) |
| Đối chứng consumer và DB cuối phiên | Consumer đọc `--from-beginning` đủ 6 message trên `order-events` (2 đơn từ 07/11 + 4 đơn ngày 11/11), key là `orderId`, rải trên 3 partition `0`, `1`, `2`; DB `b2_smoke` có 6/6 outbox `sent=t` | [kafka-consumer-order-events.txt](kafka-consumer-order-events.txt), [sql-final.txt](sql-final.txt) |

## Ghi chú và phạm vi

- Schema `b2_smoke` và volume Kafka giữ lại 2 đơn từ lượt chạy đầu ngày `07/11/2026` (`add87559-4590-4462-9b47-2963b349d9ce`, `7b20927a-fc92-4998-82cc-003c24a581db`), cộng thêm 4 đơn mới trong lượt kiểm tra ngày `11/11/2026` thành 6 đơn / 6 event khớp 1-1 giữa PostgreSQL và Kafka.
- Chạy smoke test trên `b2_smoke` (`18081`) để giữ schema mặc định `pos01` sạch; build/runtime sử dụng JDK 25 với compiler target 21.
- Bằng chứng này xác nhận xong mốc **B2 (Kafka 3 broker + OutboxPublisher)**. Các phần sau chưa triển khai nên chưa kiểm thử trong lượt này: B3 (Inventory Service), B4 (Kitchen + cập nhật trạng thái về Order Service), B5 (đa nguồn + replication) và failpoint D07/D08.
