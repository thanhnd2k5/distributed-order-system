# Backend — Order Service (POS01) & Inventory Service

Base Java 21, Spring Boot 4.1.2, Maven 3.10.0 qua Wrapper và PostgreSQL 18.6.
Hiện gồm ba module Maven dưới `backend/`:
- `event-contracts`: record/enum dùng chung cho `OrderCreated`, `Reserved`, `Failed`, `Done` (`docs/contracts/events/*.v1.json`).
- `order-service`: menu, Actuator health, luồng B1 tạo/đọc đơn và worker B2 gửi outbox `OrderCreated` lên Kafka (`order-events`).
- `inventory-service`: consumer B3 nhận `order-events`, giữ nguyên liệu nguyên tử trong PostgreSQL, chống trùng theo `eventId`/`orderId` và phát `Reserved`/`Failed` lên `status-events`.

Xem [contract HTTP](../docs/contracts/order-api.yaml) và [hướng dẫn làm việc song song](../docs/TEAM_START.md).

Đã build và chạy thử B0 ngày 01/11/2026, B2 và B3 ngày 11/11/2026 trên JDK 25 (compiler target 21). Xem [bằng chứng B0](../docs/evidence/B0-2026-11-01/README.md), [bằng chứng B2](../docs/evidence/B2-2026-11-11/README.md) và [bằng chứng B3](../docs/evidence/B3-2026-11-11/README.md).

## Chạy local bằng PowerShell

Cần JDK 21 (hoặc JDK tương thích), Docker Desktop đang chạy và mạng để tải dependency lần đầu.
Từ root repository:

```powershell
docker compose -f infra/compose.base.yaml up -d --wait pos01-db
Set-Location backend
.\mvnw.cmd -version
.\mvnw.cmd -B -ntp -pl order-service -am clean verify
.\mvnw.cmd -pl order-service spring-boot:run
```

Mở terminal khác:

```powershell
Invoke-RestMethod http://localhost:8081/actuator/health
Invoke-RestMethod http://localhost:8081/api/menu

$body = @{
    clientRequestId = [guid]::NewGuid().ToString()
    items = @(@{ dishId = 'COM_GA'; quantity = 2 })
} | ConvertTo-Json -Depth 3
$order = Invoke-RestMethod http://localhost:8081/api/orders -Method Post -ContentType 'application/json' -Body $body
Invoke-RestMethod "http://localhost:8081/api/orders/$($order.orderId)"
Invoke-RestMethod http://localhost:8081/api/orders
```

Health trả `UP`. Menu trả mảng hai món, theo thứ tự `CHAO_GA`, `COM_GA`.
Chỉ các món `active = true` được trả về.

Lần đầu gửi `$body` nhận `201` và trạng thái `PENDING_INVENTORY`.
Gửi lại cùng `$body` nhận `200` với cùng mã đơn. Đổi nội dung nhưng giữ mã request nhận `409`.
Để xem outbox local, chạy từ root repository:

```powershell
docker compose -f infra/compose.base.yaml exec -T pos01-db psql -U order_app -d pos01_orders -c 'SELECT event_type, topic, message_key, sent_at, payload FROM pos01.outbox_events ORDER BY created_at DESC LIMIT 5;'
```

Để chạy JAR sau khi build:

```powershell
java -jar order-service/target/order-service-0.0.1-SNAPSHOT.jar
```

## Cấu hình

| Biến môi trường | Mặc định | Ý nghĩa |
|---|---|---|
| `SERVER_PORT` | `8081` | Cổng HTTP |
| `DB_URL` | `jdbc:postgresql://localhost:5433/pos01_orders` | DB POS01 trên host |
| `DB_USERNAME` | `order_app` | User DB local demo |
| `DB_PASSWORD` | `order_demo` | Password DB local demo |
| `DB_SCHEMA` | `pos01` | Schema do Flyway quản lý |
| `SITE_ID` | `POS01` | Mã nguồn, chữ hoa/số/gạch dưới, bắt đầu bằng chữ, tối đa 50 ký tự |
| `SOURCE_TYPE` | `POS` | `POS` hoặc `ONLINE` |
| `FRONTEND_ORIGIN` | `http://localhost:5173` | Origin React được phép GET/POST vào `/api/**` |
| `OUTBOX_ENABLED` | `false` | Bật worker gửi outbox |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:19092,localhost:19093,localhost:19094` | Địa chỉ broker cho app chạy trên máy host |

Ví dụ đặt biến cho terminal hiện tại: `$env:SITE_ID = 'POS01'`.
Spring và Compose hiện dùng mặc định local đồng bộ; Spring không tự nạp file `.env`.
Khi chạy site khác, chọn DB/schema riêng theo thiết kế trước khi đổi site config.

## Chạy Kafka cho B2

Từ root repository, khởi động cụm Kafka ba broker và tạo hai topic. Lệnh tạo topic có thể chạy lại.

```powershell
docker compose -f infra/compose.kafka.yaml up -d --wait
docker compose -f infra/compose.kafka.yaml run --rm kafka-init
```

Trong terminal chạy Order Service, đặt `$env:OUTBOX_ENABLED = 'true'` trước lệnh Maven hoặc Java.
Worker quét mỗi 500 ms, khóa từng dòng outbox trong lúc chờ Kafka xác nhận và ghi `sent_at` sau khi nhận ACK.
Lỗi gửi giữ dòng ở trạng thái chờ và lên lịch thử lại tối đa 10 giây. Mỗi lần gửi lại dùng cùng key và payload đã lưu.
Nếu app dừng sau ACK nhưng trước khi commit `sent_at`, consumer có thể nhận lại cùng `eventId`.

Mở consumer thử từ root repository.

```powershell
docker compose -f infra/compose.kafka.yaml exec -T kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --topic order-events --from-beginning --property print.key=true
```

Để thử phục hồi, giữ Order Service và PostgreSQL chạy, dừng ba broker bằng
`docker compose -f infra/compose.kafka.yaml stop kafka-1 kafka-2 kafka-3`, rồi gửi một đơn mới.
API vẫn trả `201`. Trong outbox, kiểm tra `sent_at` còn trống và `attempt_count` tăng.
Chạy lại `up -d --wait` như trên. Kiểm tra dòng outbox của đơn đã có `sent_at` và consumer nhận
`OrderCreated` với key là mã đơn. Các broker dùng volume riêng để giữ topic và dữ liệu qua restart.

Để thử mất một broker, chờ cả ba replica đồng bộ rồi dừng riêng `kafka-3`.
Tạo đơn mới và kiểm tra consumer vẫn đọc được event. Sau đó chạy lại `up -d --wait`
và kiểm tra các partition có đủ ba ISR.

Xem kết quả kiểm chứng build, outbox recovery và mất một broker tại [bằng chứng B2](../docs/evidence/B2-2026-11-11/README.md).

## Inventory Service (B3)

Inventory Service dùng PostgreSQL riêng (`infra/compose.inventory.yaml`, cổng `127.0.0.1:5435`, DB `inventory_db`, schema mặc định `inventory`), Flyway V1 tạo các bảng tồn kho/công thức/quyết định/outbox và V2 seed dữ liệu mẫu `NORMAL` (`GAO` 1.000 g, `THIT_GA` 1.500 g).

Từ root repository:

```powershell
docker compose -f infra/compose.inventory.yaml up -d --wait inventory-db
docker compose -f infra/compose.kafka.yaml up -d --wait
docker compose -f infra/compose.kafka.yaml run --rm kafka-init
Set-Location backend
$env:OUTBOX_ENABLED = 'true'
.\mvnw.cmd -pl inventory-service spring-boot:run
```

Kiểm tra API đọc của Kho trên terminal khác:

```powershell
Invoke-RestMethod http://localhost:8084/actuator/health
Invoke-RestMethod http://localhost:8084/api/ingredients
Invoke-RestMethod http://localhost:8084/api/recipes
# Sau khi tạo đơn bên Order Service (8081):
Invoke-RestMethod "http://localhost:8084/api/reservations/<orderId>"
```

Để chạy instance Inventory thứ hai song song cùng consumer group `inventory-v1`:

```powershell
$env:SERVER_PORT = '18084'
$env:INSTANCE_ID = 'inventory-2'
$env:OUTBOX_ENABLED = 'true'
java -jar inventory-service/target/inventory-service-0.0.1-SNAPSHOT.jar
```

### Biến môi trường Inventory Service

| Biến môi trường | Mặc định | Ý nghĩa |
|---|---|---|
| `SERVER_PORT` | `8084` | Cổng HTTP (dùng `18084` cho instance 2) |
| `DB_URL` | `jdbc:postgresql://localhost:5435/inventory_db` | DB Inventory trên host |
| `DB_USERNAME` | `inventory_app` | User DB Inventory local demo |
| `DB_PASSWORD` | `inventory_demo` | Password DB Inventory local demo |
| `DB_SCHEMA` | `inventory` | Schema do Flyway quản lý |
| `INSTANCE_ID` | `inventory-1` | Định danh instance xuất hiện trong mọi log nghiệp vụ |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:19092,localhost:19093,localhost:19094` | Địa chỉ Kafka broker từ host |
| `KAFKA_GROUP_ID` | `inventory-v1` | Consumer group đọc `order-events` |
| `KAFKA_LISTENER_AUTO_STARTUP` | `true` | Có tự động bật `@KafkaListener` khi khởi động không |
| `FRONTEND_ORIGIN` | `http://localhost:5173` | Origin React được phép GET vào `/api/**` |
| `OUTBOX_ENABLED` | `false` | Bật worker phát `Reserved`/`Failed` lên `status-events` |
| `DEMO_FAILPOINT_HALT_AFTER_COMMIT_ONCE` | `false` | Failpoint D08: dừng JVM (`exit 137`) một lần sau khi commit DB và trước khi commit offset Kafka |
| `DEMO_FAILPOINT_MARKER_FILE` | `target/d08-failpoint.marker` | Tệp đánh dấu để failpoint D08 chỉ kích hoạt đúng 1 lần |

### Script tiện ích demo (`scripts/`)

Chạy từ root repository (lưu ý `reset-inventory.ps1` và `reset-kafka-topics.ps1` tự kiểm tra và từ chối chạy nếu app vẫn đang mở cổng tương ứng):

- `.\scripts\reset-inventory.ps1 -Preset NORMAL|LAST_PORTION|MISSING_RICE|OFFLINE_LIMITED`: xóa các bảng xử lý đơn/outbox và đặt lại tồn `GAO`/`THIT_GA` theo bộ test.
- `.\scripts\reset-kafka-topics.ps1`: xóa group `inventory-v1` và tạo lại 2 dev topic `order-events`, `status-events` (3 partition, RF=3, `min.insync.replicas=2`). Chỉ dùng cho môi trường dev.
- `.\scripts\partition-of.ps1 -Key <orderId> -Partitions 3`: tính partition Kafka (`murmur2`) của một `orderId` để chọn cặp đơn rơi vào 2 instance khác nhau trong kịch bản D03/D08.

Xem kết quả kiểm chứng build, 2 instance đồng thời D03, replay D04/D05 và failpoint D08 tại [bằng chứng B3](../docs/evidence/B3-2026-11-11/README.md).

## Build và test

`.\mvnw.cmd -B -ntp clean verify` trong `backend/` chạy test cho cả 3 module (`event-contracts`, `order-service`, `inventory-service`) trên PostgreSQL thật, nên cả hai container `pos01-db` (`5433`) và `inventory-db` (`5435`) phải đang healthy trước khi chạy toàn reactor. Có thể chạy riêng một module bằng `-pl order-service -am` hoặc `-pl inventory-service -am`.
Test dùng schema `b0_test`, `b1_test` (trên `pos01_orders`) và `b3_test`, `b3_kafka_test` (trên `inventory_db`). Test `InventoryKafkaTests` dùng `@EmbeddedKafka` nên bộ `clean verify` không phụ thuộc cụm Kafka Docker đang bật.
Các bộ test tắt publisher (trừ test outbox riêng trong `@EmbeddedKafka`) và kiểm tra migration, health, menu, đặt đơn đồng thời, giữ nguyên liệu nguyên tử, lock contention thật, replay/conflict, poison message handling và rollback khi DB lỗi.
Schema test được giữ để kiểm tra; không dùng DB có dữ liệu quan trọng làm DB test.

Flyway tạo schema/bảng và seed, Hibernate chỉ validate. Sau khi migration đã được nhóm dùng,
thay đổi schema/seed bằng migration mới để giữ checksum.

## Dừng và khởi động lại

Dừng app bằng `Ctrl+C`. Từ root repository:

```powershell
docker compose -f infra/compose.base.yaml stop pos01-db
docker compose -f infra/compose.base.yaml up -d --wait pos01-db
docker compose -f infra/compose.inventory.yaml stop inventory-db
docker compose -f infra/compose.inventory.yaml up -d --wait inventory-db
```

Các volume (`distributed-order-base_pos01-data`, `distributed-order-inventory_inventory-data`) giữ dữ liệu qua restart. Lệnh `down -v` sẽ xóa volume, không dùng cho thao tác restart thông thường.

Thiết kế và các mốc tiếp theo: [BACKEND_BOOTSTRAP](../docs/BACKEND_BOOTSTRAP.md),
[PROJECT_SPEC](../docs/PROJECT_SPEC.md).
