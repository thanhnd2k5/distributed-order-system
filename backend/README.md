# Backend — Order Service POS01

Base Java 21, Spring Boot 4.1.2, Maven 3.10.0 qua Wrapper và PostgreSQL 18.6.
Hiện có menu, Actuator health, luồng B1 tạo/đọc đơn và worker B2 gửi outbox lên Kafka.
Xem [contract HTTP](../docs/contracts/order-api.yaml) và [hướng dẫn làm việc song song](../docs/TEAM_START.md).

Đã build và chạy thử ngày 01/11/2026 trên JDK 25, target 21; hai integration test và kiểm tra
restart đều đạt. Xem [bằng chứng B0](../docs/evidence/B0-2026-11-01/README.md).

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

## Build và test

`clean verify` chạy integration test với PostgreSQL thật, DB phải healthy trước.
Test dùng schema `b0_test` và `b1_test` trong DB cấu hình.
Hai bộ test này tắt publisher để chạy độc lập với Kafka.
Test kiểm tra migration, health, menu, tạo và đọc đơn qua HTTP, gửi lại đồng thời,
rollback khi PostgreSQL từ chối outbox, dữ liệu đầu vào và CORS từ Vite.
Schema test được giữ để kiểm tra; không dùng DB có dữ liệu quan trọng làm DB test.

Flyway tạo schema/bảng và seed, Hibernate chỉ validate. Sau khi migration đã được nhóm dùng,
thay đổi schema/seed bằng migration mới để giữ checksum.

## Dừng và khởi động lại

Dừng app bằng `Ctrl+C`. Từ root repository:

```powershell
docker compose -f infra/compose.base.yaml stop pos01-db
docker compose -f infra/compose.base.yaml up -d --wait pos01-db
```

Volume `distributed-order-base_pos01-data` giữ dữ liệu. Lệnh `down -v` sẽ xóa volume,
không dùng cho thao tác restart thông thường.

Thiết kế và các mốc tiếp theo: [BACKEND_BOOTSTRAP](../docs/BACKEND_BOOTSTRAP.md),
[PROJECT_SPEC](../docs/PROJECT_SPEC.md).
