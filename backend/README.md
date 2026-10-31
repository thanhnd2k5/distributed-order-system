# Backend B0 — Order Service POS01

Base Java 21, Spring Boot 4.1.2, Maven 3.10.0 qua Wrapper và PostgreSQL 18.6.
Hiện có menu đọc từ DB, Flyway migration và Actuator health. Mốc B1 sẽ thêm đơn hàng và outbox.

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
```

Health trả `UP`. Menu trả mảng hai món, theo thứ tự `CHAO_GA`, `COM_GA`.
Chỉ các món `active = true` được trả về.

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

Ví dụ đặt biến cho terminal hiện tại: `$env:SITE_ID = 'POS01'`.
Spring và Compose hiện dùng mặc định local đồng bộ; Spring không tự nạp file `.env`.
Khi chạy site khác, chọn DB/schema riêng theo thiết kế trước khi đổi site config.

## Build và test

`clean verify` chạy integration test với PostgreSQL thật, DB phải healthy trước.
Test dùng schema `b0_test` trong DB cấu hình, không thay menu ở schema `pos01`.
Test kiểm tra hai migration thành công, HTTP health, seed tiếng Việt, thứ tự món và lọc món inactive.
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
