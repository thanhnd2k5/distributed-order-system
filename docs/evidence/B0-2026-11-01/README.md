# Kết quả B0 ngày 01/11/2026

Chạy local trên Windows, giờ Asia/Bangkok, khoảng 01:05–01:08.
Workspace chưa có commit cho các file hiện tại (`git status` hiển thị untracked).

## Cấu hình đã chạy

- Maven Wrapper 3.3.4, Maven 3.10.0, Spring Boot 4.1.2.
- JDK build/runtime: Temurin 25.0.4.1; compiler `release 21`.
- PostgreSQL image `postgres:18.6`, container `distributed-order-base-pos01-db-1`.
- DB `pos01_orders`, cổng host `127.0.0.1:5433`, volume `distributed-order-base_pos01-data`.
- App mặc định POS01/POS, schema `pos01`, HTTP 8081.
- Integration test dùng PostgreSQL trên cùng DB, schema riêng `b0_test`.

## Kết quả

| Kiểm tra | Kết quả | Bằng chứng |
|---|---|---|
| `mvnw.cmd -B -ntp -pl order-service -am clean verify` | BUILD SUCCESS; 2 tests, 0 failures/errors/skipped | [build.txt](build.txt) |
| Migration và Hibernate validate | Flyway V1/V2 thành công; entity/schema khớp | [build.txt](build.txt), [SQL trước restart](sql-before.txt) |
| Health qua HTTP | 200, `status=UP` | [health-before.json](health-before.json) |
| Menu qua HTTP | 200, CHAO_GA/Cháo gà và COM_GA/Cơm gà | [menu-before.json](menu-before.json) |
| Lọc món inactive | Test chèn món inactive, API chỉ trả hai món active, dọn dòng test | [build.txt](build.txt), `BackendBaseTests` |
| Restart app + PostgreSQL | DB healthy; SQL trước/sau giống nhau, vẫn hai món và V1/V2, không seed trùng | [SQL sau restart](sql-after.txt), [startup](startup-after-restart.txt) |
| API sau restart | Health UP; menu đúng hai món | [health-after.json](health-after.json), [menu-after.json](menu-after.json) |
| Chạy app từ terminal | Cả `java -jar` và `mvnw.cmd -pl order-service spring-boot:run` thành công | [startup](startup-after-restart.txt) ghi lần chạy Maven |
| Đổi config qua env | `SITE_ID=ONLINE`, `SOURCE_TYPE=ONLINE`, `DB_SCHEMA=b0_test` khởi động thành công ở chế độ non-web | [config-online.txt](config-online.txt) |
| Config không hợp lệ | `SITE_ID=invalid` bị từ chối startup, exit khác 0, lỗi binding nêu đúng biến môi trường | [config-invalid.txt](config-invalid.txt) |

Lượt restart đã dừng app JAR, chạy `docker compose -f infra/compose.base.yaml restart pos01-db`,
chờ healthy bằng `up -d --wait`, rồi chạy app lại bằng Maven và đối chiếu SQL/API.
Không xóa volume. Flyway có thêm một dòng lịch sử tạo schema không có version; V1/V2 là hai migration nghiệp vụ.

Build có warning metadata repository phụ trả 401/timeout và Mockito tự nạp Java agent trên JDK 25;
dependency vẫn resolve, build/test đều thành công. Log được giữ nguyên để đối chiếu.

Đây là bằng chứng cho base B0. Chưa chạy trên runtime JDK 21; chưa triển khai hoặc kiểm thử
đơn hàng, outbox, Kafka, Inventory, Kitchen hay replication. Các kịch bản D01–D13 vẫn chưa chạy.
