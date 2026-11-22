# Hệ thống đơn hàng POS và Online cho Bếp Kho trung tâm

Đồ án môn Xây dựng hệ thống phân tán. Hệ thống tiếp nhận đơn từ các POS và kênh Online, chuyển qua Kafka để Kho giữ nguyên liệu trước khi Bếp xử lý, đồng thời nhân bản dữ liệu đơn hàng về trung tâm để truy vấn.

Mục tiêu là triển khai và chứng minh các cơ chế phân tán bằng những kịch bản chạy thật: phân mảnh dữ liệu, replication, transaction cục bộ, eventual consistency, outbox, xử lý trùng và phục hồi sau mất kết nối.

- [Tài liệu thiết kế chi tiết dự án](docs/PROJECT_SPEC.md)
- [Hướng dẫn dựng base backend từ đầu](docs/BACKEND_BOOTSTRAP.md)
- [Kịch bản demo và tiêu chí nghiệm thu](docs/DEMO_PLAN.md)
- Frontend có sẵn: `react-starter-kit-zq/`, dùng React và JavaScript.
- Backend đã chọn hướng Java và Spring Boot; PostgreSQL và Kafka kế thừa tài liệu dự án.
- Base backend B0: `backend/` và `infra/compose.base.yaml`; xem [cách chạy local](backend/README.md).
- Luồng B1 tạo/đọc đơn và ghi outbox đã có trong Order Service. Xem [contract và cách chia việc](docs/TEAM_START.md).
- B2 gửi outbox lên cụm Kafka ba broker. Xem [cách chạy Kafka và thử phục hồi](backend/README.md#chạy-kafka-cho-b2) và [bằng chứng B2](docs/evidence/B2-2026-11-11/README.md).
- B3 bổ sung module `event-contracts` và Inventory Service (`localhost:8084`) tiêu thụ `order-events`, giữ nguyên liệu nguyên tử và phát `Reserved`/`Failed` lên `status-events`. Xem [hướng dẫn Inventory](backend/README.md#inventory-service-b3) và [bằng chứng B3](docs/evidence/B3-2026-11-11/README.md).
- B4 bổ sung module Kitchen Service (`localhost:8085`) quản lý vé bếp, hoàn thành vé phát `Done`, và Order Service tiêu thụ `status-events` đồng bộ trạng thái đơn (`PENDING_INVENTORY -> RESERVED -> DONE/FAILED`) kèm ma trận D13 chống revert. Xem [hướng dẫn Kitchen](backend/README.md#kitchen-service-b4--đồng-bộ-trạng-thái-đơn) và [bằng chứng B4](docs/evidence/B4-2026-11-11/README.md).
- B5 bổ sung cụm nguồn `POS02` (`localhost:8082`) và `ONLINE` (`localhost:8083`), hạ tầng PostgreSQL Logical Replication về DB trung tâm `central-db` (`localhost:5434`), cùng module `order-query-service` (`localhost:8080`, user chỉ đọc `query_reader`) và kịch bản kiểm chứng **D11**. Xem [hướng dẫn Replication & Query Service](backend/README.md#replication-đa-nguồn--order-query-service-b5) và [bằng chứng B5](docs/evidence/B5-2026-11-12/README.md).

Tài liệu ngày 30/10/2026 là bản thiết kế triển khai v0.1. Những lựa chọn kỹ thuật bổ sung được đánh dấu là đề xuất để nhóm rà soát; tài liệu chưa phải bằng chứng hệ thống đã được triển khai hoặc kiểm thử.

Phạm vi môn học không bao gồm auth, thanh toán, quản lý khách hàng, nhà cung cấp, nhập kho hay triển khai production. Tồn kho và công thức được khởi tạo bằng dữ liệu mẫu.
