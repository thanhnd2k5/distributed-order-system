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
- B2 gửi outbox lên cụm Kafka ba broker. Xem [cách chạy Kafka và thử phục hồi](backend/README.md#chạy-kafka-cho-b2).

Tài liệu ngày 30/10/2026 là bản thiết kế triển khai v0.1. Những lựa chọn kỹ thuật bổ sung được đánh dấu là đề xuất để nhóm rà soát; tài liệu chưa phải bằng chứng hệ thống đã được triển khai hoặc kiểm thử.

Phạm vi môn học không bao gồm auth, thanh toán, quản lý khách hàng, nhà cung cấp, nhập kho hay triển khai production. Tồn kho và công thức được khởi tạo bằng dữ liệu mẫu.
