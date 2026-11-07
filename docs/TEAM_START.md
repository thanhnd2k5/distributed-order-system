# Bắt đầu làm việc song song

Mỗi người chạy stack local trên máy mình theo [hướng dẫn backend](../backend/README.md).
Điểm ghép giữa các phần là [OpenAPI của Order Service](contracts/order-api.yaml) và các event JSON v1 bên dưới.

| Phần | Có thể bắt đầu ngay | Điểm ghép |
|---|---|---|
| Frontend POS | Dùng API menu, tạo đơn, danh sách và chi tiết trên `localhost:8081`. Mock các trạng thái Kho/Bếp để làm giao diện. | Giữ nguyên `clientRequestId` khi retry, dùng ID mới cho lần đặt mới. Đọc trạng thái từ chi tiết đơn. |
| Kafka và outbox | Xây publisher từ các dòng chưa gửi trong `outbox_events`. Dùng `topic`, `message_key` và gửi toàn bộ JSON trong cột `payload`. | Key là `orderId`. Giữ nguyên event ID và JSON khi gửi lại. |
| Inventory | Dùng fixture OrderCreated để viết xử lý giữ hàng và test PostgreSQL của Kho. | Xuất Reserved hoặc Failed sau khi quyết định giữ hàng. |
| Kitchen | Dùng fixture Reserved để viết tạo phiếu và xử lý hoàn tất. | Chỉ tạo phiếu từ Reserved. Xuất Done khi hoàn tất. |
| Order status | Dùng fixture Reserved, Failed và Done để test cập nhật đơn local. | Chỉ xử lý sự kiện của đúng `sourceSiteId`. |

| Event | Topic | Mẫu JSON |
|---|---|---|
| OrderCreated | `order-events` | [order-created.v1.json](contracts/events/order-created.v1.json) |
| Reserved | `status-events` | [reserved.v1.json](contracts/events/reserved.v1.json) |
| Failed | `status-events` | [failed.v1.json](contracts/events/failed.v1.json) |
| Done | `status-events` | [done.v1.json](contracts/events/done.v1.json) |

B1 ghi OrderCreated vào PostgreSQL. Ba mẫu trạng thái là fixture để nhóm phát triển phần nhận và phát độc lập.
Dùng chuỗi OrderCreated → Reserved → Done cho ca thành công, và OrderCreated → Failed cho ca từ chối.
Khi mô phỏng thêm đơn, thay `orderId` và tạo `eventId` riêng cho từng sự kiện.

Mỗi service sở hữu DB và migration của mình. Trao đổi trước khi đổi tên trường, topic, key hoặc trạng thái trong contract.
Khi phần xử lý đã chạy với fixture, dùng cùng dữ liệu để kiểm tra lại qua Kafka khi ghép.
