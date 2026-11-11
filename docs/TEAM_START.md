# Bắt đầu làm việc song song

Mỗi người chạy stack local trên máy mình theo [hướng dẫn backend](../backend/README.md).
Điểm ghép giữa các phần là [OpenAPI của Order Service](contracts/order-api.yaml) và các event JSON v1 bên dưới.

| Phần | Có thể bắt đầu ngay | Điểm ghép |
|---|---|---|
| Frontend POS | Dùng API menu, tạo đơn, danh sách và chi tiết trên `localhost:8081`. Mock các trạng thái Kho/Bếp để làm giao diện. | Giữ nguyên `clientRequestId` khi retry, dùng ID mới cho lần đặt mới. Đọc trạng thái từ chi tiết đơn. |
| Kafka và outbox | Chạy cụm Kafka và bật publisher của Order Service theo [hướng dẫn B2](../backend/README.md#chạy-kafka-cho-b2). | Key là `orderId`. Giữ nguyên event ID và JSON khi gửi lại. |
| Inventory | Đã triển khai (B3) trên `localhost:8084` theo [hướng dẫn Inventory](../backend/README.md#inventory-service-b3). Đọc `order-events`, giữ tồn nguyên tử và phát `Reserved`/`Failed`. | Xuất Reserved hoặc Failed sau khi quyết định giữ hàng. |
| Kitchen | Dùng fixture Reserved (hoặc chạy cùng Inventory B3) để viết tạo phiếu và xử lý hoàn tất. | Chỉ tạo phiếu từ Reserved. Xuất Done khi hoàn tất. |
| Order status | Dùng fixture Reserved, Failed và Done (hoặc output `status-events` từ B3) để test cập nhật đơn local. | Chỉ xử lý sự kiện của đúng `sourceSiteId`. |

| Event | Topic | Mẫu JSON |
|---|---|---|
| OrderCreated | `order-events` | [order-created.v1.json](contracts/events/order-created.v1.json) |
| Reserved | `status-events` | [reserved.v1.json](contracts/events/reserved.v1.json) |
| Failed | `status-events` | [failed.v1.json](contracts/events/failed.v1.json) |
| Done | `status-events` | [done.v1.json](contracts/events/done.v1.json) |

B1 ghi OrderCreated vào PostgreSQL, worker B2 gửi lên `order-events`, và Inventory Service B3 (dùng chung module `event-contracts`) xử lý giữ hàng nguyên tử rồi phát `Reserved` hoặc `Failed` lên `status-events` (xem [bằng chứng B3](evidence/B3-2026-11-11/README.md)). Fixture `Done` và chuỗi `Reserved`/`Failed` giữ làm contract cho B4 (Kitchen và đồng bộ ngược về Order Service).
Dùng chuỗi OrderCreated → Reserved → Done cho ca thành công, và OrderCreated → Failed cho ca từ chối.
Khi mô phỏng thêm đơn, thay `orderId` và tạo `eventId` riêng cho từng sự kiện.

Mỗi service sở hữu DB và migration của mình. Trao đổi trước khi đổi tên trường, topic, key hoặc trạng thái trong contract.
Khi phần xử lý đã chạy với fixture, dùng cùng dữ liệu để kiểm tra lại qua Kafka khi ghép.
