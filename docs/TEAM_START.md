# Bắt đầu làm việc song song

Mỗi người chạy stack local trên máy mình theo [hướng dẫn backend](../backend/README.md).
Điểm ghép giữa các phần là [OpenAPI của Order Service](contracts/order-api.yaml) và các event JSON v1 bên dưới.

| Phần | Có thể bắt đầu ngay | Điểm ghép |
|---|---|---|
| Frontend POS | Dùng API menu, tạo đơn, danh sách và chi tiết trên `localhost:8081`. Mock các trạng thái Kho/Bếp để làm giao diện. | Giữ nguyên `clientRequestId` khi retry, dùng ID mới cho lần đặt mới. Đọc trạng thái từ chi tiết đơn. |
| Kafka và outbox | Chạy cụm Kafka và bật publisher của Order Service theo [hướng dẫn B2](../backend/README.md#chạy-kafka-cho-b2). | Key là `orderId`. Giữ nguyên event ID và JSON khi gửi lại. |
| Inventory | Đã triển khai (B3) trên `localhost:8084` theo [hướng dẫn Inventory](../backend/README.md#inventory-service-b3). Đọc `order-events`, giữ tồn nguyên tử và phát `Reserved`/`Failed`. | Xuất Reserved hoặc Failed sau khi quyết định giữ hàng. |
| Kitchen | Đã triển khai (B4) trên `localhost:8085` theo [hướng dẫn Kitchen](../backend/README.md#kitchen-service-b4--đồng-bộ-trạng-thái-đơn). Đọc `Reserved` từ `status-events`, tạo vé bếp `WAITING` và phát `Done` khi hoàn tất. | Chỉ tạo phiếu từ Reserved. Xuất Done khi hoàn tất. |
| Order status | Đã triển khai (B4) trong `order-service`. Đọc `status-events`, đồng bộ trạng thái đơn (`PENDING_INVENTORY -> RESERVED -> DONE/FAILED`) kèm ma trận D13 chống revert. | Chỉ xử lý sự kiện của đúng `sourceSiteId`. |
| Query & Replication | Đã triển khai (B5) qua PostgreSQL Logical Replication (`POS01`/`POS02`/`ONLINE` $\to$ `central-db`) và `order-query-service` (`localhost:8080`, xem [OpenAPI Reporting](contracts/reporting-api.yaml)). | Chỉ đọc `reporting.all_orders` / `all_order_items` qua user `query_reader`. |

| Event | Topic | Mẫu JSON |
|---|---|---|
| OrderCreated | `order-events` | [order-created.v1.json](contracts/events/order-created.v1.json) |
| Reserved | `status-events` | [reserved.v1.json](contracts/events/reserved.v1.json) |
| Failed | `status-events` | [failed.v1.json](contracts/events/failed.v1.json) |
| Done | `status-events` | [done.v1.json](contracts/events/done.v1.json) |

B1 ghi OrderCreated vào PostgreSQL, worker B2 gửi lên `order-events`, Inventory Service B3 xử lý giữ hàng nguyên tử rồi phát `Reserved`/`Failed` lên `status-events` (xem [bằng chứng B3](evidence/B3-2026-11-11/README.md)), Kitchen Service B4 xử lý vé bếp kèm đồng bộ ngược trạng thái về Order Service (xem [bằng chứng B4](evidence/B4-2026-11-11/README.md)), và B5 mở rộng đa nguồn (`POS01`/`POS02`/`ONLINE`) kèm PostgreSQL Logical Replication về `central-db` và `order-query-service` (xem [bằng chứng B5](evidence/B5-2026-11-12/README.md)). B5 đóng phạm vi backend + D11; dashboard React (`/dashboard`) và bài thử nhiều instance (D12, ví dụ ONLINE ×2) để sang mốc tiếp theo cùng các bài thử lỗi bổ sung (D03/D07/D09/D10).
Dùng chuỗi OrderCreated → Reserved → Done cho ca thành công, và OrderCreated → Failed cho ca từ chối.
Khi mô phỏng thêm đơn, thay `orderId` và tạo `eventId` riêng cho từng sự kiện.

Mỗi service sở hữu DB và migration của mình. Trao đổi trước khi đổi tên trường, topic, key hoặc trạng thái trong contract.
Khi phần xử lý đã chạy với fixture, dùng cùng dữ liệu để kiểm tra lại qua Kafka khi ghép.
