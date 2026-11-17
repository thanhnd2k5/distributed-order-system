# Frontend React — Hệ thống đơn hàng phân tán (POS / Online / Kho / Bếp / Dashboard)

Ứng dụng React + Vite + Tailwind CSS + TanStack Query + Zustand (Base B) kết nối trực tiếp tới các backend service của hệ thống:

- `POS01` (`VITE_POS01_API_URL`, mặc định `http://localhost:8081`)
- `POS02` (`VITE_POS02_API_URL`, mặc định `http://localhost:8082`)
- `ONLINE` (`VITE_ONLINE_API_URL`, mặc định `http://localhost:8083`)
- `Inventory Service` (`VITE_INVENTORY_API_URL`, mặc định `http://localhost:8084`)
- `Kitchen Service` (`VITE_KITCHEN_API_URL`, mặc định `http://localhost:8085`)
- `Order Query Service` (`VITE_QUERY_API_URL`, mặc định `http://localhost:8080`)

> **Lưu ý CORS:** Backend mặc định cho phép origin `http://localhost:5173` và không bật `allowCredentials`. Các client trong `src/api/serviceClients.js` được cấu hình `withCredentials: false`.

## Các route chính (không yêu cầu đăng nhập)

- `/dashboard`: Dashboard tổng hợp từ `Central Order Replica` (`order-query-service`), kèm banner `replicaNote` và trạng thái 3 PostgreSQL subscriptions (`pos01_sub`, `pos02_sub`, `online_sub`).
- `/pos/POS01`, `/pos/POS02`: Giao diện tạo đơn tại quầy POS, danh sách đơn theo site, chi tiết món và công cụ kiểm chứng retry / `409 Conflict` theo `clientRequestId` (D13).
- `/online`: Giao diện đặt hàng Online và tra cứu tiến trình xử lý theo `orderId` UUID.
- `/inventory`: Theo dõi tồn kho nguyên liệu (`ingredients`), công thức (`recipes`) và tra cứu quyết định giữ nguyên liệu (`reservations/{orderId}`).
- `/kitchen`: Quản lý phiếu bếp `WAITING` / `DONE` và hoàn tất phiếu phát event `Done`.

## Chạy local

```powershell
Set-Location react-starter-kit-zq
Copy-Item .env.example .env   # Nếu chưa có .env
npm install
npm run dev                   # Mặc định mở http://localhost:5173
```

Kiểm tra lint và build production:

```powershell
npm run lint
npm run build
```
