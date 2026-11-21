# Thiết kế chi tiết – Sự kiện Kafka, schema DB và API

Bản sửa 3 · 21/11/2026 · @Naul · theo đặc tả nghiệp vụ v1.5

Tài liệu này biến đặc tả nghiệp vụ v1.5 (bản gọn) thành ba thứ để code: danh mục sự kiện Kafka, schema DB của từng dịch vụ và API. Mọi tên sự kiện, tên bảng, tên cột và đường dẫn API ở đây là đề xuất; nền B5 đã có tên nào thì giữ tên của B5 và sửa lại bảng tương ứng.

## Thay đổi ở bản sửa 3

Bản sửa 3 đưa các thay đổi của đặc tả v1.5 vào thiết kế. Thay đổi đáng kể nhất là quy tắc về lúc bộ gửi bắt đầu thử gửi, vốn thiếu ở cả đặc tả v1.4 lẫn bản sửa 2.

| Mục | Thay đổi | Lý do |
|---|---|---|
| 5, 8 | Thêm quy tắc bắt đầu thử gửi: với gói chưa có cờ, bộ gửi chỉ ghi cờ và gửi khi phép thử kết nối tới Kafka vừa thành công (điểm 11 ở mục 8) | BR9 của đặc tả v1.5. Thiếu quy tắc này thì đơn tạo lúc mất mạng vẫn bị ghi cờ và kịch bản 10 không đạt |
| 3 | Lệnh hoàn hoặc báo Đã dùng tới bản ghi Thất bại cũng ghi `alerts` loại `LENH_KHONG_KHOP` | Bảng 3.3 của đặc tả v1.5 |
| 6 | Thêm `GET /monitor` ở Kho và Bếp cho cảnh báo consumer chậm; `GET /stock` phục vụ cả danh sách nguyên liệu chờ kiểm kho | Bốn dòng mới ở Luồng D của đặc tả v1.5 |
| 7 | Xếp 8 kịch bản mới (48–55) vào các bước, thêm bước 7 cho báo cáo; đợt 1 thành 40 kịch bản | Mục 12 của đặc tả v1.5 |
| Đầu file | Bỏ "v1.4" khỏi tiêu đề và tên file; dòng đầu ghi phiên bản đặc tả mà thiết kế này theo | Số trong tiêu đề cũ là phiên bản của đặc tả, không phải của tài liệu này |

## Thay đổi ở bản sửa 2

Bản sửa 2 sửa bốn lỗi tìm thấy khi đối chiếu bản đầu với đặc tả và làm rõ bảy chỗ còn bỏ ngỏ. Bốn dòng đầu là lỗi; các dòng sau là làm rõ.

| Mục | Thay đổi | Lý do |
|---|---|---|
| 3, 4, 6 | Thêm bảng `alerts` ở Kho và Bếp, thêm `GET /alerts` | Kịch bản 29 đòi đánh dấu đơn "cần kiểm tra" nhưng schema chưa có chỗ lưu. Kịch bản 22, 27, 34 nhờ đó kiểm được bằng truy vấn |
| 2, 5 | Thêm thao tác (4) cho `CancelPending`, có điều kiện về hạng và cờ; thêm thao tác (5) cho `CancelRejected` | Tin của Kho và tin của Bếp tới chi nhánh không theo thứ tự. Thiếu điều kiện thì đơn đã Thất bại bị bật lại cờ hủy (kịch bản 8) |
| 5, 6 | Thao tác hủy (2) khóa dòng `orders` trước, rồi mới kiểm tra và xóa gói | Bấm hủy hai lần sát nhau làm một đơn chưa từng gửi vẫn phát gói hủy (kịch bản 10) |
| 7 | `IngredientCheckChanged`, `flagged_ingredients` và `menu_version` chuyển lên đợt 1 | Đặc tả chỉ dời vế "món sang Hết" của kịch bản 6 sang đợt 2; vế cảnh báo trên phiếu thuộc đợt 1 |
| 3 | Khóa dòng `dish_status` trước khi tính trạng thái món; ghi rõ thứ tự khóa của Kho | `menu_version` chỉ xếp hàng việc cấp `seq`, không xếp hàng việc tính |
| 3, 6 | Thêm thủ tục tăng `epoch` và API tương ứng | Kịch bản 20 cần thao tác này; thiếu thì snapshot vẫn mang đời cũ |
| 6 | `clear-check` nhận tồn thực tế đếm được, không phải tồn khả dụng | Người kiểm kho đếm được cả phần đang giữ |
| 4 | `cancel_reason` thành `cancel_reasons`, lưu lý do theo từng chi nhánh | Yêu cầu sai chi nhánh tới trước không được ghi đè lý do của chi nhánh gốc |
| 2, 3 | Kho lấy `origin_branch` từ tên topic; ghi rõ hai lớp chống trùng và cách Kho trả lại kết quả cũ | Bản đầu chỉ nói cho phía Bếp |
| 2 | Tin vào dlq được gửi thẳng, không qua outbox; Bếp bỏ qua `DishStatusChanged` và `Heartbeat` | Tin hỏng có thể không phải JSON; hai loại tin kia là loại đã biết, không được vào dlq |
| 7, 8 | Ghi rõ mức kiểm thử của từng bước; thêm bốn điểm cần chốt (7–10); bỏ dòng "đồng ý" đứng riêng ở cuối mục 8 | Dòng đó không nói áp cho điểm nào; trạng thái chốt nay ghi ở đầu mục 8 |

Các câu lệnh ở mục 3, 4 và 5 đã được chạy thử trên PostgreSQL; kết quả ghi ở mục 9.

## 1. Phạm vi và quy ước

Đặc tả v1.5 là nguồn quyết định về nghiệp vụ. Khi tài liệu này lệch với đặc tả thì đặc tả đúng, và phải sửa tài liệu này.

- **Mã đơn (`order_id`)**: UUID do chi nhánh sinh lúc tạo đơn, dùng làm khóa phân vùng Kafka và khóa chính ở cả ba nơi (chi nhánh, Kho, Bếp).
- **Mã sự kiện (`event_id`)**: UUID do bên phát sinh ngay khi ghi outbox. Gửi lại cùng một dòng outbox thì giữ nguyên `event_id`; tạo một tin mới, kể cả tin trả lại kết quả cũ, thì sinh `event_id` mới.
- **Mã chi nhánh (`branch_id`)**: `POS01` … `POSN` và `ONLINE`. Tên topic theo chi nhánh có dạng `order-events.POS01`.
- **Thời gian**: UTC, định dạng ISO 8601. `created_at` của đơn do chi nhánh ghi và không đổi qua các tin; Kho dùng nó để kiểm tra hạn X giờ (BR11).
- **Trạng thái trong DB**: mã không dấu viết hoa (`CHO_KHO`, `DANG_GIU`…), nối tiếp cách đặt `CHUA_CO` và `THAT_BAI` trong mục 13.4 của đặc tả.
- **Lý do**: mã cố định theo danh mục ở mục 10 của đặc tả, không nhập tự do.
- **Số lượng nguyên liệu**: số thập phân kèm đơn vị cố định theo từng nguyên liệu, kiểu `NUMERIC(12,3)`.

## 2. Danh mục sự kiện Kafka

Hệ thống có 13 loại sự kiện trên 6 nhóm topic, chưa tính dlq. Mọi sự kiện dùng khuôn tin chung ở mục 9 của đặc tả; bảng dưới chỉ nêu phần `payload`.

```json
{ "event_id": "uuid", "event_type": "CancelRequested", "schema_version": 1,
  "order_id": "uuid", "branch_id": "POS01", "occurred_at": "2026-11-20T09:15:00Z",
  "causation_id": "event_id của tin gây ra tin này", "payload": { } }
```

| event_type | Topic | Gửi → nhận | payload | Phát khi |
|---|---|---|---|---|
| `OrderCreated` | `order-events.<chi_nhanh>` | Chi nhánh → Kho | `created_at`, `channel` (POS, ONLINE), `items[{dish_id, quantity}]` | Thu ngân hoặc khách gửi đơn hợp lệ |
| `CancelRequested` | `cancel-events.<chi_nhanh>` | Chi nhánh → Bếp | `reason_code` | Hủy một đơn có gói tạo đơn đã thử gửi (BR9) |
| `OrderReserved` | `kitchen-orders` | Kho → Bếp | `origin_branch`, `created_at`, `items[]`, `reserved[{ingredient_id, quantity}]` | Kho giữ đủ nguyên liệu |
| `OrderReserved` | `status-events.<chi_nhanh>` | Kho → chi nhánh gốc | rỗng | Cùng giao dịch với dòng trên; chi nhánh sang Đã giữ |
| `OrderFailed` | `kitchen-orders` và `status-events.<chi_nhanh>` | Kho → Bếp, chi nhánh gốc | `origin_branch`, `reason_code` | Kho từ chối đơn (mục 10 của đặc tả) |
| `StockConsumed` | `stock-commands` | Bếp → Kho | rỗng | Nhân viên bấm Bắt đầu nấu |
| `ReleaseRequested` | `stock-commands` | Bếp → Kho | `cancelled_by` (CUSTOMER, KITCHEN), `reason_code`, `spoiled_ingredients[]` | Bếp chốt hủy hoặc từ chối phiếu (lệnh hoàn) |
| `CancelPending` | `status-events.<chi_nhanh>` | Bếp → chi nhánh gửi yêu cầu | rỗng | Bếp tạo hoặc đã có Hủy chờ đơn: "bếp sẽ không nấu" |
| `CancelRejected` | `status-events.<chi_nhanh>` | Bếp → chi nhánh gốc | `kitchen_status` (DANG_NAU, HOAN_TAT) | Yêu cầu hủy tới khi phiếu đã Đang nấu hoặc Hoàn tất |
| `OrderCancelled` | `status-events.<chi_nhanh>` | Bếp → chi nhánh gốc | `cancelled_by`, `reason_code` | Bếp chốt hủy hoặc từ chối phiếu |
| `OrderCompleted` | `status-events.<chi_nhanh>` | Bếp → chi nhánh gốc | rỗng | Nhân viên bấm Hoàn tất |
| `DishStatusChanged` | `menu-events` | Kho → mọi chi nhánh | `dish_id`, `status` (CON, HET, AN), `epoch`, `seq` | Một món đổi giữa Còn, Hết, Ẩn (BR10) |
| `IngredientCheckChanged` | `menu-events` | Kho → Bếp | `ingredient_id`, `needs_check`, `epoch`, `seq` | Đánh dấu hoặc gỡ đánh dấu cần kiểm kho |
| `Heartbeat` | `menu-events` | Kho → mọi chi nhánh | `epoch`, `seq` hiện tại | Theo chu kỳ 60 giây |

Ghi chú cho bảng:

- `OrderReserved` và `OrderFailed` đi hai topic là hai dòng outbox với hai `event_id` khác nhau, ghi trong cùng một giao dịch của Kho.
- "Trả lại kết quả cũ" nghĩa là phát lại đúng loại sự kiện ứng với trạng thái đang lưu, với `event_id` mới. Bếp ở Hủy chờ đơn phát `CancelPending`, ở Đã hủy phát `OrderCancelled`, ở Thất bại phát `OrderFailed` với `reason_code` lấy từ `close_reason`. Cách Kho trả lại kết quả cũ nằm ở mục 3. Bên nhận đã chịu được tin lặp nhờ quy tắc hạng (mục 3.1 của đặc tả) và BR12.
- Yêu cầu hủy từ chi nhánh khác chi nhánh gốc không nhận tin trả lời nào; Bếp ghi một dòng `alerts` loại `HUY_SAI_CHI_NHANH` (mục 13.2 của đặc tả).
- Tin trên `menu-events` không thuộc đơn nào: `order_id` để `null`, khóa phân vùng là `dish_id` hoặc `ingredient_id`, riêng `Heartbeat` dùng khóa cố định `heartbeat`.
- Mỗi bên chỉ xử lý loại tin dành cho mình trên `menu-events`: Bếp xử lý `IngredientCheckChanged`, chi nhánh xử lý `DishStatusChanged` và `Heartbeat`. Các loại còn lại trong danh mục này là loại đã biết: bên nhận bỏ qua, không ghi `processed_events` và không chuyển vào dlq.
- Tin vào dlq giữ nguyên nội dung gốc và thêm header: topic gốc, partition, offset, tên consumer, lỗi. Consumer gửi thẳng tin này vào dlq, không qua outbox, vì nội dung có thể không phải JSON. Thứ tự là: gửi vào dlq và chờ Kafka xác nhận, ghi một dòng `alerts` loại `CAN_KIEM_TRA`, rồi mới commit offset.

Chi nhánh áp tin trạng thái như sau:

| Sự kiện nhận | Đơn tại chi nhánh | Cờ "Đang yêu cầu hủy" |
|---|---|---|
| `OrderReserved` | Đã giữ (hạng 1) | Không đổi |
| `OrderFailed` | Thất bại (hạng 2) | Tắt |
| `OrderCancelled` | Đã hủy (hạng 2) | Tắt |
| `OrderCompleted` | Hoàn tất (hạng 2) | Tắt |
| `CancelPending` | Không đổi | Thêm dòng "bếp sẽ không nấu", chỉ khi đơn chưa ở trạng thái cuối và cờ đang là `DANG_YEU_CAU`; ngược lại bỏ qua |
| `CancelRejected` | Không đổi | Tắt, hiện "Không hủy được: bếp đã bắt đầu nấu", kể cả khi đơn đã ở trạng thái cuối |

Điều kiện ở dòng `CancelPending` là bắt buộc. `OrderFailed` do Kho phát và `CancelPending` do Bếp phát đi qua hai bộ đẩy outbox khác nhau, nên `CancelPending` có thể tới sau khi đơn đã Thất bại. `CancelRejected` thì không cần điều kiện về hạng: Bếp chỉ phát nó khi phiếu đã Đang nấu hoặc Hoàn tất, và trong trường hợp Hoàn tất nó luôn tới sau `OrderCompleted`.

## 3. Schema Inventory DB

Kho cần chín bảng: tồn kho, công thức, bản ghi đơn, lượng giữ theo dòng, trạng thái món, bộ đếm phiên bản, cảnh báo, chống trùng và outbox.

```sql
CREATE TABLE stock (
  ingredient_id TEXT PRIMARY KEY,
  unit          TEXT NOT NULL,
  available     NUMERIC(12,3) NOT NULL CHECK (available >= 0),           -- tồn khả dụng
  reserved      NUMERIC(12,3) NOT NULL DEFAULT 0 CHECK (reserved >= 0),  -- đang giữ
  consumed      NUMERIC(12,3) NOT NULL DEFAULT 0,   -- đã dùng, cộng dồn
  wasted        NUMERIC(12,3) NOT NULL DEFAULT 0,   -- hao hụt, cộng dồn
  needs_check   BOOLEAN NOT NULL DEFAULT FALSE      -- cần kiểm kho (BR6)
);

CREATE TABLE recipes (
  dish_id       TEXT NOT NULL,
  ingredient_id TEXT NOT NULL REFERENCES stock,
  quantity      NUMERIC(12,3) NOT NULL CHECK (quantity > 0),
  PRIMARY KEY (dish_id, ingredient_id)
);

CREATE TABLE inventory_orders (                     -- BR12: một dòng mỗi mã đơn
  order_id         UUID PRIMARY KEY,
  origin_branch    TEXT NOT NULL,                   -- lấy từ tên topic order-events.<chi_nhanh>
  status           TEXT NOT NULL
    CHECK (status IN ('DANG_GIU','THAT_BAI','DA_TRA','DA_DUNG')),
  reason           TEXT,                            -- lý do Thất bại hoặc lý do hoàn
  order_created_at TIMESTAMPTZ,                     -- NULL nếu đơn sai dữ liệu
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE inventory_reservations (               -- lượng đã ghi lúc giữ
  order_id      UUID NOT NULL REFERENCES inventory_orders,
  ingredient_id TEXT NOT NULL REFERENCES stock,
  quantity      NUMERIC(12,3) NOT NULL CHECK (quantity > 0),
  outcome       TEXT CHECK (outcome IN ('DA_HOAN','DA_DUNG','HAO_HUT')),  -- NULL: còn giữ
  PRIMARY KEY (order_id, ingredient_id)
);

CREATE TABLE dish_status (
  dish_id TEXT PRIMARY KEY,
  status  TEXT NOT NULL CHECK (status IN ('CON','HET','AN')),
  epoch   INT NOT NULL,
  seq     BIGINT NOT NULL
);

CREATE TABLE menu_version (                         -- đúng một dòng
  id    BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),
  epoch INT NOT NULL,
  seq   BIGINT NOT NULL
);

CREATE TABLE alerts (                               -- Luồng D: việc cần người xem
  id               BIGSERIAL PRIMARY KEY,
  kind             TEXT NOT NULL CHECK (kind IN
    ('CAN_KIEM_TRA','LENH_KHONG_KHOP','HUY_SAI_CHI_NHANH')),
  order_id         UUID,                            -- NULL nếu không đọc được mã đơn
  event_id         UUID,                            -- NULL nếu tin không đọc được
  source_topic     TEXT NOT NULL,                   -- tin đã gây ra cảnh báo
  source_partition INT NOT NULL,
  source_offset    BIGINT NOT NULL,
  detail           JSONB NOT NULL DEFAULT '{}',
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (kind, source_topic, source_partition, source_offset)
);

CREATE TABLE processed_events (
  event_id     UUID PRIMARY KEY,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE outbox_events (
  id         BIGSERIAL PRIMARY KEY,
  event_id   UUID NOT NULL UNIQUE,
  topic      TEXT NOT NULL,
  msg_key    TEXT NOT NULL,
  payload    JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  sent_at    TIMESTAMPTZ                            -- NULL: chưa được Kafka xác nhận
);
```

Các quy tắc đi kèm schema:

- **Chi nhánh gốc**: `origin_branch` lấy từ tên topic mà tin `OrderCreated` tới, không lấy từ `branch_id` trong tin (mục 9 của đặc tả).
- **Hai lớp chống trùng**: cùng `event_id` thì `processed_events` chặn và Kho không phát gì (mục 13.3 của đặc tả). Khác `event_id` nhưng trùng `order_id` thì `INSERT INTO inventory_orders ... ON CONFLICT (order_id) DO NOTHING` không chèn được dòng nào, và Kho trả lại kết quả cũ mà không xét lại đơn (BR12).
- **Kho trả lại kết quả cũ**: chỉ phát về `status-events.<chi_nhánh gốc>`, không phát lại sang `kitchen-orders` vì Bếp đã nhận tin gốc qua outbox. Bản ghi `THAT_BAI` phát `OrderFailed` với lý do đã lưu; bản ghi `DANG_GIU`, `DA_TRA`, `DA_DUNG` phát `OrderReserved`.
- **Giữ**: theo đúng đoạn SQL ở mục 13.4 của đặc tả. Câu `UPDATE stock` trừ `available`, cộng `reserved`, có điều kiện `available >= :q AND NOT needs_check`; mỗi nguyên liệu giữ được ghi một dòng `inventory_reservations`. Nhu cầu của cả đơn được cộng theo từng nguyên liệu trước, nên mỗi nguyên liệu chỉ có một câu `UPDATE`.
- **Hoàn, đã dùng, hao hụt**: chỉ chạy khi `UPDATE inventory_orders SET status = ... WHERE order_id = :id AND status = 'DANG_GIU'` đổi đúng một dòng. Không đổi dòng nào thì là tin lặp hoặc lệnh không khớp: không đụng vào `stock`.
- **Lệnh không khớp**: lệnh hoàn hoặc báo Đã dùng cho đơn không có bản ghi, hoặc cho đơn có bản ghi `THAT_BAI`, thì ghi một dòng `alerts` loại `LENH_KHONG_KHOP`, kèm dòng log có `order_id` và `event_id`. Lệnh tới bản ghi `DA_TRA` hoặc `DA_DUNG` là tin lặp: bỏ qua, không ghi cảnh báo.
- **Bảng `alerts`**: Kho dùng hai loại `CAN_KIEM_TRA` (tin của đơn đã vào dlq) và `LENH_KHONG_KHOP`. Mọi lần chèn dùng `ON CONFLICT DO NOTHING` trên khóa duy nhất, nên Kafka giao lại tin không tạo dòng thứ hai. Với tin không đọc được, `order_id` lấy từ khóa Kafka của tin nếu khóa là UUID.
- **Thứ tự khóa**: mọi giao dịch của Kho khóa theo đúng thứ tự: dòng `inventory_orders` của đơn, các dòng `stock` theo `ingredient_id` tăng dần, các dòng `dish_status` theo `dish_id` tăng dần, cuối cùng là dòng `menu_version`. Không giao dịch nào khóa ngược thứ tự này.
- **Tính trạng thái món**: sau khi đổi `stock`, giao dịch khóa các dòng `dish_status` của những món dùng nguyên liệu vừa đổi bằng `SELECT ... FOR UPDATE`, rồi mới đọc tồn của mọi nguyên liệu trong công thức và tính lại theo mục 3.4 của đặc tả. Đọc tồn trước khi khóa là sai: hai giao dịch đổi hai nguyên liệu khác nhau của cùng một món sẽ không thấy thay đổi của nhau, và món có thể nằm ở Còn trong khi đáng lẽ là Hết.
- **Phiên bản món**: mỗi lần một món đổi trạng thái, hoặc một nguyên liệu đổi cờ cần kiểm kho, giao dịch chạy `UPDATE menu_version SET seq = seq + 1 RETURNING epoch, seq`. Khóa dòng này làm thứ tự `seq` trùng với thứ tự commit; `SEQUENCE` của PostgreSQL không bảo đảm điều đó.
- **Tăng `epoch`** (sau khi khôi phục Kho từ bản sao lưu): một giao dịch khóa mọi dòng `dish_status`, chạy `UPDATE menu_version SET epoch = epoch + 1, seq = seq + 1`, rồi mới đọc tồn, tính lại và ghi lại mọi dòng `dish_status` với `epoch` và `seq` mới, kể cả món không đổi trạng thái. Giao dịch phát `IngredientCheckChanged` cho mọi nguyên liệu để Bếp có cờ theo đời mới, và không phát `DishStatusChanged`: chi nhánh thấy `epoch` mới ở `Heartbeat` kế tiếp rồi tự lấy snapshot.
- **Bất biến** (chạy sau mỗi bài test): `stock.reserved` của một nguyên liệu bằng tổng `quantity` của các dòng `inventory_reservations` có `outcome IS NULL`; và mọi dòng có `outcome IS NULL` đều thuộc một đơn đang ở `DANG_GIU`.
- **Bộ đẩy outbox với 2 instance**: đọc theo `id` tăng dần với `FOR UPDATE SKIP LOCKED`. Hai instance có thể đẩy lệch thứ tự nhau; điều này chấp nhận được vì mọi bên nhận đều xử lý theo bảng trạng thái, không theo thứ tự tin. Ở chi nhánh, điều kiện của thao tác (4) ở mục 5 là thứ bảo đảm điều đó.

## 4. Schema Kitchen DB

Bếp giữ mọi thứ về một đơn trong đúng một dòng của `kitchen_orders` (BR8); phiếu, Hủy chờ đơn và kết quả Thất bại chỉ khác nhau ở cột `status`.

```sql
CREATE TABLE kitchen_orders (
  order_id         UUID PRIMARY KEY,
  status           TEXT NOT NULL CHECK (status IN
    ('CHUA_CO','HUY_CHO_DON','CHO_NAU','DANG_NAU','HOAN_TAT','DA_HUY','THAT_BAI')),
  origin_branch    TEXT,                          -- chi nhánh gốc; chỉ ghi từ tin của Kho
  cancel_branches  TEXT[] NOT NULL DEFAULT '{}',  -- mọi chi nhánh đã yêu cầu hủy
  cancel_reasons   JSONB NOT NULL DEFAULT '{}',   -- lý do theo chi nhánh: {"POS01": "KHACH_DOI_Y"}
  cancelled_by     TEXT CHECK (cancelled_by IN ('CUSTOMER','KITCHEN')),
  close_reason     TEXT,                          -- lý do Đã hủy hoặc Thất bại
  items            JSONB,                         -- món của đơn, chép từ OrderReserved
  reserved         JSONB,                         -- nguyên liệu đã giữ, chép từ OrderReserved
  spoiled          JSONB,                         -- nguyên liệu bị báo hỏng khi từ chối
  order_created_at TIMESTAMPTZ,
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE flagged_ingredients (                -- từ IngredientCheckChanged
  ingredient_id TEXT PRIMARY KEY,
  needs_check   BOOLEAN NOT NULL,
  epoch         INT NOT NULL,
  seq           BIGINT NOT NULL
);

-- alerts, processed_events và outbox_events: giống Inventory DB
```

Các quy tắc đi kèm schema:

- **Một cửa vào cho mọi thay đổi**: tin từ `kitchen-orders`, tin từ `cancel-events` và ba nút của nhân viên đều chạy cùng một đoạn: chèn dòng `CHUA_CO` nếu chưa có, `SELECT ... FOR UPDATE`, áp bảng 3.2, ghi outbox, commit (mục 13.4 của đặc tả).
- **Chi nhánh gửi yêu cầu hủy** lấy từ tên topic, không lấy từ `branch_id` trong tin, rồi thêm vào `cancel_branches` nếu chưa có. Lý do của chi nhánh đó ghi vào `cancel_reasons`; yêu cầu lặp từ cùng chi nhánh không ghi đè lý do đã có.
- **Khi `OrderReserved` tới dòng `HUY_CHO_DON`**: `origin_branch = ANY(cancel_branches)` thì sang `DA_HUY`, `close_reason` lấy lý do của chi nhánh gốc trong `cancel_reasons`, và phát `ReleaseRequested` cùng `OrderCancelled`. Ngược lại xóa `cancel_branches` và `cancel_reasons`, sang `CHO_NAU` và ghi một dòng `alerts` loại `HUY_SAI_CHI_NHANH` có danh sách chi nhánh bị bác.
- **Yêu cầu hủy sai chi nhánh** tới khi dòng đã có `origin_branch`: giữ nguyên trạng thái, không trả lời, ghi một dòng `alerts` loại `HUY_SAI_CHI_NHANH`.
- **Bảng `alerts`**: Bếp dùng hai loại `CAN_KIEM_TRA` và `HUY_SAI_CHI_NHANH`, theo cùng cách chèn như ở Kho.
- **Cảnh báo đỏ trên phiếu** là phép nối lúc hiển thị giữa `reserved` của các phiếu `CHO_NAU` và `flagged_ingredients` có `needs_check = TRUE`. Dòng `flagged_ingredients` chỉ bị ghi đè khi `(epoch, seq)` của tin lớn hơn.
- **Lệnh hoàn lưu bền trước khi gửi**: `ReleaseRequested` nằm trong `outbox_events` cùng giao dịch với việc đổi `status`, nên Kho dừng thì lệnh vẫn còn đó (kịch bản 4).

## 5. Schema DB chi nhánh (POS và Online)

Local Order DB và Online Order DB dùng chung schema; điểm mới so với luồng nền là cột hạng, cờ hủy và cờ "đã thử gửi" trên hàng chờ gửi.

```sql
CREATE TABLE orders (
  order_id      UUID PRIMARY KEY,
  status        TEXT NOT NULL CHECK (status IN
    ('CHO_KHO','DA_GIU','HOAN_TAT','DA_HUY','THAT_BAI')),
  status_rank   SMALLINT NOT NULL DEFAULT 0,      -- 0, 1, 2 theo mục 3.1
  close_reason  TEXT,
  cancel_flag   TEXT NOT NULL DEFAULT 'KHONG'
    CHECK (cancel_flag IN ('KHONG','DANG_YEU_CAU','BEP_KHONG_NAU')),
  cancel_requested_at TIMESTAMPTZ,                -- để hiện thời gian đã chờ
  cancel_message      TEXT,                       -- "Không hủy được: bếp đã bắt đầu nấu"
  items         JSONB NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL
);

CREATE TABLE outbox_events (                      -- hàng chờ gửi
  id        BIGSERIAL PRIMARY KEY,
  event_id  UUID NOT NULL UNIQUE,
  order_id  UUID NOT NULL REFERENCES orders,
  kind      TEXT NOT NULL CHECK (kind IN ('TAO_DON','HUY')),
  topic     TEXT NOT NULL,
  payload   JSONB NOT NULL,
  attempted BOOLEAN NOT NULL DEFAULT FALSE,       -- cờ "đã thử gửi" (BR9)
  sent_at   TIMESTAMPTZ                           -- NULL: chưa được Kafka xác nhận
);

CREATE TABLE menu_cache (                         -- đợt 2, Luồng C
  dish_id TEXT PRIMARY KEY,
  status  TEXT NOT NULL CHECK (status IN ('CON','HET','AN')),
  epoch   INT NOT NULL,
  seq     BIGINT NOT NULL
);

CREATE TABLE menu_cache_meta (                    -- đúng một dòng
  id            BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),
  epoch         INT NOT NULL,
  last_heard_at TIMESTAMPTZ                       -- tin hoặc heartbeat gần nhất
);

-- processed_events: giống Inventory DB
```

Năm thao tác ghi có điều kiện quyết định tính đúng của phía chi nhánh:

```sql
-- (1) Bộ gửi ghi cờ TRƯỚC khi gọi Kafka, commit rồi mới gửi.
--     Với gói chưa có cờ, chỉ chạy khi phép thử kết nối tới Kafka vừa thành công.
--     Không đổi dòng nào: gói đã bị hủy cục bộ, bỏ qua.
UPDATE outbox_events SET attempted = TRUE
 WHERE id = :id AND sent_at IS NULL RETURNING id;

-- (2) Hủy: một giao dịch, khóa dòng đơn trước rồi mới quyết định.
SELECT status_rank, cancel_flag FROM orders WHERE order_id = :o FOR UPDATE;
--   Không có dòng nào:       trả 404, dừng.
--   status_rank = 2:         trả 409, dừng.
--   cancel_flag <> 'KHONG':  trả 202, dừng. Không chèn gói HUY thứ hai.
DELETE FROM outbox_events
 WHERE order_id = :o AND kind = 'TAO_DON' AND attempted = FALSE RETURNING id;
--   1 dòng: hủy cục bộ.
--     UPDATE orders SET status = 'DA_HUY', status_rank = 2, close_reason = :r
--      WHERE order_id = :o;
--   0 dòng: đi qua Bếp.
--     INSERT INTO outbox_events (event_id, order_id, kind, topic, payload)
--     VALUES (:event_id, :o, 'HUY', :topic, :payload);
--     UPDATE orders SET cancel_flag = 'DANG_YEU_CAU', cancel_requested_at = now(),
--            cancel_message = NULL
--      WHERE order_id = :o;

-- (3) Áp tin trạng thái: chỉ tiến, không lùi (BR3).
UPDATE orders SET status = :s, status_rank = :rank,
       close_reason = COALESCE(:reason, close_reason),
       cancel_flag = CASE WHEN :rank = 2 THEN 'KHONG' ELSE cancel_flag END
 WHERE order_id = :o AND status_rank < :rank;

-- (4) CancelPending: chỉ khi đơn chưa ở trạng thái cuối và đang chờ Bếp trả lời.
UPDATE orders SET cancel_flag = 'BEP_KHONG_NAU'
 WHERE order_id = :o AND status_rank < 2 AND cancel_flag = 'DANG_YEU_CAU';

-- (5) CancelRejected: tắt cờ và ghi thông báo, ở mọi hạng.
UPDATE orders SET cancel_flag = 'KHONG', cancel_message = :msg
 WHERE order_id = :o;
```

- (1) và (2) cùng khóa một dòng `outbox_events`, nên đúng một bên thắng (kịch bản 37). Nếu tiến trình chết giữa lúc ghi cờ và lúc gửi, gói vẫn mang cờ và được gửi lại; đơn đó không còn hủy cục bộ được, đúng theo BR9.
- **Bắt đầu thử gửi**: trước khi chạy (1) cho một gói chưa có cờ, bộ gửi thử kết nối tới Kafka, ví dụ lấy metadata của cluster với thời gian chờ ngắn. Thử thất bại thì không ghi cờ và không gửi, nên đơn tạo lúc mất mạng vẫn hủy cục bộ được (kịch bản 10). Phép thử sai theo hướng nào cũng an toàn. Báo nhầm "mất kết nối" chỉ làm gói đi chậm hơn. Báo nhầm "có kết nối" thì gói mang cờ, lần gửi lỗi và được thử lại như kịch bản 12. Gói đã có cờ thì được gửi lại mà không cần thử kết nối trước. Kết quả của phép thử gần nhất là "trạng thái kết nối" mà `GET /monitor` của chi nhánh trả về.
- (2) khóa dòng `orders` trước khi xóa gói, nên hai lần bấm hủy sát nhau chạy lần lượt: lần sau thấy đơn đã ở trạng thái cuối hoặc đã có cờ và dừng. Thao tác (3) cũng ghi vào dòng `orders` đó, nên một tin trạng thái cuối tới giữa chừng không để lại cờ hủy treo trên đơn đã khép.
- Thứ tự khóa ở chi nhánh là dòng `orders` trước, dòng `outbox_events` sau. Thao tác (1) chỉ khóa `outbox_events`, thao tác (3), (4), (5) chỉ khóa `orders`, nên không có khóa chéo.
- Bộ gửi chỉ lấy dòng chưa gửi có `id` nhỏ nhất của mỗi `order_id`. Gói hủy vì vậy không bao giờ đi trước gói tạo đơn của cùng đơn, còn đơn khác không bị chặn (BR9).
- `CancelPending` và `CancelRejected` không đụng vào `status`.
- Ghi đè `menu_cache` chỉ khi `(epoch, seq)` của tin lớn hơn của dòng đang có; snapshot cũng hợp nhất theo quy tắc này, không xóa rồi nạp lại (BR10).

## 6. API

Mỗi dịch vụ chỉ mở những API mà một người hoặc một màn hình cần; các dịch vụ không gọi API của nhau, trừ snapshot món. Thân yêu cầu và trả lời là JSON.

| Dịch vụ | API | Việc làm | Trả về |
|---|---|---|---|
| Chi nhánh | `POST /orders` | Kiểm tra dữ liệu, sinh `order_id`, lưu đơn Chờ Kho và gói tạo đơn trong một giao dịch | 201 kèm `order_id`; 400 nếu sai dữ liệu (BR9) |
| Chi nhánh | `POST /orders/{id}/cancel` | Chạy thao tác (2) ở mục 5 với `reason_code` | 200 `DA_HUY` nếu hủy cục bộ; 202 `DANG_YEU_CAU` nếu đi qua Bếp hoặc đã có yêu cầu hủy đang chờ; 409 nếu đơn đã ở trạng thái cuối; 404 nếu không có đơn |
| Chi nhánh | `GET /orders`, `GET /orders/{id}` | Đọc đơn kèm cờ hủy và thông báo | 200 |
| Chi nhánh | `GET /menu` (đợt 2) | Đọc `menu_cache`, kèm cờ `stale` khi quá N phút không nghe Kho | 200 |
| Chi nhánh | `GET /monitor` | Luồng D: trạng thái kết nối, số gói chờ gửi, các cảnh báo | 200 |
| Bếp | `GET /tickets` | Phiếu Chờ nấu và Đang nấu, kèm cảnh báo nguyên liệu bị đánh dấu | 200 |
| Bếp | `POST /tickets/{order_id}/start` | Chờ nấu → Đang nấu, phát `StockConsumed` | 200; 409 kèm trạng thái hiện tại nếu phiếu không còn Chờ nấu |
| Bếp | `POST /tickets/{order_id}/reject` | Chờ nấu → Đã hủy với `reason_code`, `spoiled_ingredients[]`; phát `OrderCancelled` và `ReleaseRequested` | 200; 409 như trên; 400 nếu lý do Nguyên liệu hỏng mà thiếu nguyên liệu, hoặc nguyên liệu không thuộc phiếu |
| Bếp | `POST /tickets/{order_id}/complete` | Đang nấu → Hoàn tất, phát `OrderCompleted` | 200; 409 nếu phiếu không ở Đang nấu |
| Bếp | `GET /alerts` | Luồng D: đơn cần kiểm tra và yêu cầu hủy sai chi nhánh; lọc được theo `kind` | 200 |
| Kho | `GET /menu/snapshot` (đợt 2) | Trạng thái và phiên bản của mọi món, kể cả món Ẩn, kèm `epoch` hiện tại | 200 |
| Kho | `POST /ingredients/{id}/clear-check` | Gỡ đánh dấu cần kiểm kho sau khi kiểm xong; nhận tùy chọn `counted_on_hand` là tồn thực tế đếm được | 200; 409 nếu `counted_on_hand` nhỏ hơn lượng đang giữ |
| Kho | `POST /stock/receipts` | Nhập hàng: cộng `available` cho một nguyên liệu | 200 |
| Kho | `GET /stock` | Tồn kho bốn đại lượng và cờ cần kiểm kho; dùng cho kiểm thử, demo và danh sách nguyên liệu chờ kiểm kho của Luồng D | 200 |
| Kho | `GET /alerts` | Luồng D: đơn cần kiểm tra và lệnh không khớp; lọc được theo `kind` | 200 |
| Kho, Bếp | `GET /monitor` | Luồng D: tuổi của tin cũ nhất chưa đọc trên từng topic, kèm cảnh báo khi vượt ngưỡng consumer chậm | 200 |
| Kho | `POST /admin/epoch/bump` (đợt 2) | Chạy thủ tục tăng `epoch` ở mục 3; dùng sau khi khôi phục Kho và cho kịch bản 20 | 200 kèm `epoch` mới |

- **Ba nút của Bếp** chạy cùng đoạn khóa dòng với consumer (mục 4). 409 ở đây là kết quả bình thường của tranh chấp Hủy với Bắt đầu nấu (BR7), giao diện chỉ cần tải lại phiếu.
- **`POST /orders` không chờ Kho**: kết quả Đã giữ hoặc Thất bại về sau qua `status-events`. Giao diện đọc lại đơn bằng `GET /orders/{id}`.
- **`counted_on_hand` của `clear-check`** là tồn thực tế, tức tồn khả dụng cộng đang giữ (mục 3.3 của đặc tả). Kho đặt `available = counted_on_hand − reserved`. Nếu kết quả âm thì trả 409: phải từ chối các phiếu còn giữ nguyên liệu đó theo Luồng A trước. Không gửi `counted_on_hand` thì `available` giữ nguyên.
- **`clear-check` và `receipts`** đều có thể làm món đổi trạng thái, nên chạy lại bước tính trạng thái món của Luồng C trong cùng giao dịch. `clear-check` còn phát `IngredientCheckChanged`.
- **Báo cáo của Quản lý** giữ nguyên Order Query Service của luồng nền, không thêm API mới.

## 7. Thứ tự hiện thực

Đợt 1 gồm bảy bước cho Luồng A, B, D và báo cáo với 40 kịch bản, đợt 2 là Luồng C với 4 kịch bản. Bước sau dựa trên bước trước; bước 2–3 (Kho) và bước 6 (chi nhánh) có thể làm song song sau khi xong bước 1.

| Bước | Dịch vụ | Làm gì | Kịch bản phải qua |
|---|---|---|---|
| 1 | Dùng chung | Khuôn tin, `processed_events`, bộ đẩy outbox, consumer theo 5 bước của mục 13.3, đẩy tin hỏng vào dlq và ghi `alerts` | 26, 29, 31 |
| 2 | Kho | Nhận `OrderCreated`: bản ghi BR12, hạn X giờ, kiểm tra dữ liệu, giữ có điều kiện `NOT needs_check`, phát `OrderReserved` hoặc `OrderFailed` | 9, 15, 16, 24, 30, 39, 42, 44 |
| 3 | Kho | Nhận `ReleaseRequested` và `StockConsumed`: hoàn, hao hụt, đánh dấu cần kiểm kho, đã dùng; `menu_version` và phát `IngredientCheckChanged`; API gỡ đánh dấu và nhập hàng | 5, 6 (vế hao hụt và hoàn), 22, 43, 48 (vế gỡ cờ và giữ lại được), 49 |
| 4 | Bếp | Đường chính của bảng 3.2: lập phiếu, Bắt đầu nấu, Từ chối, Hoàn tất, ghi kết quả Thất bại; nhận `IngredientCheckChanged` vào `flagged_ingredients` và hiện cảnh báo đỏ trên phiếu | 4, 6 (vế cảnh báo trên phiếu), 48 (vế cảnh báo mất), 50, 52 |
| 5 | Bếp | Nhận `CancelRequested`: Hủy chờ đơn, tranh chấp với Bắt đầu nấu, kiểm tra chi nhánh gốc | 1, 2, 3, 8, 23, 25, 27, 28, 33, 34, 35, 51 |
| 6 | Chi nhánh | Áp tin trạng thái theo hạng, phép thử kết nối và cờ "đã thử gửi", hủy cục bộ, gửi yêu cầu hủy, cờ "Đang yêu cầu hủy", `GET /monitor` | 7, 10, 12, 36, 37, 53, 54 |
| 7 | Báo cáo | Nhân bản về Central Order Replica và mốc "dữ liệu tính đến", theo cách của B5 | 55 |
| Đợt 2 | Kho, chi nhánh | Luồng C: tính trạng thái món, `DishStatusChanged`, heartbeat, snapshot, `menu_cache`, thủ tục tăng `epoch` | 18, 19, 20, 21, vế "món sang Hết" của 6 và vế "món về Còn" của 48 |

Mỗi kịch bản có hai mức kiểm thử. Ở bước của mình, kịch bản được kiểm ở mức handler: bài test đưa tin thẳng vào handler của một dịch vụ và kiểm tra DB cùng các dòng outbox, không cần dịch vụ khác. Một số kịch bản chỉ chạy trọn khi đủ các dịch vụ, ví dụ 24 cần chi nhánh và Bếp, 3 cần cả ba bên; bản đầu-cuối của chúng chạy sau bước 6. Kịch bản 42 lấy bài test gọi handler của Kho từ hai luồng trên cùng một DB làm bài chính; việc hai instance mỗi bên nhận một đơn chỉ là kiểm tra bổ sung.

Xong bước 6 thì chạy bộ demo 1, 3, 4, 10, 36 trên đủ các dịch vụ. Kịch bản 28 và 42 cần hai instance của Kitchen và Inventory; kịch bản 31 cần đủ 3 broker.

## 8. Điểm cần nhóm chốt trước khi code

Mười một điểm dưới đây là lựa chọn thiết kế của tài liệu này, chưa có trong đặc tả; mỗi điểm kèm đề xuất, điểm nào được đồng ý thì đánh dấu. Điểm 1–6 có từ bản đầu, điểm 7–10 thêm ở bản sửa 2, điểm 11 thêm ở bản sửa 3.

**Trạng thái:** cả mười một điểm đã được @Naul đồng ý theo đúng đề xuất ngày 21/11/2026. Hai việc đi kèm vẫn phải làm khi có điều kiện: rà tên gọi với code B5 (điểm 1) và chạy thử cách Kafka chia partition (điểm 6). Năm điểm ở mục 11 của đặc tả vẫn phải chốt riêng.

- [x] **1. Tên gọi so với nền B5.** Đề xuất: tên của B5 thắng ở mọi chỗ B5 đã có (bảng đơn, outbox, `OrderCreated`), tài liệu này chỉ đặt tên cho phần mới. Một người nắm code B5 rà bảng ở mục 2 đến 6 một lượt trước khi chia việc.
- [x] **2. Bếp trả lời yêu cầu hủy về đâu.** Đề xuất: giữ như mục 2. `CancelPending` về topic của chi nhánh gửi yêu cầu vì chỉ chi nhánh đó có cờ cần cập nhật; kết quả cuối luôn về chi nhánh gốc. Với chi nhánh trung thực, hai chi nhánh này là một.
- [x] **3. "Trả lại kết quả cũ".** Đề xuất: phát tin mới với `event_id` mới. Gửi lại dòng outbox cũ thì bên nhận đã có `event_id` đó trong `processed_events` và bỏ qua, tức là không trả lời gì; tin mới thì bên nhận áp lại an toàn nhờ quy tắc hạng và BR12.
- [x] **4. Bộ đếm `seq`.** Đề xuất: một dòng trong `menu_version`. Cái giá là các giao dịch làm món đổi trạng thái phải xếp hàng trên dòng này; việc đó hiếm, vì đa số lần giữ không làm món nào đổi trạng thái.
- [x] **5. Món Hết khi dữ liệu còn mới.** Đề xuất: chỉ chặn ở giao diện, `POST /orders` không kiểm tra `menu_cache`. Kho là nơi duy nhất từ chối đơn vì nguyên liệu; thêm 409 ở chi nhánh là thêm một đường từ chối thứ hai dựa trên dữ liệu chỉ là gợi ý.
- [x] **6. Số partition.** Đề xuất: 2 cho `order-events.*`, `cancel-events.*`, `kitchen-orders` và `stock-commands`; 1 cho `status-events.*`, `menu-events` và `dlq`. Khóa phân vùng là mã đơn nên một đơn vẫn nằm trọn trong một partition. Ở kịch bản 42, chọn hai mã đơn rơi vào hai partition khác nhau và kiểm tra trong log rằng hai instance Inventory mỗi bên xử lý một đơn; cách Kafka chia partition cho instance cần chạy thử để xác nhận.
- [x] **7. Bảng `alerts`.** Đề xuất: một bảng ở Kho và một bảng ở Bếp cho ba loại cảnh báo của Luồng D, đọc qua `GET /alerts`. Cách khác là chỉ ghi log, nhưng khi đó dấu "cần kiểm tra" của kịch bản 29 không có chỗ lưu và các kịch bản 22, 27, 34 phải kiểm bằng cách dò log.
- [x] **8. Tham số của `clear-check`.** Đề xuất: `counted_on_hand` là tồn thực tế, Kho tự trừ lượng đang giữ. Nếu nhóm muốn người kiểm kho nhập thẳng tồn khả dụng thì đổi tên tham số và bỏ phép trừ.
- [x] **9. Thủ tục tăng `epoch`.** Đề xuất: như mục 3, gọi qua `POST /admin/epoch/bump`. Khôi phục Kho từ bản sao lưu vẫn là việc thủ công ngoài phạm vi; thủ tục này chỉ lo phần "tăng epoch" mà đặc tả yêu cầu hệ thống bảo đảm.
- [x] **10. Kho trả lại kết quả cũ về đâu.** Đề xuất: chỉ về chi nhánh gốc, không phát lại sang Bếp. Bếp đã nhận tin gốc qua outbox, và phát lại sang Bếp chỉ làm Bếp trả lời chi nhánh thêm một lần nữa.
- [x] **11. Cách thử kết nối trước khi thử gửi.** Đặc tả v1.5 quy định bộ gửi chỉ bắt đầu thử gửi khi phép thử kết nối thành công (BR9); lựa chọn của tài liệu này là cách thử. Đề xuất: lấy metadata của cluster Kafka với thời gian chờ ngắn, và dùng luôn kết quả đó làm trạng thái kết nối ở `GET /monitor`. Cách khác là để thu ngân tự bật chế độ ngoại tuyến, nhưng khi đó tính đúng của kịch bản 10 phụ thuộc vào thao tác của người.

## 9. Kết quả chạy thử

Phần này ghi những gì đã được chạy thật, để phân biệt với những gì mới chỉ đối chiếu bằng cách đọc.

Lần chạy ngày 21/11/2026 dùng PostgreSQL 16.2 và một script tạm không thuộc mã nguồn dự án. DDL ở mục 3, 4, 5 và năm thao tác ở mục 5 được trích nguyên văn từ tài liệu này; các quy tắc viết bằng lời ở mục 3 và 4 được hiện thực ở mức tối thiểu để chạy được. Lần chạy không có Kafka: thứ tự tin được mô phỏng bằng cách gọi các thao tác theo thứ tự mong muốn, và tranh chấp được tạo bằng hai kết nối chạy cùng lúc. Lần chạy gần nhất là trên bản sửa 3; cả 44 phép kiểm đều đạt.

| Phần | Đã kiểm | Kịch bản có phần được kiểm |
|---|---|---|
| Schema | DDL của cả ba DB tạo được | |
| Kho | Giữ, đã dùng, hoàn, hao hụt đúng bảng hạch toán; hai lớp chống trùng; lệnh không khớp và lệnh tới bản ghi Thất bại ghi đúng một dòng `alerts`; hai bất biến trả về 0 dòng; `clear-check` với `counted_on_hand` | 5, 6, 22, 30, 43, 44 |
| Kho, tranh chấp | 200 lần hai đơn tranh một nguyên liệu: luôn đúng một đơn được giữ | 42 |
| Trạng thái món | Không khóa `dish_status` thì tái hiện được trạng thái sai; khóa trước khi đọc tồn thì đúng ở cả hai thứ tự giành khóa; tăng `epoch` ghi lại mọi dòng | |
| Chi nhánh | Hủy cục bộ; áp tin theo hạng; `CancelPending` tới sau `OrderFailed` bị bỏ qua; `CancelRejected` tới trước và sau Hoàn tất; hợp nhất `menu_cache` theo phiên bản | 7, 8, 10, 19, 20, 36 |
| Chi nhánh, tranh chấp | 200 lần bộ gửi và nút hủy cùng lúc (75 lần hủy cục bộ, 125 lần đi qua Bếp); 200 lần hai lệnh hủy cùng lúc; 200 lần hủy và tin Hoàn tất cùng lúc. Cách làm của bản đầu tái hiện được cả hai lỗi đã nêu | 37 |
| Bếp | Hủy tới trước đơn; đơn tới lặp; hủy sai chi nhánh trước và sau khi đơn tới; lý do theo chi nhánh; hủy khi Đang nấu; cảnh báo trên phiếu; dấu "cần kiểm tra" không bị nhân đôi | 1, 2, 6, 23, 27, 29, 34, 35 |
| Bếp, tranh chấp | 200 lần Hủy và Bắt đầu nấu cùng lúc (103 lần nấu thắng, 97 lần hủy thắng); 200 lần đơn và yêu cầu hủy của cùng mã đơn tới cùng lúc | 3, 28 |

Chưa kiểm:

- Mọi thứ cần Kafka thật: kịch bản 4, 12, 24, 25, 31 và phần consumer của kịch bản 26, 29.
- Kiểm tra hạn X giờ và thời điểm tạo ở tương lai (kịch bản 15, 16).
- Phép thử kết nối trước khi thử gửi (mục 5), vì cần Kafka thật.
- Kịch bản 9, 18, 21, 33, 39 và các kịch bản mới 48–55.
- Các API ở mục 6.
- Phần phát `IngredientCheckChanged` của thủ tục tăng `epoch`.

Các lần chạy thử này chỉ xác nhận thiết kế, không thay cho kiểm thử tự động mà mục 12 của đặc tả yêu cầu.
