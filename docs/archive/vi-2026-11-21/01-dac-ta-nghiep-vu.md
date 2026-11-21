# Đặc tả nghiệp vụ v1.5 – Hủy đơn, từ chối phiếu và báo hết món

v1.5 · 21/11/2026 · @Naul

> Từ v1.5, bản Markdown này là bản chính của đặc tả, thay cho bản PDF v1.4 ngày 19/11/2026. Sơ đồ kiến trúc là hình trong bản PDF nên ở đây được mô tả lại bằng chữ.

**v1.5 (21/11/2026), sửa theo vòng rà soát thứ ba:** thêm quy tắc về lúc bộ gửi bắt đầu thử gửi, sửa ba chỗ đặc tả tự mâu thuẫn, bổ sung các dòng còn thiếu ở bảng 3.2, bảng 3.3 và Luồng D, và thêm 8 kịch bản (48–55). Không quy tắc BR nào đổi hướng.

| Mục | Thay đổi ở v1.5 | Lý do |
|---|---|---|
| 2 (BR5, BR9), 9 | Bộ gửi chỉ bắt đầu thử gửi một gói khi phép thử kết nối tới Kafka vừa thành công. Nhãn "Chưa gửi, bếp chưa nhận" chỉ dùng cho gói chưa thử gửi | Thiếu quy tắc này thì đơn tạo lúc mất mạng vẫn bị ghi cờ, và kịch bản 10 không đạt |
| 1 (Thuật ngữ), 9, 12 | Chống trùng theo hai lớp; định nghĩa "trả lại kết quả cũ" | Câu cũ "chống trùng theo mã đơn và loại tin" làm yêu cầu hủy của chi nhánh gốc bị coi là trùng với yêu cầu sai chi nhánh tới trước, trái kịch bản 35 |
| 1, 14 | Ghi rõ mô hình một Bếp và một Kho ở trung tâm; sửa câu "nguyên liệu thực tế nằm ở chi nhánh khác" | Hai câu cũ mâu thuẫn nhau |
| 15 | Lần giữ bị kẹt không phát hiện được bằng truy vấn bất biến | Khi lệnh hoàn bị mất, cả ba bất biến vẫn đúng |
| 3.2 | Thêm thứ tự áp dụng khi nhiều dòng của bảng cùng khớp | Phiếu Hoàn tất khớp hai dòng cho hai kết quả khác nhau |
| 3.3 | Thêm ô "Đang giữ, đơn tới lặp"; tách dòng tin lặp; lệnh tới bản ghi Thất bại được ghi nhật ký | Bảng thiếu một ô và có một ô đọc được theo hai cách |
| 3.4 | Gỡ món nghĩa là xóa công thức | Chưa có lý do Thất bại cho món đã gỡ |
| 8 | Thêm bốn dòng vào bảng Luồng D | Các cảnh báo này có ở mục 9, 10 và 13.2 nhưng chưa có người xem |
| 12 | Thêm kịch bản 48–55; thêm tiền điều kiện cho kịch bản 10 và 27; viết lại kịch bản 12; ghi rõ tin đánh dấu kiểm kho làm ở đợt 1 | Các hành vi này chưa có kịch bản kiểm |
| Sơ đồ, 10, 13.5, 15 | Khóa phân vùng của `menu-events`; ba tham số tên N; Kho dừng lâu hơn X; đơn có tin trong `dlq` | Làm rõ câu chữ |

**v1.4 (20/11/2026), bản gọn cho đồ án:** giữ trọn ba luồng nghiệp vụ A, B và C; bỏ các phần hạ tầng và vận hành chuẩn production để vừa sức hiện thực và demo. Đã bỏ: replica đồng bộ của Inventory DB và Kitchen DB, xác thực và ACL trên Kafka, hàng cô lập, dọn bản ghi theo TTL, đối soát quá giờ G và thao tác khép đơn có tin trong `dlq`. Mục 15 ghi lại từng phần đã bỏ, hệ quả và hướng mở rộng; bản v1.3 là bản đầy đủ để tham chiếu. Sơ đồ được vẽ lại, mục 13.6 nêu từng thành phần phục vụ vấn đề nào, mục 12 còn 36 kịch bản.

**Các bản trước:** v1.1 bổ sung giả định triển khai cho Kafka và PostgreSQL. v1.2 và v1.3 sửa theo hai vòng rà soát: hủy cục bộ đơn chưa từng thử gửi, Bếp ghi lại kết quả Thất bại và chi nhánh gốc, Kho từ chối giữ nguyên liệu đang chờ kiểm kho, bảng hạch toán tồn kho.

## Sơ đồ kiến trúc và luồng tin

Kiến trúc v1.4 (bản gọn): luồng tin giữa chi nhánh, Kho, Bếp và Quản lý. Bảng dưới liệt kê các thành phần có trên sơ đồ gốc; thành phần ghi "mới" là phần thêm so với kiến trúc ban đầu.

| Khối | Thành phần | Ghi chú trên sơ đồ |
|---|---|---|
| Nguồn đơn: POS site × N (POS01 … POSN) | Local Order Service, 1 mỗi site | Ghi nhận đơn cả khi mất mạng |
| Nguồn đơn: POS site × N | Local Order DB | Đơn hàng, hàng chờ gửi (outbox), cache trạng thái món (mới) |
| Nguồn đơn: Online site (web, app) | Online Order Service, 1 instance | Dùng chung luồng với POS |
| Nguồn đơn: Online site | Online Order DB | Đơn hàng, hàng chờ gửi (outbox), cache trạng thái món (mới) |
| Trung tâm dữ liệu (báo cáo) | Central Order Replica | Bản sao các DB đơn hàng, nhận nhân bản bất đồng bộ từ Local Order DB và Online Order DB |
| Trung tâm dữ liệu (báo cáo) | Order Query Service | |
| Trung tâm dữ liệu (báo cáo) | Dashboard quản lý | Hiện mốc "dữ liệu tính đến" (mới) |
| Kafka, 3 broker | Cấu hình chung | `acks=all`, 3 bản sao mỗi tin, `min.insync` 2, giữ tin 7 ngày |
| Kafka, topic theo chi nhánh | `order-events.<chi nhánh>` | Đơn mới |
| Kafka, topic theo chi nhánh | `cancel-events.<chi nhánh>` (mới) | Yêu cầu hủy |
| Kafka, topic theo chi nhánh | `status-events.<chi nhánh>` | Trạng thái đơn của chi nhánh |
| Kafka, topic nội bộ | `kitchen-orders` (mới) | Đơn đã giữ, đơn thất bại |
| Kafka, topic nội bộ | `stock-commands` (mới) | Hoàn và đã dùng nguyên liệu |
| Kafka, topic nội bộ | `menu-events` (mới) | Trạng thái món, heartbeat |
| Kafka, topic nội bộ | `dlq` (mới) | Tin không đọc được, không chặn consumer |
| Kho (Inventory), dùng chung cả chuỗi | Inventory Service, 2 instance | API lấy snapshot trạng thái món (mới) |
| Kho (Inventory) | Inventory DB | Tồn kho toàn chuỗi, lần giữ theo mã đơn; cờ cần kiểm kho, seq phiên bản, `processed_events` (mới); `outbox_events` |
| Bếp (Kitchen) | Kitchen Service, 1 instance | Bên duy nhất quyết định hủy (mới) |
| Bếp (Kitchen) | Kitchen DB | Phiếu bếp, một bản ghi mỗi mã đơn; Hủy chờ đơn, chi nhánh gốc, `processed_events` (mới); `outbox_events` |

Các luồng tin trên sơ đồ, tất cả đi qua Kafka:

- Chi nhánh gửi đơn mới và yêu cầu hủy; chi nhánh nhận trạng thái đơn và trạng thái món.
- Kho nhận đơn mới và lệnh hoàn hoặc báo đã dùng; Kho gửi trạng thái đơn, đơn cho Bếp và trạng thái món.
- Bếp nhận yêu cầu hủy và đơn từ Kho; Bếp gửi trạng thái đơn và lệnh hoàn hoặc báo đã dùng.

Mọi tin giữa các site đi qua Kafka và được ghi qua outbox. Đơn đi theo đường `order-events.<chi_nhanh>` → Inventory → `kitchen-orders` → Kitchen, còn yêu cầu hủy đi theo `cancel-events.<chi_nhanh>` thẳng tới Kitchen, nên có thể tới trước đơn (BR7). Chi nhánh chỉ nhận trạng thái qua `status-events.<chi_nhanh>` của mình.

Khóa phân vùng là mã đơn (riêng `menu-events` dùng mã món hoặc mã nguyên liệu), nên thứ tự chỉ được giữ trong một topic, không giữ giữa hai topic. Topic được tách theo chi nhánh để mỗi chi nhánh chỉ đọc tin trạng thái của mình và để Kitchen biết chi nhánh nào gửi yêu cầu hủy; bản gọn không xác thực và không dùng ACL (mục 13.2). POS và Online lấy snapshot món qua API của Inventory Service khi kết nối lại (BR10).

## 1. Mục đích và phạm vi

Bản v1.0 bổ sung ba luồng nghiệp vụ (Bếp từ chối phiếu, hủy đơn theo yêu cầu, báo hết món) và một luồng theo dõi. Mục tiêu là vá khoảng trống của luồng nền: Kho đã giữ nguyên liệu thì không lấy lại được nếu đơn không được nấu. Tài liệu này là nguồn duy nhất để hiện thực; mọi chỗ ghi "đề xuất" là giá trị nhóm còn có thể đổi.

**Giả định phạm vi:** mỗi đơn có đúng một phiếu bếp; hủy và từ chối luôn áp dụng cho cả đơn, không có hủy một phần. Kho dùng chung cho cả chuỗi: tồn kho và trạng thái món tính chung, giống nhau ở mọi chi nhánh. Hệ thống có một Bếp và một Kho vật lý ở trung tâm, phục vụ mọi chi nhánh; chi nhánh chỉ là nơi nhận đơn.

### Các bên tham gia

| Bên | Vai trò | Quyền quyết định |
|---|---|---|
| Chi nhánh (POS, Online) | Tạo đơn, gửi yêu cầu hủy, hiển thị trạng thái đơn và món | Chỉ tự chốt trạng thái cho đơn có gói tạo đơn chưa từng thử gửi (BR9) |
| Kho | Giữ, hoàn, ghi hao hụt nguyên liệu; phát trạng thái món | Quyết định cuối về nguyên liệu và về việc nhận đơn |
| Bếp | Nhận phiếu, nấu, từ chối phiếu | Bên duy nhất quyết định hủy đơn |
| Quản lý | Xem số liệu tổng hợp toàn chuỗi | Không thay đổi dữ liệu; số liệu có thể trễ |

### Thuật ngữ

- **Giữ:** Kho tạm khóa nguyên liệu cho một đơn, ghi lại lượng từng loại.
- **Hoàn:** trả lượng đã giữ về tồn khả dụng.
- **Hao hụt:** ghi bỏ lượng đã giữ, không trả về tồn khả dụng.
- **Đã dùng:** lượng đã giữ được tính là tiêu thụ vì món đã bắt đầu nấu.
- **Hủy chờ đơn:** bản ghi Bếp tạo khi yêu cầu hủy tới trước đơn. Bản ghi lưu mọi chi nhánh đã gửi yêu cầu hủy.
- **Hàng chờ gửi:** nơi POS lưu bền các gói tin (tạo đơn, yêu cầu hủy) chưa được Kafka xác nhận.
- **Đã thử gửi:** cờ lưu bền trên mỗi gói trong hàng chờ gửi, được ghi trước lần gửi đầu tiên. Gói chưa có cờ này chắc chắn chưa tới Kafka. Bộ gửi chỉ ghi cờ khi phép thử kết nối tới Kafka vừa thành công (BR9).
- **Trả lại kết quả cũ:** phát một tin mới mang kết quả ứng với trạng thái đang lưu của đơn, không lặp lại hiệu ứng nào: không giữ, không hoàn, không lập phiếu lần hai.
- **Phiên bản (epoch, seq):** cặp số dùng để so sánh độ mới của trạng thái món. `epoch` là số đời của dữ liệu Kho, `seq` là số thứ tự lưu bền trong Kho.
- **Snapshot:** toàn bộ trạng thái món tại một thời điểm, kèm phiên bản từng món.
- **Heartbeat:** tín hiệu định kỳ Kho gửi để báo vẫn liên lạc được, kèm `epoch` hiện tại.

## 2. Quy tắc nghiệp vụ

BR1–BR5 giữ nguyên từ luồng nền (BR3, BR4 được nói rõ hơn). BR6–BR12 là quy tắc mới của v1.0, đã được sửa qua hai vòng rà soát. Bản gọn v1.4 rút gọn BR5, BR8, BR9, BR11 và BR12.

| Mã | Quy tắc |
|---|---|
| BR1 | Kho giữ tất cả nguyên liệu của đơn hoặc không giữ gì. |
| BR2 | Bếp chỉ nấu đơn mà Kho đã giữ nguyên liệu. |
| BR3 | Trạng thái đơn không đi lùi. Chi nhánh chỉ nhận tin có hạng cao hơn hạng hiện tại (mục 3.1); trạng thái cuối không bao giờ bị thay. Nhãn "Đang yêu cầu hủy" là cờ phụ, không phải trạng thái. |
| BR4 | Gửi lại cùng một tin không tạo hiệu ứng thứ hai. Áp dụng cho mọi bên nhận (Kho, Bếp, chi nhánh) và mọi loại tin (tạo đơn, yêu cầu hủy, lệnh hoàn, báo đã dùng, kết quả). |
| BR5 | Chi nhánh mất mạng vẫn ghi nhận đơn mới và yêu cầu hủy vào hàng chờ gửi, nhưng chưa phục vụ được: Bếp ở trung tâm nên đơn tạo lúc mất mạng không được nấu cho tới khi có mạng lại. Đơn mới chịu ngưỡng X giờ (BR11), quá hạn thì Thất bại. Với gói tạo đơn chưa thử gửi, POS hiện "Chưa gửi, bếp chưa nhận"; với gói đã thử gửi nhưng chưa được Kafka xác nhận, POS hiện "Đang gửi, chưa rõ bếp đã nhận". Yêu cầu hủy có hai trường hợp. Gói tạo đơn chưa từng thử gửi: POS hủy cục bộ, chắc chắn không nấu (BR9). Gói tạo đơn đã thử gửi: yêu cầu hủy được gửi tới Bếp và thử lại mãi khi lỗi mạng, nhưng không chắc thành công vì đơn có thể đã được nấu; POS hiện "Yêu cầu hủy chưa gửi, chưa chắc hủy được". |
| BR6 | Kho xử lý lệnh hoàn theo lý do. Nguyên liệu bị báo hỏng thì ghi hao hụt, đánh dấu cần kiểm kho và tạm coi như Hết; các nguyên liệu khác của cùng đơn thì hoàn. Kho không giữ nguyên liệu đang bị đánh dấu cho đơn mới, dù tồn khả dụng còn đủ: đơn cần nguyên liệu đó nhận Thất bại (nguyên liệu đang chờ kiểm kho), và việc kiểm tra nằm trong cùng giao dịch với thao tác giữ. Kho hoàn theo lượng đã ghi lúc giữ, không tính lại theo công thức. Phiếu mang danh sách nguyên liệu đã giữ; Bếp cảnh báo các phiếu Chờ nấu có nguyên liệu đang bị đánh dấu. |
| BR7 | Bếp là bên duy nhất quyết định hủy cho đơn đã thử gửi. Bếp không chốt Đã hủy cho đơn chưa tới: Bếp tạo bản ghi Hủy chờ đơn và trả cam kết "bếp sẽ không nấu". Cam kết chỉ có hiệu lực khi chi nhánh yêu cầu hủy đúng là chi nhánh gốc của đơn (mục 13.2). Giữa Hủy và Bắt đầu nấu, thao tác nào được Bếp ghi trước thì thắng. |
| BR8 | Bếp giữ đúng một bản ghi cho mỗi mã đơn, kể cả đơn Kho báo Thất bại. Bản ghi không bao giờ bị xóa, nên bản ghi Hủy chờ đơn có hiệu lực vô thời hạn và tin lặp luôn nhận lại kết quả cũ. |
| BR9 | POS kiểm tra dữ liệu đơn trước khi nhận: đơn sai dữ liệu bị từ chối ngay (HTTP 400) và không vào hàng chờ gửi. POS giữ đúng thứ tự gửi theo từng mã đơn, không theo cả hàng chờ, và ghi bền cờ "đã thử gửi" trước lần gửi đầu tiên của mỗi gói. Bộ gửi chỉ bắt đầu thử gửi một gói, tức ghi cờ rồi gửi, khi phép thử kết nối tới Kafka vừa thành công; không kết nối được thì gói ở nguyên trạng thái chưa thử gửi và vẫn hủy cục bộ được. Phép thử kết nối chỉ quyết định lúc bắt đầu thử; việc được hủy cục bộ hay không vẫn do cờ quyết định. POS chỉ tự chốt trạng thái trong một trường hợp: có yêu cầu hủy khi gói tạo đơn chưa từng thử gửi, khi đó POS khép Đã hủy và bỏ gói. Gói đã thử gửi thì không bao giờ hủy cục bộ và không bao giờ bị bỏ, dù lần gửi đó báo lỗi: mọi lỗi gửi đều được thử lại với khoảng chờ tăng dần. Kafka xác nhận đã nhận gói không có nghĩa Kho đã nhận đơn. Thứ tự gửi ở POS **không** đảm bảo thứ tự Bếp nhận. |
| BR10 | Kho chỉ phát tin khi món đổi giữa Còn, Hết và Ẩn. Mỗi tin mang phiên bản (epoch, seq) lấy từ số thứ tự lưu bền, không dùng giờ hệ thống. Kho gửi heartbeat định kỳ kèm epoch. POS hợp nhất từng món theo phiên bản, không xóa bộ nhớ đệm để nạp lại; khi thấy epoch mới, POS lấy snapshot rồi chuyển các món còn mang đời cũ sang Ẩn. |
| BR11 | Kho trả Thất bại và không giữ nguyên liệu cho đơn tạo quá X giờ, hoặc có thời điểm tạo ở tương lai quá dung sai. |
| BR12 | Kho giữ đúng một bản ghi kết quả cho mỗi mã đơn, kể cả đơn Thất bại vì sai dữ liệu, để đơn gửi lặp không bao giờ được xét lại. Bản ghi không bao giờ bị xóa. Kho chuyển lần giữ sang Đã dùng khi Bếp báo Đang nấu; chỉ hoàn khi lần giữ còn Đang giữ. Mỗi lần giữ rời Đang giữ đúng một lần (mục 3.3). Lệnh hoàn không khớp bản ghi giữ nào thì bỏ qua và ghi nhật ký, tuyệt đối không cộng vào tồn. |

BR12 được thêm khi tổng hợp bản này, xem mục 11.

## 3. Máy trạng thái

Có bốn máy trạng thái độc lập: đơn tại chi nhánh, bản ghi đơn tại Bếp (bao gồm phiếu), bản ghi đơn tại Kho và trạng thái món. Mọi chuyển trạng thái không có trong bảng đều bị từ chối.

### 3.1 Đơn tại chi nhánh

Hạng: Chờ Kho = 0, Đã giữ = 1, Hoàn tất / Đã hủy / Thất bại = 2 (trạng thái cuối). Tin có hạng nhỏ hơn hoặc bằng hạng hiện tại bị bỏ qua. Đơn có thể nhảy hạng, ví dụ từ Chờ Kho sang Đã hủy khi tin Đã giữ đến sau hoặc bị mất.

| Từ | Sự kiện | Đến | Bên gửi |
|---|---|---|---|
| (chưa có) | Thu ngân hoặc khách gửi đơn | Chờ Kho | POS, Online |
| Chờ Kho | Kho giữ đủ nguyên liệu | Đã giữ | Kho |
| Chờ Kho | Kho từ chối (lý do ở mục 10) | Thất bại | Kho |
| Chờ Kho, Đã giữ | Bếp chốt hủy hoặc từ chối phiếu | Đã hủy | Bếp |
| Chờ Kho, Đã giữ | Bếp báo hoàn tất | Hoàn tất | Bếp |
| Chờ Kho | Yêu cầu hủy khi gói tạo đơn chưa từng thử gửi | Đã hủy (khách hủy) | POS |

Cờ phụ "Đang yêu cầu hủy":

- Bật khi thu ngân hoặc khách gửi yêu cầu hủy.
- Thêm dòng "bếp sẽ không nấu" khi Bếp xác nhận đã tạo bản ghi Hủy chờ đơn.
- Tắt khi Bếp từ chối hủy (hiện thông báo "Không hủy được: bếp đã bắt đầu nấu") hoặc khi đơn vào trạng thái cuối.

### 3.2 Bản ghi đơn tại Bếp

Mỗi mã đơn có đúng một bản ghi (BR8). "Phiếu" là bản ghi ở các trạng thái Chờ nấu, Đang nấu, Hoàn tất. Trạng thái cuối: Hoàn tất, Đã hủy, Thất bại. Mọi chuyển trạng thái phải là ghi có điều kiện trên trạng thái hiện tại. Dòng `CHUA_CO` ở mục 13.4 được coi là "(chưa có)". "Chi nhánh gốc" là chi nhánh tạo đơn, do Kho chuyển kèm; bản ghi Hủy chờ đơn lưu danh sách chi nhánh đã gửi yêu cầu hủy.

| Từ | Sự kiện | Đến | Bếp làm thêm |
|---|---|---|---|
| (chưa có) | Đơn tới từ Kho | Chờ nấu | Hiện phiếu kèm danh sách nguyên liệu đã giữ |
| (chưa có) | Yêu cầu hủy tới | Hủy chờ đơn | Ghi chi nhánh gửi; trả chi nhánh cam kết "bếp sẽ không nấu" |
| (chưa có) | Kho báo Thất bại | Thất bại | Ghi kết quả cuối, để yêu cầu hủy tới sau không tạo Hủy chờ đơn |
| Hủy chờ đơn | Yêu cầu hủy từ chi nhánh chưa có trong danh sách | (giữ nguyên) | Thêm chi nhánh vào danh sách; trả cam kết "bếp sẽ không nấu" |
| Hủy chờ đơn | Đơn tới từ Kho, chi nhánh gốc có trong danh sách | Đã hủy (khách hủy) | Gửi Kho lệnh hoàn; báo chi nhánh Đã hủy |
| Hủy chờ đơn | Đơn tới từ Kho, chi nhánh gốc không có trong danh sách | Chờ nấu | Bác các yêu cầu hủy đã ghi và cảnh báo (mục 13.2); xử lý đơn như khi chưa có bản ghi |
| Hủy chờ đơn | Kho báo Thất bại | Thất bại | Không làm gì thêm |
| Chờ nấu | Nhân viên bấm Bắt đầu nấu | Đang nấu | Báo Kho Đã dùng |
| Chờ nấu | Yêu cầu hủy tới từ chi nhánh gốc | Đã hủy (khách hủy) | Gửi Kho lệnh hoàn; báo chi nhánh Đã hủy |
| Chờ nấu | Nhân viên bấm Từ chối kèm lý do | Đã hủy (Bếp từ chối) | Gửi Kho lệnh hoàn kèm lý do (BR6); báo chi nhánh Đã hủy |
| Đang nấu | Nhân viên bấm Hoàn tất | Hoàn tất | Báo chi nhánh Hoàn tất |
| Đang nấu, Hoàn tất | Yêu cầu hủy tới từ chi nhánh gốc | (giữ nguyên) | Trả chi nhánh "Không hủy được: bếp đã bắt đầu nấu" |
| Chờ nấu, Đang nấu hoặc trạng thái cuối | Yêu cầu hủy từ chi nhánh khác chi nhánh gốc | (giữ nguyên) | Bỏ qua yêu cầu và cảnh báo (mục 13.2) |
| Chờ nấu, Đang nấu | Đơn tới lặp từ Kho | (giữ nguyên) | Bỏ qua, không lập phiếu thứ hai |
| Hủy chờ đơn hoặc trạng thái cuối | Tin lặp (đơn, yêu cầu hủy, Thất bại) | (giữ nguyên) | Trả lại kết quả cũ, không gửi lệnh hoàn lần hai |

Khi nhiều dòng cùng khớp một tin, áp theo thứ tự sau:

1. Yêu cầu hủy từ chi nhánh khác chi nhánh gốc: bỏ qua và cảnh báo, ở mọi trạng thái đã biết chi nhánh gốc.
2. Dòng nêu đích danh trạng thái và sự kiện. Ví dụ yêu cầu hủy từ chi nhánh gốc tới phiếu Hoàn tất nhận "Không hủy được: bếp đã bắt đầu nấu", không nhận lại kết quả Hoàn tất.
3. Dòng cuối bảng, cho mọi tin còn lại tới bản ghi Hủy chờ đơn hoặc trạng thái cuối. Dòng này cũng áp cho yêu cầu hủy đầu tiên từ chi nhánh gốc tới bản ghi Đã hủy hoặc Thất bại, dù đó không phải tin lặp.

### 3.3 Bản ghi đơn tại Kho

Mỗi mã đơn có đúng một bản ghi (BR12). Bản ghi lưu lượng đã giữ của từng nguyên liệu và kết quả của từng dòng.

| Từ | Sự kiện | Đến | Kho làm thêm |
|---|---|---|---|
| (chưa có) | Đơn tới, trong hạn, đủ nguyên liệu và không nguyên liệu nào đang bị đánh dấu cần kiểm kho | Đang giữ | Báo chi nhánh Đã giữ; chuyển đơn kèm danh sách nguyên liệu đã giữ sang Bếp |
| (chưa có) | Đơn tới, thiếu nguyên liệu, có nguyên liệu đang bị đánh dấu cần kiểm kho, món chưa có công thức, dữ liệu đơn không hợp lệ, quá hạn hoặc thời điểm tạo ở tương lai | Thất bại | Báo Thất bại kèm lý do cho cả chi nhánh và Bếp |
| Đang giữ | Lệnh hoàn từ Bếp | Đã trả | Mỗi dòng: hoàn, hoặc ghi hao hụt nếu là nguyên liệu bị báo hỏng (BR6) |
| Đang giữ | Bếp báo Đã dùng | Đã dùng | Chuyển lượng đã giữ sang Đã dùng; không trừ tồn khả dụng lần hai |
| Đang giữ, Thất bại, Đã trả, Đã dùng | Đơn tới lặp | (giữ nguyên) | Trả lại kết quả cũ cho chi nhánh; không giữ lần hai, không chuyển đơn sang Bếp lần hai |
| Đã trả, Đã dùng | Lệnh hoàn hoặc báo Đã dùng | (giữ nguyên) | Bỏ qua; không hoàn và không tính đã dùng lần hai |
| Thất bại hoặc (không có bản ghi) | Lệnh hoàn hoặc báo Đã dùng | (giữ nguyên hoặc không tạo) | Bỏ qua, ghi nhật ký; không cộng vào tồn |

#### Hạch toán tồn kho

Mỗi nguyên liệu có bốn đại lượng: **tồn khả dụng** (cột `available`, còn giữ được cho đơn mới), **đang giữ**, **đã dùng** và **hao hụt** (hai số sau là số cộng dồn). Tồn thực tế trong kho bằng tồn khả dụng cộng đang giữ. Mỗi thao tác chỉ đổi các đại lượng theo bảng dưới, với q là lượng đã ghi lúc giữ.

| Thao tác | Tồn khả dụng | Đang giữ | Đã dùng | Hao hụt |
|---|---|---|---|---|
| Giữ cho một đơn | − q | + q | không đổi | không đổi |
| Hoàn | + q | − q | không đổi | không đổi |
| Bếp báo Đã dùng | không đổi | − q | + q | không đổi |
| Ghi hao hụt (nguyên liệu hỏng) | không đổi | − q | không đổi | + q |
| Nhập hàng, điều chỉnh sau kiểm kho | cộng hoặc trừ lượng điều chỉnh | không đổi | không đổi | không đổi |

Ví dụ: Kho có 10 kg thịt khả dụng, một đơn cần 2 kg. Sau khi giữ còn 8 kg khả dụng và 2 kg đang giữ. Bếp bắt đầu nấu thì khả dụng vẫn là 8 kg, 2 kg chuyển sang đã dùng. Nếu khách hủy trước khi nấu thì khả dụng trở lại 10 kg.

Bất biến phải kiểm tra được bằng truy vấn: tồn khả dụng không bao giờ âm; lượng đang giữ của một nguyên liệu bằng tổng lượng đã ghi của các bản ghi còn ở Đang giữ; mỗi lần giữ rời Đang giữ đúng một lần, tới đúng một trong ba đích là hoàn, đã dùng hoặc hao hụt. Đánh dấu cần kiểm kho không đổi đại lượng nào, nhưng chặn mọi lần giữ mới trên nguyên liệu đó (BR6).

### 3.4 Trạng thái món

Mỗi món có một trong ba trạng thái, luôn kèm phiên bản (epoch, seq). So sánh phiên bản theo thứ tự từ điển: so epoch trước, bằng nhau thì so seq.

| Trạng thái | Điều kiện tại Kho | POS và Online hiển thị |
|---|---|---|
| Còn | Đủ nguyên liệu khả dụng cho ít nhất 1 phần và không nguyên liệu nào đang bị đánh dấu cần kiểm kho | Cho chọn |
| Hết | Không đủ cho 1 phần, hoặc có nguyên liệu đang bị đánh dấu cần kiểm kho | Chặn hoặc cảnh báo (mục 7) |
| Ẩn | Món chưa có công thức hoặc đã bị gỡ | Không hiển thị |

Gỡ một món nghĩa là xóa công thức của nó. Đơn có món đã gỡ vì vậy nhận Thất bại với lý do "Món chưa có công thức", kể cả khi POS còn hiện món đó do dữ liệu cũ.

Món Hết vì nguyên liệu bị đánh dấu không chỉ là gợi ý cho giao diện: Kho từ chối giữ nguyên liệu đó cho mọi đơn mới, kể cả đơn đặt lúc POS mất mạng hoặc đang dùng dữ liệu cũ (BR6).

## 4. Luồng nền: đặt đơn

Luồng nền giữ nguyên, chỉ thêm các kiểm tra của BR11 và BR12 tại Kho.

1. Thu ngân hoặc khách chọn món và gửi đơn. POS kiểm tra dữ liệu đơn, sai thì từ chối ngay (HTTP 400). Với đơn hợp lệ, POS sinh mã đơn duy nhất, ghi thời điểm tạo và lưu đơn ở trạng thái Chờ Kho vào hàng chờ gửi, kể cả khi đang mất mạng.
2. Kho nhận đơn. Nếu mã đơn đã có bản ghi, Kho trả lại kết quả cũ và dừng (BR12).
3. Kho kiểm tra hạn: đơn tạo quá X giờ hoặc có thời điểm tạo ở tương lai quá dung sai thì Thất bại (BR11).
4. Kho tính tổng nguyên liệu theo công thức hiện tại. Đủ tất cả và không nguyên liệu nào đang bị đánh dấu cần kiểm kho thì giữ, ghi lượng từng loại và báo Đã giữ. Thiếu dù một loại, có nguyên liệu đang bị đánh dấu (BR6), hoặc món chưa có công thức, thì không giữ gì và báo Thất bại (BR1). Đơn đọc được nhưng dữ liệu không hợp lệ (thiếu trường bắt buộc, số lượng không dương) cũng nhận Thất bại, không vào `dlq` (mục 9).
5. Kho gửi kết quả về đúng chi nhánh đã tạo đơn. Kết quả Thất bại được gửi cả cho Bếp. Đơn Đã giữ được chuyển sang Bếp kèm danh sách nguyên liệu đã giữ.
6. Bếp lập phiếu Chờ nấu (BR2). Nhân viên bấm Bắt đầu nấu thì phiếu sang Đang nấu và Bếp báo Kho Đã dùng.
7. Nhân viên bấm Hoàn tất thì phiếu và đơn chuyển sang Hoàn tất.
8. Quản lý xem số liệu tổng hợp toàn chuỗi, có thể trễ hơn dữ liệu tại chi nhánh.

## 5. Luồng A: Bếp từ chối phiếu

Bếp chỉ từ chối được phiếu đang Chờ nấu. Kho hoàn hoặc ghi hao hụt tùy lý do.

| Mục | Nội dung |
|---|---|
| Người thực hiện | Nhân viên Bếp |
| Điều kiện | Phiếu đang ở Chờ nấu |
| Tình huống | Hỏng thiết bị, thiếu người, nguyên liệu bị hỏng sau khi đã giữ |

### Các bước

1. Nhân viên chọn phiếu, bấm Từ chối và chọn lý do trong danh sách cố định (mục 10). Với lý do "Nguyên liệu hỏng", nhân viên bắt buộc chọn thêm nguyên liệu nào bị hỏng, trong danh sách nguyên liệu đã giữ của phiếu.
2. Bếp chuyển phiếu sang Đã hủy (Bếp từ chối) bằng ghi có điều kiện. Nếu phiếu không còn ở Chờ nấu, thao tác bị từ chối.
3. Bếp báo chi nhánh đơn Đã hủy kèm lý do. Chi nhánh hiển thị ngay, không chờ Kho.
4. Bếp gửi Kho lệnh hoàn kèm lý do và nguyên liệu hỏng (nếu có).
5. Kho xử lý theo BR6:
   - Hỏng thiết bị, thiếu người: hoàn toàn bộ lượng đã giữ.
   - Nguyên liệu hỏng: ghi hao hụt lượng đã giữ của nguyên liệu đó, hoàn các nguyên liệu còn lại, đánh dấu nguyên liệu hỏng là cần kiểm kho.
6. Nếu có nguyên liệu bị đánh dấu: mọi món dùng nguyên liệu đó chuyển sang Hết (Luồng C), và Bếp hiện cảnh báo đỏ trên các phiếu Chờ nấu có nguyên liệu đó. Từ lúc này Kho từ chối giữ nguyên liệu đó cho đơn mới (BR6).

### Quy tắc

- Phiếu đã Đang nấu thì không từ chối được, phải hoàn tất. Nguyên liệu đã dùng không được hoàn.
- Kho hoàn theo lượng đã ghi lúc giữ, không tính lại theo công thức hiện tại.
- Mỗi đơn chỉ được hoàn một lần, dù lệnh hoàn bị gửi lặp (BR4, BR12).
- Kho đang dừng thì đơn vẫn hiển thị Đã hủy ngay. Lệnh hoàn được lưu bền và chắc chắn được xử lý khi Kho chạy lại.
- Cảnh báo trên phiếu chỉ là cảnh báo: hệ thống không tự hủy phiếu. Nhân viên kiểm tra thấy hỏng thật thì từ chối bằng chính luồng này.
- Cảnh báo và trạng thái Hết được gỡ khi Kho xác nhận kiểm kho xong. Bản gọn làm thao tác này bằng một API của Inventory Service; gỡ đánh dấu cũng cho phép Kho giữ nguyên liệu đó trở lại.

## 6. Luồng B: Hủy đơn theo yêu cầu

Chi nhánh chỉ gửi yêu cầu hủy, Bếp là bên duy nhất quyết định (BR7). Ngoại lệ duy nhất là đơn có gói tạo đơn chưa từng thử gửi: đơn này chắc chắn chưa vào Kafka nên POS hủy tại chỗ (BR9).

| Mục | Nội dung |
|---|---|
| Người gửi yêu cầu | Thu ngân (POS) hoặc khách (Online), dùng chung luồng |
| Người quyết định | Bếp, vì chỉ Bếp biết món đã bắt đầu nấu hay chưa |
| Lý do | Bắt buộc, chọn trong danh sách cố định (mục 10) |
| Đường đi | Yêu cầu hủy đi thẳng từ chi nhánh tới Bếp, không qua Kho |

### Phía chi nhánh

1. Thu ngân hoặc khách bấm Hủy và chọn lý do. Đơn đã ở trạng thái cuối thì nút Hủy bị ẩn.
2. Nếu gói tạo đơn còn trong hàng chờ gửi và chưa có cờ "đã thử gửi": POS khép đơn sang Đã hủy (khách hủy), bỏ gói đó và dừng. Không gửi gì đi. Việc kiểm tra cờ và bỏ gói nằm trong cùng một giao dịch cục bộ với việc bộ gửi ghi cờ; bên nào ghi trước thì thắng.
3. Ngược lại, gói tạo đơn đã thử gửi: POS ghi yêu cầu hủy vào hàng chờ gửi, sau gói tạo đơn của cùng mã đơn, rồi bật cờ "Đang yêu cầu hủy". Trạng thái chính của đơn chưa đổi.
4. Với đơn đã thử gửi, POS luôn gửi cả gói tạo đơn lẫn gói hủy và không hủy cục bộ, dù lần gửi trước báo lỗi: POS không biết Kafka đã nhận gói hay chưa.
5. Chi nhánh cập nhật hiển thị theo phản hồi của Bếp và các tin trạng thái, theo quy tắc hạng ở mục 3.1.

### Bảng quyết định của Bếp

Bếp quyết định theo bản ghi của mình, không theo trạng thái chi nhánh đang thấy. Chi nhánh thấy Đã giữ chưa chắc Bếp đã nhận đơn.

| Bếp thấy khi yêu cầu hủy tới | Bếp làm | Chi nhánh thấy |
|---|---|---|
| Chưa có bản ghi | Tạo Hủy chờ đơn, trả cam kết "bếp sẽ không nấu", chưa chốt trạng thái cuối | "Đang yêu cầu hủy: bếp sẽ không nấu" |
| Chờ nấu | Chuyển Đã hủy, gửi Kho lệnh hoàn | Đã hủy |
| Đang nấu hoặc Hoàn tất | Từ chối hủy | Trạng thái giữ nguyên, thông báo "Không hủy được: bếp đã bắt đầu nấu" |
| Hủy chờ đơn, Đã hủy, Thất bại | Trả lại kết quả cũ | Không đổi |

Bảng này áp dụng cho yêu cầu hủy từ đúng chi nhánh gốc của đơn. Yêu cầu từ chi nhánh khác xử lý theo mục 3.2 và 13.2.

Sau khi có bản ghi Hủy chờ đơn, kết quả cuối phụ thuộc vào Kho:

| Kho xử lý đơn | Kết quả |
|---|---|
| Giữ được nguyên liệu, chuyển đơn sang Bếp | Bếp không lập phiếu, chuyển Đã hủy, gửi Kho lệnh hoàn. Chi nhánh thấy Đã hủy |
| Từ chối (kể cả quá hạn) | Kho gửi Thất bại cho chi nhánh và Bếp. Chi nhánh thấy Thất bại, cờ hủy tắt; Bếp chuyển bản ghi sang Thất bại |
| Kho đang dừng | Chi nhánh giữ "Đang yêu cầu hủy: bếp sẽ không nấu" cho tới khi Kho chạy lại. Bếp không được tự chốt Đã hủy |

### Tranh chấp và mất mạng

- **Hủy và Bắt đầu nấu gần như cùng lúc:** thao tác nào được Bếp ghi trước thì thắng; thao tác còn lại nhận kết quả từ chối. Hai thao tác cùng là ghi có điều kiện trên trạng thái Chờ nấu.
- **Yêu cầu hủy tới Bếp trước đơn:** đây là trường hợp bình thường, không phải lỗi, vì đơn đi vòng qua Kho còn yêu cầu hủy đi thẳng.
- **Chi nhánh mất mạng:** đơn chưa từng thử gửi thì hủy cục bộ, chắc chắn không nấu. Đơn đã thử gửi thì yêu cầu hủy được lưu và gửi sau; yêu cầu này không phải cam kết: khi có mạng lại, Bếp có thể đã nấu xong. Mạng rớt lại giữa lúc gửi gói tạo đơn và gói hủy cũng cho kết quả này. Giao diện phải nói rõ điều này.
- **Bếp đang dừng:** cờ "Đang yêu cầu hủy" hiển thị kèm thời gian đã chờ và được đưa vào cảnh báo của Luồng D.

## 7. Luồng C: Báo hết món

Kho phát trạng thái món cho POS và Online để gợi ý; Kho vẫn quyết định cuối, nên POS thấy món Còn thì đơn vẫn có thể Thất bại.

### Phía Kho

1. Sau mỗi thay đổi tồn (giữ, hoàn, hao hụt, nhập hàng, đánh dấu hoặc gỡ đánh dấu kiểm kho, đổi công thức), Kho tính lại trạng thái các món bị ảnh hưởng theo điều kiện ở mục 3.4. Việc tính lại này chỉ diễn ra bên trong Kho.
2. Kho chỉ phát tin ra ngoài khi trạng thái một món đổi giữa Còn, Hết và Ẩn. Số lượng tồn thay đổi mà trạng thái món không đổi thì không phát gì.
3. Mỗi tin mang mã món, trạng thái và phiên bản (epoch, seq). `seq` lấy từ một số thứ tự lưu bền trong cơ sở dữ liệu Kho, dùng chung cho mọi món, không reset khi Kho khởi động lại. `epoch` chỉ tăng khi Kho được khôi phục từ bản sao lưu.
4. Kho gửi heartbeat theo chu kỳ cố định (mục 10), kèm `epoch` hiện tại.
5. Kho cung cấp snapshot: trạng thái và phiên bản của mọi món, kể cả món Ẩn.
6. Kho cũng phát tin đánh dấu và gỡ đánh dấu "cần kiểm kho" cho Bếp, có phiên bản như trên.

### Phía POS và Online

1. Khi nhận tin đổi trạng thái: chỉ ghi đè món đó nếu phiên bản mới lớn hơn phiên bản đang giữ.
2. Khi kết nối lại: đăng ký nhận tin mới, lấy snapshot, rồi hợp nhất từng món theo đúng quy tắc so phiên bản. Không xóa bộ nhớ đệm để nạp lại.
3. Khi heartbeat hoặc tin bất kỳ mang `epoch` lớn hơn epoch đang giữ: lấy snapshot, hợp nhất, rồi chuyển mọi món còn mang epoch cũ sang Ẩn.
4. Dữ liệu được coi là mới nếu đã nhận heartbeat hoặc tin từ Kho trong vòng N phút.

| Tình trạng | Món Còn | Món Hết | Món Ẩn |
|---|---|---|---|
| Online, dữ liệu mới | Cho chọn | Chặn, không cho chọn | Không hiển thị |
| Mất mạng hoặc dữ liệu cũ quá N phút | Cho chọn; màn hình hiện một dòng chung "Trạng thái món có thể đã thay đổi" | Hiện "Có thể đã hết", vẫn cho đặt | Không hiển thị |

Hai món dùng chung một nguyên liệu có thể cùng hiện Còn dù không đủ làm cả hai. Điều này được chấp nhận vì đây chỉ là gợi ý.

## 8. Luồng D: Theo dõi

Luồng D không thêm trạng thái đơn nào và không có thao tác nào của người vận hành làm đổi trạng thái đơn. Hệ thống chỉ cảnh báo, không tự hủy đơn vì chờ lâu.

| Ai xem | Hiển thị | Thao tác |
|---|---|---|
| Chi nhánh | Trạng thái kết nối; số gói đang chờ gửi | Không |
| Chi nhánh | Cảnh báo đơn ở Chờ Kho quá N phút | Nút Yêu cầu hủy ngay trên cảnh báo |
| Chi nhánh | Cảnh báo đơn trong hàng chờ gửi sắp quá hạn X giờ | Không |
| Chi nhánh | Đơn có cờ "Đang yêu cầu hủy" kèm thời gian đã chờ | Không |
| Bếp | Cảnh báo đỏ trên phiếu Chờ nấu có nguyên liệu đang bị đánh dấu cần kiểm kho | Từ chối theo Luồng A nếu hỏng thật |
| Bếp | Cảnh báo yêu cầu hủy từ chi nhánh khác chi nhánh gốc (mục 13.2) | Người vận hành kiểm tra cấu hình của chi nhánh gửi |
| Kho | Nhật ký lệnh hoàn không khớp bản ghi giữ nào | Người vận hành kiểm tra thủ công |
| Kho | Danh sách nguyên liệu đang bị đánh dấu cần kiểm kho | Gỡ đánh dấu sau khi kiểm kho xong (Luồng A) |
| Kho, Bếp | Đơn "cần kiểm tra" vì tin nằm trong `dlq` (mục 9) | Không. Bản gọn không có thao tác khép đơn (mục 15) |
| Kho, Bếp | Cảnh báo consumer chậm: tin chưa đọc cũ hơn ngưỡng ở mục 10 | Không |
| Quản lý | Mỗi chi nhánh có dòng "dữ liệu tính đến hh:mm:ss"; số đơn theo trạng thái và theo lý do hủy, từ chối, thất bại | Không |

Báo cáo của Quản lý chỉ đếm trạng thái đơn tại chi nhánh. Bản ghi Hủy chờ đơn ở Bếp không phải trạng thái cuối nên không được đếm.

## 9. Yêu cầu kỹ thuật bắt buộc

Giao tin giữa các bên là "ít nhất một lần" (at-least-once) và không đảm bảo thứ tự giữa các đường khác nhau. Mọi yêu cầu dưới đây đều xuất phát từ giả định này.

| Quy tắc | Cơ chế phải dùng |
|---|---|
| BR4, BR8, BR12 | Mã đơn do POS sinh (UUID) lúc tạo. Mọi bên nhận chống trùng theo hai lớp (idempotent consumer). Tin có `event_id` đã xử lý thì bỏ qua, không phát gì (mục 13.3). Tin có `event_id` mới nhưng mã đơn đã có bản ghi thì áp bảng trạng thái ở mục 3, tức trả lại kết quả cũ. Không chống trùng theo cặp mã đơn và loại tin: hai yêu cầu hủy của cùng một đơn từ hai chi nhánh là hai tin khác nhau |
| BR5, BR9 | Hàng chờ gửi lưu bền tại POS (outbox pattern). Giữ thứ tự theo mã đơn; mỗi gói có cờ "đã thử gửi" ghi bền trước lần gửi đầu tiên, và bộ gửi chỉ ghi cờ khi phép thử kết nối tới Kafka vừa thành công; ghi cờ và hủy cục bộ là hai thao tác ghi có điều kiện trên cùng một dòng. Lỗi gửi thì thử lại với khoảng chờ tăng dần, không bỏ gói |
| BR7 | Mọi chuyển trạng thái tại Bếp là ghi có điều kiện (compare-and-set) trên trạng thái hiện tại |
| BR3 | Chi nhánh gộp trạng thái theo hạng, không dùng "tin đến sau thì đè" |
| Luồng A, B | Hoàn nguyên liệu là giao dịch bù (saga compensation). Lệnh hoàn được Bếp lưu bền trước khi gửi |
| BR7, BR8 | Bản ghi Hủy chờ đơn là một dòng trong bảng đơn của Bếp (tombstone), không phải bảng riêng |
| BR10 | Phiên bản (epoch, seq) lưu bền; hợp nhất từng món theo phiên bản; heartbeat mang epoch |
| BR11 | Đồng hồ POS đồng bộ NTP; Kho kiểm tra cả đơn quá hạn lẫn đơn có thời điểm tạo ở tương lai |

### Những điều không được làm

- Không xóa bản ghi đơn ở Bếp hay Kho.
- Không đặt bộ đếm phiên bản trong RAM, không dùng giờ hệ thống làm phiên bản.
- Không gán thẳng snapshot vào bộ nhớ đệm (kiểu `menuCache = snapshot`).
- Không giả định Bếp nhận đơn trước yêu cầu hủy, dù POS gửi đúng thứ tự.
- Không để Bếp trả "Đã hủy" cho đơn chưa tới.
- Không tính lại lượng hoàn theo công thức hiện tại.
- Không chặn cả hàng chờ gửi vì một gói lỗi; không bỏ gói vì lỗi gửi.
- Không cộng vào tồn khi lệnh hoàn không khớp bản ghi giữ nào.
- Không hủy cục bộ đơn có gói tạo đơn đã thử gửi, dù lần gửi đó báo lỗi.
- Không coi Kafka xác nhận là Kho đã nhận đơn. Kết quả nghiệp vụ chỉ đến qua `status-events.<chi_nhanh>`.
- Không để Bếp bỏ qua tin Thất bại của đơn chưa có bản ghi; phải ghi lại kết quả.
- Không giữ nguyên liệu đang bị đánh dấu cần kiểm kho, dù tồn khả dụng còn đủ.

### Khuôn tin chung

Mọi sự kiện trên mọi topic dùng chung một khuôn. Khi đổi khuôn chỉ được thêm trường, không đổi tên hay xóa trường; khi triển khai, cập nhật bên đọc trước, bên gửi sau.

```json
{ "event_id": "uuid", "event_type": "CancelRequested", "schema_version": 1,
  "order_id": "uuid", "branch_id": "POS01", "occurred_at": "2026-11-20T09:15:00Z",
  "causation_id": "event_id của tin gây ra tin này", "payload": { } }
```

- `event_id` dùng để chống trùng (bảng `processed_events`, mục 13).
- `order_id` là khóa phân vùng và có trong mọi dòng log của mọi dịch vụ, để lọc được toàn bộ đường đi của một đơn.
- `branch_id` trong nội dung tin chỉ để tham khảo. Chi nhánh gửi thật được suy ra từ tên topic.

### Xử lý tin lỗi và sự kiện lạ

| Tình huống | Bên chỉ hiển thị (POS, Online) | Bên quyết định (Inventory, Kitchen) |
|---|---|---|
| Lỗi tạm thời (mất kết nối DB, timeout) | Thử lại mãi, khoảng chờ tăng dần | Thử lại mãi, khoảng chờ tăng dần |
| Tin không đọc được | Thử N lần, chuyển vào `dlq`, cảnh báo | Thử N lần, chuyển vào `dlq`, đánh dấu đơn "cần kiểm tra", cảnh báo |
| Tin tạo đơn đọc được nhưng dữ liệu đơn không hợp lệ | Không áp dụng | Inventory ghi kết quả Thất bại (dữ liệu đơn không hợp lệ) theo BR12, báo chi nhánh và Kitchen. Không chuyển vào `dlq` |
| `event_type` không biết | Ghi log, cảnh báo, bỏ qua (tin sau sẽ đè theo hạng, BR3) | Chuyển vào `dlq`, đánh dấu đơn "cần kiểm tra", cảnh báo. Không tự bỏ qua |
| `schema_version` cao hơn mức hỗ trợ | Như `event_type` không biết | Như `event_type` không biết |

Tin chuyển vào `dlq` không chặn partition: các đơn khác vẫn được xử lý. Đơn bị đánh dấu "cần kiểm tra" hiện trong Luồng D. Vì POS và Kho đều kiểm tra dữ liệu đơn, `dlq` chỉ nhận tin không đọc được hoặc có loại lạ, tức là chỉ xuất hiện trong kiểm thử. Bản gọn không có thao tác khép đơn: nếu một đơn thật rơi vào `dlq`, đơn nằm ở Chờ Kho (mục 15).

## 10. Danh mục lý do và tham số

Mọi lý do đều là danh sách cố định, không nhập tự do. Các giá trị ghi "đề xuất" cần nhóm xác nhận.

### Lý do

| Nhóm | Lý do | Xử lý nguyên liệu |
|---|---|---|
| Thất bại (Kho) | Thiếu nguyên liệu | Không giữ |
| Thất bại (Kho) | Nguyên liệu đang chờ kiểm kho | Không giữ |
| Thất bại (Kho) | Món chưa có công thức | Không giữ |
| Thất bại (Kho) | Đơn quá hạn | Không giữ |
| Thất bại (Kho) | Thời điểm tạo không hợp lệ (ở tương lai) | Không giữ |
| Thất bại (Kho) | Dữ liệu đơn không hợp lệ | Không giữ |
| Bếp từ chối | Hỏng thiết bị | Hoàn toàn bộ |
| Bếp từ chối | Thiếu người | Hoàn toàn bộ |
| Bếp từ chối | Nguyên liệu hỏng (bắt buộc chọn nguyên liệu) | Hao hụt nguyên liệu đó, hoàn phần còn lại, đánh dấu kiểm kho |
| Khách hủy (đề xuất) | Khách đổi ý | Hoàn toàn bộ nếu đã giữ |
| Khách hủy (đề xuất) | Khách chờ quá lâu | Hoàn toàn bộ nếu đã giữ |
| Khách hủy (đề xuất) | Thu ngân nhập sai đơn | Hoàn toàn bộ nếu đã giữ |

### Tham số

| Tham số | Giá trị | Ghi chú |
|---|---|---|
| X: ngưỡng quá hạn nhận đơn tại Kho | 4 giờ (đề xuất) | Đặt theo nghiệp vụ: đơn cũ hơn mốc này thì khách đã đi |
| Dung sai thời điểm tạo ở tương lai | 5 phút (đề xuất) | Bù lệch giờ nhỏ giữa POS và Kho |
| N: cảnh báo đơn ở Chờ Kho | 10 phút (đề xuất) | Luồng D |
| Cảnh báo đơn sắp quá hạn trong hàng chờ gửi | X trừ 1 giờ (đề xuất) | Luồng D |
| Chu kỳ heartbeat của Kho | 60 giây (đề xuất) | |
| N: dữ liệu món bị coi là cũ | 5 phút (đề xuất) | Phải lớn hơn ít nhất hai chu kỳ heartbeat |
| Thời gian Kafka giữ tin | 7 ngày | Phải dài hơn thời gian dừng lâu nhất của một consumer |
| Số lần thử với tin lỗi vĩnh viễn | 5 lần (đề xuất) | Sau đó chuyển vào dlq |
| Cảnh báo consumer chậm | Tin chưa đọc cũ hơn 5 phút (đề xuất) | Luồng D |

Ba tham số cùng tên N (cảnh báo đơn ở Chờ Kho, dữ liệu món bị coi là cũ, số lần thử với tin lỗi ở mục 9) là ba giá trị độc lập, cấu hình riêng.

## 11. Quyết định, giới hạn và phạm vi

Tám câu hỏi Q1–Q8 đã được chốt; còn năm điểm cần nhóm xác nhận trước khi code.

### Quyết định đã chốt

| # | Câu hỏi | Quyết định |
|---|---|---|
| Q1 | Có thêm trạng thái Đang nấu cho phiếu không? | Có. Đây là mốc chặn hủy và mốc Kho tính Đã dùng |
| Q2 | Có cho hủy khi đơn còn Chờ Kho không? | Có, qua bản ghi Hủy chờ đơn tại Bếp |
| Q3 | Khách Online có được tự hủy không? | Có, dùng chung luồng với thu ngân |
| Q4 | Có bắt buộc chọn lý do không? | Có, danh sách cố định. Lý do quyết định cả việc hoàn hay hao hụt |
| Q5 | Đơn hiển thị Đã hủy khi nào? | Khi Bếp chấp nhận, không chờ Kho hoàn. Với Hủy chờ đơn: khi đơn tới Bếp |
| Q6 | Đơn chờ Kho quá lâu có tự hủy không? | Không, chỉ cảnh báo kèm nút Yêu cầu hủy. Đơn quá X giờ bị Kho từ chối theo BR11 |
| Q7 | Món đã Đang nấu rồi bỏ có hoàn nguyên liệu không? | Không |
| Q8 | Món Hết thì chặn hay cảnh báo? | Chặn khi dữ liệu còn mới, cảnh báo khi dữ liệu cũ hoặc mất mạng |

### Cần nhóm xác nhận

- [ ] Giá trị X (đề xuất 4 giờ) và các tham số đề xuất ở mục 10.
- [ ] Danh sách lý do khách hủy.
- [ ] BR12 và tin "Đã dùng" từ Bếp sang Kho. Hai điểm này được thêm khi tổng hợp bản này. Nếu Kho không lưu kết quả Thất bại, một đơn bị gửi lặp sau khi Kho nhập thêm hàng sẽ được giữ và nấu dù chi nhánh đã thấy Thất bại. Nếu không có tin Đã dùng, lượng đã giữ sẽ nằm ở Đang giữ mãi.
- [ ] Hủy cục bộ đơn chưa từng thử gửi (BR9). Nếu không chấp nhận, phải bỏ câu "không bao giờ nấu" ở kịch bản 10 và coi mọi yêu cầu hủy lúc mất mạng là không chắc chắn.
- [ ] Kho từ chối giữ nguyên liệu đang bị đánh dấu cần kiểm kho (BR6). Hệ quả: Kho dùng chung, nên một lần báo hỏng chặn mọi đơn dùng nguyên liệu đó trên toàn chuỗi cho tới khi kiểm kho xong.

### Giới hạn đã biết

- **Khóa oan:** trong khoảng ngắn từ lúc Kho giữ nguyên liệu cho một đơn đã có Hủy chờ đơn tới lúc Kho nhận lệnh hoàn, một đơn khác có thể Thất bại dù thực tế còn hàng.
- **Gợi ý món không chính xác:** hai món dùng chung nguyên liệu có thể cùng hiện Còn; món sát ngưỡng có thể đổi qua lại giữa Còn và Hết.
- **Yêu cầu hủy cho đơn đã thử gửi không phải cam kết,** kể cả khi yêu cầu được ghi lúc mất mạng. Chỉ đơn chưa từng thử gửi mới hủy chắc chắn.
- **Cam kết "bếp sẽ không nấu" có điều kiện:** nó chỉ có hiệu lực khi chi nhánh yêu cầu hủy là chi nhánh gốc của đơn. Chi nhánh trung thực luôn thỏa điều kiện này.
- **Nhân viên quên bấm Bắt đầu nấu** thì món đang nấu vẫn hủy được và nguyên liệu bị hoàn sai. Xử lý bằng đào tạo và giao diện, không bằng hệ thống.

### Ngoài phạm vi v1.0

- Khôi phục Kho từ bản sao lưu: xử lý thủ công, bắt buộc kiểm kho thực tế trước khi Kho nhận đơn lại. Hệ thống chỉ đảm bảo tăng epoch và bỏ qua lệnh hoàn không khớp.
- Hủy hoặc từ chối một phần đơn.
- Thanh toán và hoàn tiền, kể cả đơn Online đã trả trước.
- Tự động hủy phiếu có nguyên liệu bị đánh dấu kiểm kho.
- Chống đổi qua lại Còn và Hết ở ngưỡng.
- Các phần hạ tầng và vận hành đã bỏ khỏi bản gọn: xem mục 15.

## 12. Kịch bản kiểm thử bắt buộc

Mỗi kịch bản phải có kiểm thử tự động. Số kịch bản giữ nguyên theo v1.3 để dễ đối chiếu; 11 kịch bản thuộc các phần đã bỏ (11, 13, 14, 17, 32, 38, 40, 41, 45, 46, 47) không còn trong bảng, còn lại 36 kịch bản từ v1.3. Bản v1.5 thêm 8 kịch bản mang số 48–55, nên bảng có 44 kịch bản.

Kịch bản 18–21 thuộc Luồng C và làm ở đợt 2; các kịch bản khác làm ở đợt 1. Có hai vế kiểm tra ở đợt 2 dù kịch bản thuộc đợt 1: vế "món sang Hết" của kịch bản 6 và vế "món về Còn" của kịch bản 48. Tin đánh dấu và gỡ đánh dấu cần kiểm kho mà Kho gửi cho Bếp (bước 6 phía Kho của Luồng C) làm ở đợt 1, vì vế cảnh báo trên phiếu của kịch bản 6 cần nó.

Trong các kịch bản, "gửi lặp" và "gửi lại" gồm hai trường hợp, và bài test phải phủ cả hai: tin tới lại với cùng `event_id` (bên nhận bỏ qua, không phát tin mới), và tin mang `event_id` mới cho một mã đơn đã có bản ghi (bên nhận trả lại kết quả cũ bằng một tin mới).

Các kịch bản 1, 3, 4, 10 và 36 nên dùng để demo; 10 và 36 đi thành một cặp để chỉ ra ranh giới bảo đảm khi hủy lúc mất mạng.

| # | Kịch bản | Kết quả mong đợi | Quy tắc |
|---|---|---|---|
| 1 | Yêu cầu hủy tới Bếp trước đơn; sau đó Kho giữ được và chuyển đơn sang Bếp | Không có phiếu; đơn Đã hủy; Kho hoàn đủ một lần | BR7 |
| 2 | Như kịch bản 1, rồi Kho gửi lại đơn lần hai | Không lập phiếu, không gửi lệnh hoàn lần hai | BR8 |
| 3 | Thu ngân bấm hủy và Bếp bấm Bắt đầu nấu cùng lúc | Đúng một thao tác thắng; trạng thái Bếp, Kho và chi nhánh khớp nhau | BR7 |
| 4 | Bếp từ chối phiếu khi Kho đang dừng, rồi Kho chạy lại | Chi nhánh thấy Đã hủy ngay; Kho hoàn đúng một lần khi chạy lại | BR4 |
| 5 | Lệnh hoàn của cùng một đơn tới Kho ba lần | Tồn tăng đúng một lần | BR12 |
| 6 | Bếp từ chối với lý do nguyên liệu hỏng (thịt) | Thịt ghi hao hụt, nguyên liệu khác được hoàn; món dùng thịt sang Hết; phiếu Chờ nấu có thịt hiện cảnh báo | BR6 |
| 7 | Tin Đã hủy tới chi nhánh trước tin Đã giữ | Chi nhánh giữ Đã hủy, bỏ qua Đã giữ | BR3 |
| 8 | Đơn có Hủy chờ đơn bị Kho từ chối | Chi nhánh thấy Thất bại, cờ hủy tắt; bản ghi Bếp sang Thất bại | BR7 |
| 9 | Kho trả Thất bại vì thiếu hàng, nhập thêm hàng, rồi đơn bị gửi lặp | Kho trả lại Thất bại, không giữ | BR12 |
| 10 | POS mất mạng nên bộ gửi không ghi cờ; thu ngân tạo đơn rồi hủy khi gói tạo đơn chưa từng thử gửi, sau đó có mạng lại | POS khép Đã hủy ngay, bỏ gói, không gửi gì; Kho và Bếp không có bản ghi; đơn không bao giờ được nấu | BR5, BR9 |
| 12 | Mạng chập chờn lâu: các gói đã thử gửi gặp nhiều lần gửi thất bại | Không gói nào bị bỏ; khi có mạng lại mọi gói được gửi, đúng thứ tự theo từng mã đơn | BR9 |
| 15 | POS mất mạng lâu hơn X rồi gửi đơn cũ | Kho trả Thất bại (đơn quá hạn), không giữ nguyên liệu | BR11 |
| 16 | Đơn có thời điểm tạo ở tương lai 1 giờ | Kho trả Thất bại | BR11 |
| 18 | Kho khởi động lại, sau đó một món đổi trạng thái | Phiên bản tiếp tục tăng; POS nhận cập nhật ngay | BR10 |
| 19 | POS nhận tin Hết (v6) rồi snapshot có Còn (v5) tới sau | POS giữ Hết (v6) | BR10 |
| 20 | Kho khôi phục từ bản sao lưu và tăng epoch; món A đang Hết ở đời cũ, món B không có trong snapshot mới | A theo snapshot mới; B chuyển Ẩn | BR10 |
| 21 | Kho không đổi tồn trong 10 phút, heartbeat đều | POS không hiện "Có thể đã hết" | BR10 |
| 22 | Kho nhận lệnh hoàn cho đơn không có bản ghi giữ | Tồn không đổi, có dòng nhật ký | BR12 |
| 23 | Yêu cầu hủy tới khi phiếu Đang nấu | Chi nhánh thấy "Không hủy được: bếp đã bắt đầu nấu", trạng thái giữ nguyên | BR7 |
| 24 | Inventory giữ được một đơn của POS01 | Kitchen nhận đơn qua kitchen-orders; POS01 nhận Đã giữ qua status-events của mình; chi nhánh khác không nhận gì | Mục 13 |
| 25 | Tắt Kitchen 2 giờ trong khi vẫn có đơn và yêu cầu hủy, rồi bật lại | Kitchen xử lý đủ mọi tin còn trong Kafka, không mất, không trùng | Mục 13 |
| 26 | Dừng tiến trình consumer sau khi commit DB nhưng trước khi commit offset | Kafka gửi lại tin; processed_events chặn bản trùng, không có hiệu ứng thứ hai | BR4 |
| 27 | POS01 gửi yêu cầu hủy cho đơn của POS02 đã có phiếu ở Bếp | Kitchen bỏ qua yêu cầu và cảnh báo; đơn của POS02 không đổi trạng thái | Mục 13.2 |
| 28 | Chạy hai instance Kitchen; cả hai cùng lúc nhận đơn và yêu cầu hủy của một mã đơn | Chỉ có một bản ghi; kết quả khớp bảng 3.2 dù tin nào đến trước | BR7 |
| 29 | Inventory nhận tin có event_type lạ | Tin vào dlq, đơn được đánh dấu cần kiểm tra, các đơn khác vẫn được xử lý | Mục 9 |
| 30 | Kho thiếu một nguyên liệu khi giữ đơn, rồi tin đơn đó bị gửi lại | Tồn không đổi; bản ghi Thất bại, dòng processed_events và tin Failed đều được lưu; lần gửi lại nhận đúng kết quả cũ | BR12 |
| 31 | Tắt một broker Kafka trong lúc các chi nhánh đang gửi đơn | Không mất tin nào đã được Kafka xác nhận; việc gửi vẫn tiếp tục | Mục 13 |
| 33 | Kho báo Thất bại tới Bếp trước, yêu cầu hủy của đơn đó tới sau | Bếp có bản ghi Thất bại; không tạo Hủy chờ đơn; chi nhánh thấy Thất bại | BR8 |
| 34 | POS02 gửi yêu cầu hủy cho mã đơn của POS01 trước khi đơn tới Bếp | Đơn của POS01 vẫn được lập phiếu Chờ nấu; yêu cầu của POS02 bị bác; có cảnh báo | Mục 13.2 |
| 35 | Như kịch bản 34, nhưng POS01 cũng gửi yêu cầu hủy trước khi đơn tới | Đơn Đã hủy; Kho hoàn đúng một lần | BR7 |
| 36 | POS gửi đơn rồi mất mạng, thu ngân bấm hủy; khi có mạng lại, Bếp đã bấm Bắt đầu nấu trước khi nhận yêu cầu hủy | Chi nhánh thấy "Không hủy được: bếp đã bắt đầu nấu"; đơn đi tiếp tới Hoàn tất; nguyên liệu không bị hoàn | BR5, BR7 |
| 37 | Thu ngân bấm hủy đúng lúc bộ gửi lấy gói tạo đơn chưa từng thử gửi | Đúng một bên thắng: hoặc đơn bị hủy cục bộ và không gói nào được gửi, hoặc gói được gửi và yêu cầu hủy đi qua Bếp | BR9 |
| 39 | Inventory nhận đơn đọc được nhưng có số lượng âm | Kho ghi bản ghi Thất bại (dữ liệu đơn không hợp lệ), không giữ; chi nhánh thấy Thất bại; tin không vào dlq | BR12 |
| 42 | Hai đơn tới cùng lúc ở hai instance Inventory, cùng cần một nguyên liệu, tổng nhu cầu vượt tồn khả dụng | Đúng một đơn Đã giữ, đơn kia Thất bại; tồn khả dụng không âm; không đơn nào được giữ một phần | BR1 |
| 43 | Kho có 10 kg thịt; một đơn giữ 2 kg rồi Bếp bấm Bắt đầu nấu | Tồn khả dụng là 8 kg trước và sau khi nấu; đang giữ về 0; đã dùng tăng 2 kg | Mục 3.3 |
| 44 | Thịt đang bị đánh dấu cần kiểm kho, tồn khả dụng còn đủ; POS mất mạng vẫn nhận đơn có món dùng thịt rồi gửi đi | Kho trả Thất bại (nguyên liệu đang chờ kiểm kho); không nguyên liệu nào của đơn bị giữ | BR6 |
| 48 | Thịt đang bị đánh dấu cần kiểm kho, một phiếu Chờ nấu có thịt đang hiện cảnh báo; Kho xác nhận kiểm kho xong | Cờ được gỡ; cảnh báo trên phiếu mất; đơn mới dùng thịt được giữ; món dùng thịt về Còn | BR6 |
| 49 | Một đơn giữ 2 kg thịt; công thức của món đổi thành 3 kg; rồi Bếp hủy hoặc từ chối đơn | Kho hoàn đúng 2 kg đã ghi lúc giữ; tồn khả dụng trở lại như trước khi giữ | BR6 |
| 50 | Nhân viên bấm Từ chối trên phiếu đã Đang nấu | Thao tác bị từ chối; phiếu vẫn Đang nấu; Kho không nhận lệnh hoàn nào | Luồng A |
| 51 | Nhân viên bấm Từ chối với lý do nguyên liệu hỏng đúng lúc yêu cầu hủy của chi nhánh gốc tới phiếu Chờ nấu | Đúng một thao tác thắng; Kho xử lý đúng một lệnh hoàn theo lý do của bên thắng: hoặc hoàn toàn bộ, hoặc hao hụt và đánh dấu kiểm kho | BR6, BR7 |
| 52 | Hai nhân viên bấm Từ chối và Bắt đầu nấu trên cùng một phiếu Chờ nấu cùng lúc | Đúng một thao tác thắng; lần giữ rời Đang giữ đúng một lần, tới đã dùng hoặc tới hoàn | BR7, BR12 |
| 53 | Kho đang dừng; một đơn nằm ở Chờ Kho quá N phút | Chi nhánh hiện cảnh báo kèm nút Yêu cầu hủy; đơn không tự đổi trạng thái | Luồng D |
| 54 | POS mất mạng; một đơn trong hàng chờ gửi còn chưa tới 1 giờ là quá hạn X | Chi nhánh hiện mất kết nối, số gói đang chờ gửi và cảnh báo đơn sắp quá hạn | Luồng D |
| 55 | Ngắt nhân bản từ POS01 về Central Order Replica trong khi POS01 và POS02 vẫn nhận đơn, rồi nối lại | Trong lúc ngắt, báo cáo vẫn xem được, mốc "dữ liệu tính đến" của POS01 dừng lại còn của POS02 vẫn chạy; khi nối lại, số liệu của POS01 khớp với DB của chi nhánh | Luồng D, mục 13.5 |

## 13. Giả định triển khai

Các quy tắc BR1–BR12 chỉ đúng khi hệ thống được triển khai đúng các giả định dưới đây. Kiến trúc bản gọn: Kafka 3 broker, PostgreSQL cho mọi DB, Inventory 2 instance, Kitchen và Online mỗi bên 1 instance (mục 13.6).

### 13.1 Topic Kafka

| Topic | Ai ghi | Ai đọc | Khóa phân vùng |
|---|---|---|---|
| `order-events.<chi_nhanh>` | Chi nhánh đó | Inventory (đăng ký theo mẫu `order-events.*`) | Mã đơn |
| `cancel-events.<chi_nhanh>` | Chi nhánh đó | Kitchen (đăng ký theo mẫu `cancel-events.*`) | Mã đơn |
| `status-events.<chi_nhanh>` | Inventory, Kitchen | Chi nhánh đó | Mã đơn |
| `kitchen-orders` | Inventory | Kitchen | Mã đơn |
| `stock-commands` | Kitchen | Inventory | Mã đơn |
| `menu-events` | Inventory | Mọi chi nhánh, Kitchen | Mã món hoặc mã nguyên liệu |
| `dlq` | Mọi consumer | Người vận hành | Mã đơn |

Khi xử lý xong một đơn, Inventory ghi tin vào cả `kitchen-orders` và `status-events.<chi_nhanh>` qua outbox, trong cùng một giao dịch.

Việc mỗi chi nhánh chỉ ghi và đọc topic của mình là quy ước trong mã nguồn, không được broker ép buộc (mục 13.2).

Cấu hình bắt buộc:

- Producer (bộ đẩy outbox): `acks=all`, `enable.idempotence=true`. Một dòng outbox chỉ được đánh dấu "đã gửi" khi Kafka đã xác nhận.
- Topic: `replication.factor=3`, `min.insync.replicas=2`, `retention.ms` = 7 ngày.
- Broker: `unclean.leader.election.enable=false`.

### 13.2 Chi nhánh gốc của đơn

- Kitchen suy ra chi nhánh gửi yêu cầu hủy từ tên topic, rồi so với chi nhánh gốc của đơn (Inventory chuyển kèm qua `kitchen-orders`, kể cả trong tin Thất bại). Lệch nhau thì bỏ qua yêu cầu hủy và cảnh báo.
- Bản ghi Hủy chờ đơn lưu mọi chi nhánh đã gửi yêu cầu hủy, không chỉ chi nhánh đầu tiên; nếu chỉ lưu một, yêu cầu sai chi nhánh tới trước sẽ nuốt mất yêu cầu hợp lệ tới sau. Khi đơn tới: chi nhánh gốc có trong danh sách thì đơn Đã hủy; không có thì Kitchen bác các yêu cầu đã ghi, lập phiếu bình thường và cảnh báo (bảng 3.2).
- Bản gọn không xác thực và không phân quyền trên Kafka. Kiểm tra này chặn hủy chéo do lỗi cấu hình hoặc bug; nó không chống được một bên cố ý ghi vào topic của chi nhánh khác (mục 15).

### 13.3 Consumer xử lý mỗi tin

Tắt tự động commit offset. Mỗi tin được xử lý đúng thứ tự:

1. Mở giao dịch DB.
2. Chèn `event_id` vào `processed_events`. Mã đã có thì tin là trùng: rollback và nhảy tới bước 5.
3. Áp bảng chuyển trạng thái ở mục 3.
4. Ghi outbox, rồi commit.
5. Commit offset Kafka.

Nếu tiến trình chết giữa bước 4 và bước 5, Kafka gửi lại tin và bước 2 chặn bản trùng. Bản gọn không dọn bảng `processed_events`. Lỗi trong lúc xử lý đi theo bảng "Xử lý tin lỗi và sự kiện lạ" ở mục 9.

### 13.4 Ghi DB khi có tranh chấp

**Kitchen:** mọi tin liên quan một mã đơn, từ bất kỳ topic nào, và mọi thao tác của nhân viên trên phiếu đều xử lý bằng một giao dịch có khóa dòng. Luồng xử lý nào giành được khóa trước thì chạy trước, dù cùng instance hay khác instance; đây là cách DB bảo đảm BR7.

```sql
INSERT INTO kitchen_orders (order_id, status)
VALUES (:id, 'CHUA_CO') ON CONFLICT (order_id) DO NOTHING;
SELECT status FROM kitchen_orders WHERE order_id = :id FOR UPDATE;
-- áp bảng 3.2 → UPDATE status → ghi outbox → COMMIT
```

Dòng `CHUA_CO` chỉ là chỗ đặt khóa: bảng 3.2 coi nó là "(chưa có)" và nó không phải bản ghi theo BR8. Chi nhánh gốc (`origin_branch`) chỉ được ghi từ tin của Kho; chi nhánh gửi yêu cầu hủy ghi vào danh sách riêng (`cancel_branches`), không ghi chung một cột.

**Inventory:** dùng savepoint để khi thiếu nguyên liệu chỉ hoàn lại phần trừ tồn, còn kết quả Thất bại và dòng chống trùng vẫn được ghi (BR12).

```sql
BEGIN;
INSERT INTO processed_events ...;
INSERT INTO inventory_orders (order_id, ...) ...;
SAVEPOINT reserve;
UPDATE stock SET available = available - :q   -- từng nguyên liệu, mã tăng dần
 WHERE ingredient_id = :i AND available >= :q AND NOT needs_check;
-- nếu có dòng không trừ được:
ROLLBACK TO SAVEPOINT reserve;
UPDATE inventory_orders SET status = 'THAT_BAI', reason = :ly_do ...;
INSERT INTO outbox ...;   -- Failed cho kitchen-orders và status-events.<chi_nhanh>
COMMIT;
```

- Luôn khóa các nguyên liệu theo thứ tự mã tăng dần để hai đơn không khóa chéo nhau.
- Điều kiện `NOT needs_check` nằm ngay trong câu UPDATE, nên kiểm tra cờ và giữ là một thao tác. Lệnh đánh dấu cần kiểm kho khóa cùng dòng tồn kho, nên không có khoảng hở giữa đánh dấu và giữ. Lý do Thất bại là "Nguyên liệu đang chờ kiểm kho" nếu dòng không trừ được vì cờ, ngược lại là "Thiếu nguyên liệu".
- Giữ giao dịch ngắn, không gọi dịch vụ ngoài trong lúc giữ khóa.

### 13.5 Cơ sở dữ liệu

- Inventory DB và Kitchen DB mỗi cái là một instance PostgreSQL, không có replica. Kho và Bếp ưu tiên nhất quán: mọi thay đổi trạng thái và tin gửi đi nằm trong cùng một giao dịch (outbox).
- Kho hoặc Bếp dừng thì tin chờ trong Kafka và được xử lý khi dịch vụ chạy lại (kịch bản 4, 25, 26). Riêng đơn mới vẫn chịu hạn X giờ: Kho dừng lâu hơn X thì các đơn đã chờ quá hạn nhận Thất bại (đơn quá hạn) khi Kho chạy lại (BR11); lệnh hoàn và báo Đã dùng thì không có hạn. Mất hẳn dữ liệu của một DB là ngoài phạm vi bản gọn (mục 15).
- Các DB đơn hàng của chi nhánh nhân bản bất đồng bộ về Central Order Replica để phục vụ báo cáo; đây là phần nhân bản dữ liệu của hệ thống.

### 13.6 Quy mô triển khai

Mỗi thành phần trên sơ đồ có mặt để trả lời một vấn đề cụ thể.

| Thành phần | Số lượng | Phục vụ vấn đề nào |
|---|---|---|
| Kafka | 3 broker | Tin đã xác nhận không mất khi một broker chết (kịch bản 31); đơn và lệnh hoàn chờ được khi Kho hoặc Bếp dừng (kịch bản 4, 25) |
| Local Order Service và DB | 1 mỗi POS site | Chi nhánh mất mạng vẫn ghi nhận đơn và gửi lại khi có mạng (BR5, kịch bản 10, 36) |
| Online Order Service | 1 instance | Nguồn đơn thứ hai, độc lập với POS, dùng chung luồng |
| Inventory Service | 2 instance | Hai đơn tranh cùng một nguyên liệu ở hai instance; khóa dòng trong DB phân xử (BR1, kịch bản 42) |
| Kitchen Service | 1 instance; 2 khi chạy kịch bản 28 | Tranh chấp Hủy và Bắt đầu nấu có ngay trong một instance: tin hủy đến qua consumer, nút bấm đến qua HTTP (BR7, kịch bản 3) |
| Inventory DB, Kitchen DB | 1 mỗi dịch vụ, không replica | Nguồn sự thật duy nhất cho tồn kho và phiếu; nhất quán nhờ giao dịch và khóa dòng |
| Central Order Replica | 1 | Nhân bản bất đồng bộ; báo cáo toàn chuỗi không đọc trực tiếp DB của chi nhánh |

## 14. Bảo đảm và giới hạn

Hệ thống chỉ hứa những điều dưới đây, và chỉ khi điều kiện đi kèm còn đúng. Mọi thứ nằm ngoài danh sách là không được bảo đảm.

### Hệ thống bảo đảm

| Bảo đảm | Với điều kiện |
|---|---|
| Không giữ hoặc hoàn nguyên liệu hai lần cho một đơn | Inventory DB không mất dữ liệu đã commit |
| Đơn đã có bản ghi Hủy chờ đơn không bao giờ bị nấu | Kitchen DB không mất dữ liệu đã commit; yêu cầu hủy do chi nhánh gốc của đơn gửi |
| Đơn bị hủy khi gói tạo đơn chưa từng thử gửi không bao giờ bị nấu | DB cục bộ của POS không mất dữ liệu đã commit |
| Đơn đã Thất bại không bao giờ được xét lại | Inventory DB không mất dữ liệu đã commit |
| Nguyên liệu đang bị đánh dấu cần kiểm kho không được giữ cho đơn mới | Inventory DB không mất dữ liệu đã commit |
| Lần giữ còn ở Đang giữ luôn có bản ghi để hoàn hoặc tính đã dùng | Inventory DB không mất dữ liệu đã commit |
| Không mất tin đã được Kafka xác nhận | Chết tối đa 1 trong 3 broker |
| Mọi tin cuối cùng được xử lý đúng một lần về mặt hiệu ứng | Consumer không dừng lâu hơn thời gian Kafka giữ tin |
| Trạng thái đơn trên chi nhánh không đi lùi | Luôn đúng (BR3) |

### Hệ thống không bảo đảm

- Yêu cầu hủy cho đơn đã thử gửi chắc chắn thành công, kể cả khi yêu cầu được ghi lúc mất mạng.
- Kho nhận mọi đơn mà Kafka đã xác nhận. Kafka xác nhận chỉ có nghĩa là gói đã vào hàng đợi; kết quả nghiệp vụ chỉ đến qua tin trạng thái.
- Trạng thái món trên POS luôn chính xác. Trạng thái món chỉ là gợi ý: Kho dùng chung nên một chi nhánh khác có thể vừa đặt hết phần nguyên liệu cuối, và hai món dùng chung nguyên liệu có thể cùng hiện Còn.
- Đơn được xử lý nếu consumer dừng lâu hơn thời gian Kafka giữ tin. Trường hợp này xử lý thủ công.
- Dữ liệu còn nguyên khi mất Inventory DB hoặc Kitchen DB. Bản gọn không có replica cho hai DB này.
- Chi nhánh không gửi được tin thay chi nhánh khác. Bản gọn không xác thực; Kitchen chỉ so chi nhánh gốc để chặn nhầm lẫn.
- Đơn có tin rơi vào `dlq`, hoặc lần giữ có lệnh hoàn bị mất, được tự khép. Bản gọn không có công cụ đối soát (mục 15).
- Thứ tự giữa hai topic khác nhau, kể cả với cùng một mã đơn.
- Thông lượng cao khi nhiều đơn cùng dùng một nguyên liệu phổ biến (đá, sữa, thịt): các đơn này phải chờ khóa của nhau trên cùng dòng tồn kho. Đây là điểm nghẽn đầu tiên khi tải cao.

## 15. Giới hạn và hướng mở rộng lên production

Các phần dưới đây có trong bản đầy đủ v1.3 nhưng không được hiện thực trong đồ án. Mỗi dòng nêu hệ quả phải chấp nhận trong bản gọn và cách bản production xử lý; số mục trong cột cuối là số mục của v1.3.

| Phần đã bỏ | Hệ quả trong bản gọn | Hướng mở rộng (theo v1.3) |
|---|---|---|
| Replica đồng bộ cho Inventory DB và Kitchen DB | DB dừng thì dịch vụ dừng, tin chờ trong Kafka. Mất hẳn DB là mất trạng thái tồn kho hoặc phiếu | 1 primary và 1 replica đồng bộ; chặn primary cũ trước khi chuyển; tăng epoch nếu chuyển lúc đang chạy bất đồng bộ (mục 13.5) |
| Xác thực và phân quyền: mTLS hoặc SASL, Kafka ACL, kiểm tra tài khoản khách Online | Tiến trình nào cũng ghi được mọi topic. Kiểm tra chi nhánh gốc chỉ chặn nhầm lẫn, không chặn giả mạo | Mỗi chi nhánh một danh tính Kafka, ACL theo topic của chi nhánh; Online kiểm tra đơn thuộc tài khoản khách (mục 13.2) |
| Hàng cô lập cho gói bị broker từ chối | Gói bị broker từ chối được thử lại mãi và nằm trong số gói chờ gửi; POS không tự khép đơn đó | Hàng cô lập, quy tắc "chắc chắn chưa vào Kafka" dựa trên các lần gửi chưa rõ kết quả, thao tác bỏ gói (BR9) |
| Dọn bản ghi theo TTL | Bảng đơn ở Bếp, ở Kho và bảng `processed_events` lớn dần theo thời gian | TTL 7 ngày, chỉ dọn bản ghi không còn việc dở; Bếp từ chối đơn và yêu cầu hủy cũ hơn TTL (BR8, mục 10) |
| Đối soát lần giữ quá G giờ | Lệnh hoàn hoặc báo Đã dùng bị mất thì lần giữ nằm ở Đang giữ. Các bất biến ở mục 3.3 vẫn đúng nên không báo gì; chỉ thấy được khi đối chiếu thủ công bản ghi Đang giữ của Kho với trạng thái phiếu ở Bếp | Cảnh báo quá G giờ và thao tác Đối soát với Bếp để phát lại lệnh hoàn hoặc báo Đã dùng (mục 8) |
| Khép đơn có tin trong `dlq` | Đơn thật có tin rơi vào `dlq` nằm ở Chờ Kho mãi. Thu ngân vẫn gửi được yêu cầu hủy và nhận cam kết "bếp sẽ không nấu", nhưng đơn không tới được trạng thái cuối | Thao tác Khép đơn tại Kho: ghi Thất bại, báo chi nhánh và Bếp (mục 8) |
| Nhiều instance cho Kitchen và Online | Kitchen hoặc Online dừng thì phần việc của nó dừng cho tới khi chạy lại; tin không mất | Chạy 2 instance. Mã nguồn đã dùng khóa dòng và chống trùng nên không phải sửa logic (kịch bản 28) |
