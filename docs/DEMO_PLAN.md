# Kịch bản demo và nghiệm thu hệ thống phân tán

Ngày lập: 30/10/2026. Phiên bản: 0.1. Căn cứ thiết kế: [PROJECT_SPEC.md](PROJECT_SPEC.md).

Tài liệu này mô tả cách kiểm chứng các cơ chế phân tán của đồ án. Mọi kịch bản hiện đều **chưa chạy** vì backend, Kafka và replication chưa được triển khai. Những tên container, script hoặc endpoint là contract dự kiến; chỉ dùng như lệnh thực thi sau khi đã tạo cấu hình tương ứng.

## 1. Mục tiêu và thứ tự ưu tiên

Ba vấn đề lõi là tranh nguyên liệu, event mất/trùng và POS mất kết nối trung tâm. Replication và tái thiết các mảnh là phần kiến trúc nền cần có bằng chứng riêng. Kiểm tra một broker dừng và hai instance xử lý là cách chứng minh các tuyên bố về cấu hình phân tán.

| Mức | Kịch bản |
|---|---|
| Lõi nghiệp vụ phân tán | D01 luồng thường; D02 thiếu nguyên liệu; D03 tranh tồn; D04/D05 event trùng; D06 Kafka ngừng; D07 cửa sổ lỗi outbox; D08 consumer chết sau commit; D09 POS offline |
| Kiến trúc nền | D10 lỗi một broker; D11 replication; D12 nhiều instance và trạng thái về đúng nguồn |
| Kiểm tra bổ trợ | D13 event muộn và HTTP retry |
| Có điều kiện | D14 standby DB nếu nhóm duyệt; D15 Kitchen lỗi và hoàn hàng nếu đã triển khai mở rộng |

Không cần trình diễn mọi bài thử trước lớp nếu thời gian ngắn, nhưng những cơ chế đã tuyên bố phải có kết quả kiểm tra lưu lại. Các bài thử bổ trợ có thể thực hiện trước và trình bày bằng bảng kết quả cùng dữ liệu đối chứng.

## 2. Dữ liệu mẫu

### 2.1. Công thức

| Mã món | Tên | Gạo | Thịt gà |
|---|---|---|---|
| `COM_GA` | Cơm gà | 100 gram | 150 gram |
| `CHAO_GA` | Cháo gà | 50 gram | 100 gram |

Hai món có chung nguyên liệu để minh họa Kho quyết định theo nguyên liệu, không chỉ theo số món. Menu các nguồn có thể khác nhau, nhưng các món được dùng trong từng bài thử phải tồn tại ở nguồn đó và có công thức tương ứng ở Inventory.

### 2.2. Bộ tồn kho

| Bộ | Gạo | Thịt gà | Dùng cho |
|---|---|---|---|
| `NORMAL` | 1.000 gram | 1.500 gram | Luồng thường và phần lớn bài thử |
| `LAST_PORTION` | 1.000 gram | 150 gram | Hai đơn COM_GA tranh phần gà cuối |
| `MISSING_RICE` | 50 gram | 1.500 gram | Một nguyên liệu thiếu, nguyên liệu khác không bị trừ một phần |
| `OFFLINE_LIMITED` | 1.000 gram | 150 gram | Hai đơn offline nối lại, một đơn được chấp nhận |

Mỗi bài thử bắt đầu bằng seed/reset riêng khi worker và consumer đã quiesce hoặc toàn môi trường demo được khởi tạo lại có kiểm soát. Không chỉ reset tồn khi event cũ còn trong Kafka/outbox. Không đổi tồn thủ công giữa lúc consumer đang xử lý rồi dùng kết quả làm bằng chứng thuật toán.

Đề xuất script reset dừng các service liên quan, dựng lại dữ liệu thử và topic/group/replication phù hợp rồi mới chạy tiếp. Việc xóa dữ liệu hoặc volume phải là một hành động reset riêng, không trộn vào các bước thử lỗi. Khi có script, phải giới hạn rõ vào tài nguyên của dự án này.

## 3. Chuẩn bị một lượt kiểm tra

1. Ghi version code, cấu hình, ngày giờ và người chạy. Nếu code chưa commit thì ghi nhận trạng thái đó.
2. Ghi topology thật: số instance service, các DB/standby, broker và partition.
3. Kiểm tra cụm Kafka đủ ISR/quorum, topic replication factor và min ISR đúng, consumer group ổn định.
4. Seed bộ dữ liệu và chụp tồn ban đầu; kiểm tra outbox/event cũ không làm nhiễu bài thử.
5. Khi kiểm tra trung tâm, chờ initial sync của các subscription hoàn tất.
6. Mở POS01, Online, Inventory, Kitchen và Dashboard cần thiết. Không có bước login.
7. Tạo mã nhận diện lượt thử; lưu danh sách orderId/eventId và timestamps.
8. Thu bằng chứng API, DB và log. Dùng thời gian chờ có giới hạn; nếu chưa hội tụ thì ghi fail/chưa kết luận và chẩn đoán, không chờ vô hạn.

Các nguồn bằng chứng chính: orders/order_items ở DB nguồn và trung tâm; nguyên liệu và reservation tại Inventory; ticket tại Kitchen; outbox; processed_events; topic/partition/offset và consumer group. Screenshot UI giúp trình bày nhưng không thay thế đối chứng dữ liệu.

## 4. Chi tiết kịch bản

### D01. Luồng bình thường từ POS và Online

**Chứng minh:** ownership, workflow Inventory trước Kitchen và trạng thái trả về nguồn.

- Chuẩn bị `NORMAL`. Tạo một COM_GA tại POS01 và một CHAO_GA tại Online, mỗi món số lượng 1.
- Kiểm tra mỗi đơn được lưu tại đúng DB nguồn cùng một outbox OrderCreated.
- Chờ cả hai được Reserved; tồn còn gạo 850 gram và gà 1.250 gram.
- Kiểm tra mỗi đơn có đúng một quyết định Reserved và một ticket WAITING.
- Hoàn tất hai ticket; chờ hai nguồn hiển thị Done. Tồn không giảm thêm sau khi hoàn tất.
- Kiểm tra trung tâm cuối cùng có cả hai đơn và đủ dòng món.

**Đạt khi:** các giá trị và số hàng đúng, không service nào cần đọc trực tiếp DB service khác. Ghi riêng độ trễ tới Reserved và độ trễ replication; không tính thời gian người demo bấm Done vào thời gian tự động.

### D02. Thiếu một nguyên liệu không làm trừ phần còn lại

**Chứng minh:** atomicity của quyết định giữ hàng nhiều nguyên liệu.

- Chuẩn bị `MISSING_RICE`. Tạo một COM_GA tại POS01.
- Chờ Failed với lý do thiếu nguyên liệu.
- Kiểm tra gạo vẫn 50 gram, gà vẫn 1.500 gram, không có reservation thành công và không có ticket Kitchen.
- Kiểm tra quyết định Failed và outbox Failed được commit, trạng thái về đúng nguồn.

**Đạt khi:** không nguyên liệu nào bị trừ một phần, không lặp retry vô hạn cho lỗi nghiệp vụ.

### D03. Hai nguồn tranh nguyên liệu cuối

**Chứng minh:** xử lý cạnh tranh ở DB, kể cả khi có hai Inventory instance.

- Chuẩn bị `LAST_PORTION`, chạy hai Inventory instance cùng group và cùng Inventory DB.
- Gửi đồng thời hai request COM_GA ×1: một từ POS01, một từ Online; dùng request ID khác nhau.
- Khi chọn dữ liệu thử cho nhiều instance, kiểm tra hai orderId có thể rơi vào hai partition do các instance khác nhau xử lý. Lưu partition/instance và bằng chứng transaction xử lý chồng thời gian; nếu chưa có, chạy lại lượt khác để kiểm tra cạnh tranh thật.
- Chờ kết quả: một Reserved, một Failed. Không yêu cầu nguồn cụ thể luôn thắng.
- Kiểm tra gà còn 0, gạo còn 900; đúng một bộ reservation và một ticket cho đơn thắng.

**Đạt khi:** không có hai Reserved và không có tồn âm. Một lượt hai event xử lý nối tiếp chỉ chứng minh kết quả nghiệp vụ, chưa đủ để khẳng định đã thử hai instance cạnh tranh; ghi rõ mức bằng chứng và lặp bằng script đồng thời nếu cần.

### D04. Cùng event được giao nhiều lần

**Chứng minh:** dedup theo event ID.

- Chuẩn bị `NORMAL`. Tạo COM_GA ×1 và chờ Reserved.
- Lưu nguyên payload OrderCreated đã phát. Phát lại nguyên payload ba lần, giữ eventId và Kafka key.
- Chờ consumer xử lý hết. Kiểm tra gạo còn 900, gà còn 1.350.
- Kiểm tra một quyết định theo orderId, một bộ reservation và một phiếu bếp. Log thể hiện duplicate nhưng DB không thêm tác dụng nghiệp vụ.
- Có thể phát lại Reserved để kiểm tra Kitchen không tạo thêm ticket.

**Đạt khi:** số lần phát event tăng nhưng lượng giữ hàng và số ticket không đổi.

### D05. Event ID khác nhưng vẫn cùng mã đơn

**Chứng minh:** idempotency theo nghiệp vụ, không chỉ theo envelope.

- Từ dữ liệu một đơn Reserved, phát lại OrderCreated với eventId mới nhưng cùng orderId, nguồn và nội dung dòng món.
- Kiểm tra unique orderId ngăn giữ lần nữa; tồn và ticket không đổi.
- Thử payload khác nhưng cùng orderId trong một lượt riêng; hệ thống phải log xung đột, không coi đó là đơn mới để trừ thêm.

**Đạt khi:** cả duplicate kỹ thuật và duplicate theo nghiệp vụ không làm tăng tác dụng phụ. Không dùng bài thử payload xung đột để tuyên bố đã có tính năng sửa đơn.

### D06. Kafka tạm ngừng sau khi nguồn vẫn có DB

**Chứng minh:** outbox tách việc nhận đơn khỏi middleware.

- Chuẩn bị `NORMAL`. Dừng cả cụm Kafka, giữ Order Service và PostgreSQL nguồn hoạt động.
- Tạo một COM_GA tại POS01. API trả thành công sau commit, trạng thái Pending.
- Kiểm tra DB có đơn, dòng món và outbox chưa gửi. Chưa có quyết định Inventory hay phiếu Kitchen.
- Khởi động lại Kafka, chờ ISR/quorum và consumer phục hồi.
- Kiểm tra outbox được gửi, đơn Reserved và chỉ trừ một lần.

**Đạt khi:** đơn không mất khi middleware ngừng, API không bắt buộc Kafka khỏe để lưu local. Bài thử này là Kafka outage, không thay thế bài thử cô lập mạng riêng POS.

### D07. Worker chết sau Kafka ack nhưng trước đánh dấu đã gửi

**Chứng minh:** hiểu và xử lý cửa sổ duplicate của outbox.

- Cần failpoint dành riêng môi trường demo: sau khi nhận producer ack nhưng trước commit `sent_at`, dừng process hoặc chặn bằng breakpoint rồi kill.
- Tạo một COM_GA; thu bằng chứng Kafka đã nhận event và Inventory đã hoặc có thể xử lý.
- Worker bị dừng tại vị trí đã định. Kiểm tra outbox chưa có sent_at bền vững.
- Khởi động lại service; worker gửi lại cùng event ID. Kiểm tra tồn chỉ giảm một lần và không thêm ticket.

**Đạt khi:** quan sát được phát lại nhưng không tác dụng lặp. Nếu chưa có failpoint/breakpoint đáng tin thì ghi chưa thử, không thay bằng thao tác kill ngẫu nhiên rồi khẳng định đã trúng cửa sổ lỗi.

### D08. Consumer chết sau commit DB nhưng trước commit offset

**Chứng minh:** tác dụng của transaction local và dedup khi Kafka replay.

- Cần failpoint sau khi transaction Inventory commit quyết định/outbox, trước ack offset.
- Xử lý một đơn, dừng process tại failpoint, kiểm tra DB đã có quyết định và tồn đã giảm.
- Khởi động lại consumer hoặc cho instance khác nhận partition khi rebalance.
- Xác nhận event được giao lại và consumer kết thúc thành công mà không giữ hàng lần hai.

**Đạt khi:** DB đúng và có bằng chứng replay. Nếu offset đã commit trước lúc kill thì lượt đó không kiểm chứng tình huống này.

### D09. POS01 bị cô lập khỏi trung tâm

**Chứng minh:** phương án A, nhận local và chờ Kho xác nhận.

- Chuẩn bị `OFFLINE_LIMITED`. Giữ UI/service/DB local POS01 hoạt động, ngắt kết nối site này với cả Kafka và subscriber trung tâm. POS02/Online vẫn có kết nối.
- Tạo hai đơn COM_GA ×1 tại POS01. Cả hai lưu Pending và có outbox, chưa có ticket.
- Nếu muốn chứng minh UI tải lại offline, dùng frontend được phục vụ local rồi tải lại; ghi rõ nếu chỉ thử trên trang đã mở.
- Kiểm tra dữ liệu mới POS01 chưa xuất hiện ở trung tâm; POS02/Online vẫn tạo được đơn nếu không tiêu thụ mất bộ nguyên liệu đang dành cho lượt thử này.
- Nối lại POS01. Chờ hai đơn hội tụ: một Reserved, một Failed; chỉ đơn Reserved có ticket. Không phụ thuộc đơn nào thắng nếu worker gửi theo thứ tự khác.
- Kiểm tra trung tâm cuối cùng có đủ hai đơn và trạng thái tương ứng.

**Đạt khi:** không mất đơn, không tự cho Bếp xử lý khi mất mạng, không tự khẳng định đủ hàng. Không tắt DB POS trong bài thử này.

### D10. Một broker Kafka dừng

**Chứng minh:** khả năng chịu lỗi một broker với cấu hình thật.

- Bắt đầu với 3 broker khỏe, ISR đủ, topic replication factor 3/min ISR 2, producer acks all, quorum KRaft phù hợp.
- Dừng một broker, ưu tiên broker đang là leader của một partition để thấy chuyển leader. Không xóa volume.
- Tạo đơn mới ở POS và Online; chấp nhận gián đoạn ngắn khi bầu leader, đo thời gian phục hồi.
- Chờ event được xử lý đúng, tồn/reservation không sai. Bật lại broker và quan sát ISR trở lại đủ.

**Đạt khi:** đơn vẫn được nhận local, luồng Kafka tự phục hồi và xử lý khi còn đủ quorum/ISR. Nếu chỉ thấy API lưu local nhưng Kafka không xử lý cho đến khi bật lại broker thì chưa đạt tuyên bố chịu lỗi một broker của luồng messaging.

### D11. Replication, tái thiết mảnh và lag

**Chứng minh:** bản sao trung tâm được đồng bộ bằng replication và có thể trễ.

- Chờ initial sync xong. Tạo ít nhất một đơn tại POS01, POS02 và Online.
- Đối chiếu các schema/table ở trung tâm với nguồn: orderId, source/site, trạng thái và số dòng món.
- Kiểm tra view union có đủ các mã duy nhất, số lượng bằng tổng các mảnh gốc trong lượt thử.
- Tạm disable subscription POS01. Tạo hoặc hoàn tất một đơn POS01, quan sát nguồn đổi nhưng Dashboard vẫn cũ; nguồn khác tiếp tục replicate.
- Enable lại subscription, chờ dữ liệu POS01 bắt kịp. Ghi thời gian và bằng chứng trước/sau.
- Nếu dừng Query Service, các luồng tạo đơn và giữ hàng vẫn hoạt động.

**Đạt khi:** dữ liệu hội tụ, không xung đột bảng giữa các publisher, Query Service chỉ đọc replica. Không thay logical replication bằng consumer Kafka tự dựng read model rồi vẫn gọi đó là replication DB đã chọn.

### D12. Nhiều instance và định tuyến trạng thái

**Chứng minh:** consumer group, ownership và chống tác dụng phụ khi chia tải/rebalance.

- Chạy cấu hình mở rộng đã được nhóm duyệt; ít nhất hai Inventory instance cho bài thử này.
- Gửi một số đơn nhỏ từ POS01, POS02 và Online với bộ tồn đủ; dùng orderId phân bố trên nhiều partition.
- Kiểm tra log có cả hai Inventory instance xử lý; nếu có hai Kitchen instance thì kiểm tra tương tự.
- Xác nhận các Order Service của các site dùng group khác nhau, còn hai Online instance dùng chung group.
- Đối chiếu từng orderId: trạng thái về đúng DB nguồn, không xuất hiện như đơn mới ở DB POS khác, một quyết định và một ticket mỗi đơn Reserved.
- Dừng một Inventory instance trong khi có event, quan sát rebalance và instance còn lại tiếp tục; kiểm tra duplicate không làm sai tồn.

**Đạt khi:** có bằng chứng cả chia tải và hội tụ. Chỉ khởi chạy hai process nhưng một process luôn idle chưa chứng minh đã xử lý song song.

### D13. Trạng thái muộn và trình duyệt retry

**Chứng minh:** đơn không lùi trạng thái và retry HTTP không sinh đơn ngoài ý muốn.

- Cho một đơn hoàn tất Done, sau đó phát lại Reserved cũ; đơn phải giữ Done.
- Thử Done hợp lệ được nguồn xử lý trước Reserved trong môi trường kiểm tra kiểm soát; nguồn hội tụ Done theo quy tắc thiết kế.
- Gửi hai request tạo đơn có cùng clientRequestId và cùng nội dung; trả về cùng đơn, không tạo hai outbox OrderCreated khác nhau cho hai đơn.
- Dùng lại request ID với nội dung khác; nhận 409 và không đổi đơn cũ.

**Đạt khi:** DB và API đúng theo các trường hợp. Cần phân biệt event giả lập trong bài thử với event sinh tự nhiên; không cho UI tùy ý gửi Done.

### D14. Standby PostgreSQL nếu được duyệt

**Chứng minh:** có bản sao dữ liệu Kho/Bếp, không đồng nhất replica với failover.

- Kiểm tra standby đúng vai trò, theo dõi replay và so sánh dữ liệu sau một số transaction.
- Dừng primary trong lượt riêng, quan sát service không còn ghi được; ghi rõ chưa có automatic failover.
- Chỉ thử promote thủ công nếu đã có quy trình cách ly primary cũ và cấu hình lại service được kiểm chứng.

**Đạt khi:** standby replay đúng; báo cáo trung thực độ trễ và giới hạn. Không tuyên bố zero data loss với streaming replication bất đồng bộ.

### D15. Kitchen lỗi và hoàn nguyên liệu nếu có mở rộng

**Chứng minh:** bù trừ có idempotency.

- Tạo đơn Reserved, giả lập Kitchen thất bại theo contract mở rộng đã thiết kế.
- Kiểm tra Inventory hoàn đúng lượng đã giữ trong reservation, ghi bù trừ đúng một lần.
- Phát lại event lỗi nhiều lần; tồn không tăng thêm.
- Kiểm tra trạng thái kết thúc của đơn về nguồn và không tiếp tục hoàn tất cùng phiếu.

**Đạt khi:** toàn luồng đã triển khai và có bằng chứng. Khi chưa làm, ghi “ngoài baseline” và bỏ qua; không tính là lỗi của baseline.

## 5. Bằng chứng và biểu mẫu kết quả

Mỗi lượt nên lưu dưới `docs/evidence/<ma-luot>/` khi bắt đầu chạy thật. Không tạo screenshot hoặc log giả. Tối thiểu gồm thông số chạy, request/response, truy vấn trước/sau, log có mã tương quan và kết luận.

| Trường | Nội dung cần ghi |
|---|---|
| Run ID | Mã duy nhất cho lượt thử |
| Scenario | D01 đến D15 |
| Code/config | Commit hoặc trạng thái code, version image, số instance |
| Dữ liệu đầu | Bộ seed và số tồn trước thử |
| Định danh | Order IDs, event IDs, request IDs, instance/partition khi liên quan |
| Thao tác lỗi | Đã tắt/ngắt cái gì, tại thời điểm nào, thời gian bao lâu |
| Kết quả mong đợi | Các con số và trạng thái của bài thử |
| Kết quả thực tế | Số hàng, tồn, trạng thái, độ trễ đã đo |
| Đối chứng | Đường dẫn log/API/SQL/screenshot |
| Kết luận | Đạt / Không đạt / Chưa đủ bằng chứng / Ngoài baseline |

Bảng tổng hợp khởi tạo:

| Kịch bản | Trạng thái hiện tại |
|---|---|
| D01–D13 | Chưa chạy |
| D14 | Chưa chốt yêu cầu standby; chưa chạy |
| D15 | Ngoài baseline; chưa triển khai |

Đối với D03, D07 và D08, “đúng kết quả” nhưng chưa kích hoạt đúng cửa sổ cạnh tranh/lỗi chỉ được ghi chưa đủ bằng chứng cho tuyên bố tương ứng.

## 6. Kịch bản trình bày trước lớp

Đề xuất thứ tự trình bày, điều chỉnh theo thời lượng thực tế được giảng viên cho phép:

1. Mở sơ đồ: chỉ nơi lưu đơn gốc, nguồn quyết định tồn, Kafka và bản tổng hợp. Nêu rõ mô phỏng các site bằng container nếu chạy trên một máy.
2. Tạo một đơn bình thường, chỉ event Reserved mới vào Bếp, hoàn tất và xem trạng thái về nguồn.
3. Dùng bộ LAST_PORTION, gửi hai đơn đồng thời và chỉ ra một thành công/một thất bại cùng dữ liệu tồn.
4. Phát lại một event, đối chiếu tồn/reservation/ticket không tăng.
5. Cô lập POS01, lưu đơn Pending, nối lại để xử lý; nhấn mạnh chưa chế biến khi chưa có xác nhận Kho.
6. Tạm dừng subscription để thấy replica trễ rồi bắt kịp. Nếu đủ thời gian, dừng một broker; nếu không, dùng bằng chứng bài thử đã chạy và nói rõ đó là kết quả trước buổi bảo vệ.
7. Kết thúc bằng các giới hạn thực tế: không có auth, không có automatic DB failover, một máy demo không chứng minh chịu lỗi cả site vật lý, Kitchen lỗi sau Reserved chưa tự hoàn hàng nếu chưa làm mở rộng.

Sau mỗi phần, giải thích theo ba câu: lỗi hoặc cạnh tranh nào xảy ra; cơ chế nào bảo vệ dữ liệu; bằng chứng nào cho thấy kết quả đúng. Không dùng số lượng công nghệ làm bằng chứng hệ thống phân tán hoạt động đúng.

## 7. Những tiện ích cần tạo khi triển khai

Các tiện ích dưới đây chưa tồn tại, cần làm cùng code và kiểm tra trước khi dùng:

- Seed/reset dữ liệu theo bộ NORMAL/LAST_PORTION/MISSING_RICE/OFFLINE_LIMITED, cô lập khỏi tài nguyên ngoài dự án.
- Gửi hai request bằng barrier để thử đồng thời, giữ lại response và request ID.
- Lấy và phát lại event, giữ hoặc đổi event ID theo tham số rõ ràng.
- Ngắt/nối đường mạng trung tâm của một POS mà vẫn giữ đường tới local DB/UI.
- Dừng/khởi động broker và service theo tên thực tế trong Compose.
- Failpoint D07/D08 chỉ bật bằng profile demo, mặc định tắt.
- Truy vấn đối chứng theo orderId: quyết định, reservation, ticket, outbox và đơn tại trung tâm.

Các thao tác điều khiển lỗi nằm trong script/profile demo; không đưa nút “tắt Kafka” hoặc endpoint quản trị hạ tầng vào luồng đặt món của người dùng.
