# Kế hoạch triển khai – kiến trúc, kiểm thử và cách dùng Claude Code

Bản 2 · 21/11/2026

Tài liệu này nói cách hiện thực đặc tả v1.5 và thiết kế chi tiết bản sửa 3: dựng bằng gì, kiểm thử thế nào, làm theo thứ tự nào và dùng Claude Code ra sao. Dự án đang ở giai đoạn tài liệu, chưa có dòng code nào. Mọi lựa chọn ở đây là đề xuất; khi lệch với đặc tả hoặc thiết kế chi tiết thì hai tài liệu đó đúng.

## Thay đổi ở bản 2

| Mục | Thay đổi | Lý do |
|---|---|---|
| 3 | Ba tầng kiểm thử chia lại cho 44 kịch bản; bỏ bài test thêm về đổi công thức | Đặc tả v1.5 thêm kịch bản 48–55, trong đó kịch bản 49 chính là bài test đó |
| 5 | Thêm mục 5.7 ghi phần cấu hình Claude Code đã tạo và phần còn chờ | Phần không phụ thuộc công nghệ được tạo ngày 21/11/2026 |

## 1. Việc phải xong trước khi code

- **Có mã nguồn nền B5.** Thiết kế chi tiết nhường tên bảng, tên sự kiện và Order Query Service cho B5, nhưng bộ tài liệu này chưa có B5. Ngôn ngữ và framework của dự án cũng phải theo B5.
- **Rà tên gọi với B5.** Một người nắm code B5 đọc mục 2 đến 6 của thiết kế chi tiết và sửa những tên B5 đã có (điểm 1 ở mục 8 của thiết kế).
- **Chốt các điểm còn mở.** Mười điểm ở mục 8 của thiết kế chi tiết đã chốt ngày 21/11/2026; còn năm điểm ở mục 11 của đặc tả. Trong năm điểm đó, tham số và danh sách lý do nằm trong cấu hình nên không chặn tiến độ; các điểm đổi hành vi (hủy cục bộ theo BR9, chặn nguyên liệu bị đánh dấu theo BR6) phải chốt trước khi làm phần liên quan.

## 2. Kiến trúc triển khai

### Công nghệ

Bảng dưới áp dụng khi B5 không quy định khác.

| Phần | Đề xuất | Lý do |
|---|---|---|
| Ngôn ngữ | Java 21, Spring Boot cho HTTP và cấu hình | Client Kafka của Java có đủ và đúng tên các cấu hình đặc tả bắt buộc: `acks=all`, `enable.idempotence`, tắt auto-commit, đăng ký theo mẫu `order-events.*` |
| Truy cập DB | JdbcTemplate và Flyway, không dùng JPA | Đặc tả và thiết kế viết thẳng bằng SQL (`FOR UPDATE`, `ON CONFLICT`, `SAVEPOINT`, UPDATE có điều kiện); JPA che mất đúng những chỗ cần kiểm soát |
| Kafka phía ứng dụng | `kafka-clients` thuần, vòng consumer tự viết theo 5 bước của mục 13.3 đặc tả | Dễ chèn điểm dừng giữa commit DB và commit offset cho kịch bản 26 |
| Hạ tầng | Docker Compose: Kafka KRaft 3 broker, mỗi DB một container PostgreSQL | Đúng mục 13.6 của đặc tả |
| Central Order Replica | Giữ cách của B5. Nếu B5 chưa có thì dùng logical replication của PostgreSQL, mỗi chi nhánh một subscription | Đây là nhân bản bất đồng bộ có sẵn, không phải viết thêm dịch vụ |
| Kiểm thử | JUnit, Testcontainers, Toxiproxy để ngắt mạng | Kiểm thử chạy trên PostgreSQL và Kafka thật |

Nếu B5 viết bằng TypeScript thì kiến trúc giữ nguyên. Khi đó cần chọn client Kafka có idempotent producer ổn định và kiểm tra lại điều này trước khi dùng.

### Module

| Module | Nội dung |
|---|---|
| `common` | Khuôn tin, bộ đẩy outbox, vòng consumer chống trùng, gửi dlq và ghi `alerts`, đồng hồ thay thế được |
| `order-service` | Một codebase cho cả POS và Online, phân biệt bằng `BRANCH_ID` |
| `inventory-service` | Bảng 3.3, hạch toán tồn, trạng thái món |
| `kitchen-service` | Bảng 3.2, ba nút của nhân viên bếp |
| `query-service` | Báo cáo của Quản lý, theo B5 |
| `infra/`, `e2e/` | Compose, script tạo topic, kịch bản đầu-cuối |

### Ba nguyên tắc cấu trúc

- **Handler tách khỏi vòng consumer.** Handler nhận một tin và một giao dịch DB, không biết gì về Kafka. Bài test vì thế tự quyết định thứ tự tin đến, và phần lớn kịch bản không cần Kafka thật.
- **Đồng hồ thay thế được.** Kho và chi nhánh lấy giờ qua một lớp đồng hồ để kịch bản 15, 16, 21 và 25 không phải chờ thời gian thật.
- **Một cửa vào ở Bếp.** Consumer và ba nút bấm gọi cùng một hàm khóa dòng, đúng mục 4 của thiết kế chi tiết.

### Lưu ý khi dựng hạ tầng

- **Tạo sẵn topic bằng script**, với số partition theo điểm 6 ở mục 8 của thiết kế, `replication.factor=3` và `min.insync.replicas=2`. Không dựa vào auto-create.
- **Topic nội bộ của Kafka cũng cần 3 bản sao** (`offsets.topic.replication.factor=3`). Nếu không, tắt một broker ở kịch bản 31 có thể làm consumer đứng.
- **Đăng ký theo mẫu chỉ thấy topic mới sau chu kỳ làm mới metadata**, mặc định 5 phút. Tạo sẵn topic của mọi chi nhánh trước khi chạy dịch vụ để tránh chờ.
- **Mô phỏng mất mạng** bằng cách ngắt container của chi nhánh khỏi mạng Kafka, hoặc bằng Toxiproxy trong kiểm thử.

## 3. Chiến lược kiểm thử

Mục 12 của đặc tả đòi mỗi kịch bản có kiểm thử tự động. 44 kịch bản chia thành ba tầng theo thứ chúng cần để chạy.

| Tầng | Cách chạy | Kịch bản |
|---|---|---|
| 1. Handler và PostgreSQL thật | Bài test gọi handler theo thứ tự mong muốn | 1, 2, 5–9, 15, 16, 19–23, 27, 30, 33–35, 39, 43, 44, 48–50, 53, 54 (27 kịch bản) |
| 2. Tranh chấp | Hai luồng trên hai kết nối, lặp nhiều lần | 3, 28, 37, 42, 51, 52 |
| 3. Kafka thật, nhân bản và chèn lỗi | Dừng container, ngắt mạng; chậm nên chạy riêng | 4, 10, 12, 18, 24–26, 29, 31, 36, 55 |

Quy ước cho mọi bài test:

- Mỗi bài gắn nhãn `KB-<số>` theo số kịch bản để lập được bảng đối chiếu kịch bản với test.
- Không dùng mock thay cho PostgreSQL hay Kafka. Mock làm test xanh mà không chứng minh gì về khóa dòng và giao dịch.
- Sau mỗi bài test của Kho, chạy hai truy vấn bất biến ở mục 3 của thiết kế chi tiết.
- Mỗi kịch bản có bản mức handler ở bước của nó và, nếu cần nhiều dịch vụ, một bản đầu-cuối chạy sau bước 6 (mục 7 của thiết kế).

Mục 9 của thiết kế chi tiết ghi những phần SQL đã được chạy thử khi soạn tài liệu. Các lần chạy thử đó không thay cho bộ kiểm thử này.

## 4. Thứ tự làm

Thứ tự theo đúng các bước và đợt 2 ở mục 7 của thiết kế chi tiết; bảng kịch bản của từng bước nằm ở đó. Kế hoạch này thêm bước chuẩn bị ở đầu, gộp phần báo cáo vào một bước ghép ở cuối, và nêu cách làm việc với Claude Code ở mỗi bước.

| Bước | Nội dung | Cách làm với Claude Code |
|---|---|---|
| 0. Chuẩn bị | Repo, compose, script tạo topic, CI, cấu hình Claude Code ở mục 5 | Một phiên ở plan mode; người duyệt kỹ |
| 1. Dùng chung | Module `common` | Một phiên; soát lại bằng một reviewer chạy trong context mới |
| 2–3. Kho | Giữ, hoàn, hao hụt, đã dùng, cờ cần kiểm kho | Một worktree riêng, làm từng kịch bản |
| 4–5. Bếp | Bảng 3.2, yêu cầu hủy, chi nhánh gốc | Một worktree riêng, làm từng kịch bản |
| 6. Chi nhánh | Hàng chờ gửi, hủy cục bộ, áp tin theo hạng | Một worktree riêng, làm từng kịch bản |
| 7. Ghép | Kịch bản tầng 3, Luồng D (`/monitor`, `/alerts`), báo cáo | Một phiên chính; giao việc chạy test chậm cho subagent |
| Đợt 2 | Luồng C | Một phiên, kịch bản 18–21 |
| Demo | Kịch bản 1, 3, 4, 10, 36 trên đủ dịch vụ | Một script demo chạy lại được |

Thiết kế chi tiết cho phép Kho và chi nhánh làm song song sau bước 1. Kế hoạch này đề xuất cho cả Bếp chạy song song: hợp đồng tin đã cố định trong thiết kế, và test mức handler của Bếp không cần Kho hay chi nhánh.

## 5. Cách dùng Claude Code

Với dự án này, phần khó khi dùng AI là giữ code khớp với ba bảng trạng thái và 44 kịch bản qua nhiều phiên làm việc. Cách làm là biến tài liệu thành thứ máy kiểm được trước, rồi mới cho Claude viết code theo từng kịch bản. Các file cấu hình nêu dưới đây chưa được tạo; chúng thuộc bước 0.

### 5.1 Ngữ cảnh

- **Bộ tài liệu này là nguồn.** Claude đọc theo mục, không nạp cả tài liệu vào mỗi phiên.
- **`CLAUDE.md`** dưới 200 dòng: lệnh build và test, sơ đồ module, 12 điều "không được làm" ở mục 9 của đặc tả, và đường dẫn tới bộ tài liệu.
- **`.claude/rules/`** có `paths:` theo module, mỗi file trỏ tới đúng mục cần đọc: `kitchen-service/**` tới bảng 3.2 và mục 4 của thiết kế; `inventory-service/**` tới bảng 3.3 và mục 3; `order-service/**` tới bảng 3.1 và mục 5.
- **Chỗ tài liệu không nói** thì Claude dừng lại và hỏi; câu trả lời được ghi vào tài liệu tương ứng, không chỉ nằm trong hội thoại.

### 5.2 Vòng lặp theo kịch bản

Một skill `/scenario <số>`, chỉ người dùng gọi được, quy định bốn bước:

1. Đọc dòng kịch bản ở mục 12 của đặc tả và mục mà cột "Quy tắc" trỏ tới.
2. Viết bài test gắn nhãn `KB-<số>`, kiểm tra đủ mọi vế của cột "Kết quả mong đợi", chạy và xác nhận test fail đúng lý do.
3. Hiện thực, chạy lại, đưa output làm bằng chứng.
4. Commit riêng cho kịch bản đó.

Đầu mỗi bước ở mục 4 dùng plan mode để ra kế hoạch. Giữa các nhóm kịch bản thì xóa context.

### 5.3 Cổng kiểm tra

| Cơ chế | Bắt được gì |
|---|---|
| Hook `PostToolUse` | Tự format sau mỗi lần sửa file |
| Hook `PreToolUse` | Sửa migration đã áp dụng; `DELETE` trên bảng đơn của Bếp và Kho; bật auto-commit offset |
| Hook `Stop` | Test tầng 1 và 2 của module vừa sửa chưa xanh thì chưa được kết thúc lượt |
| Subagent `spec-reviewer`, chỉ đọc | Chuyển trạng thái không có trong bảng 3.1–3.3; diff chạm vào kỳ vọng của test |
| Skill `/invariants` | Tồn âm, lượng đang giữ lệch, lần giữ rời Đang giữ hai lần |

### 5.4 Những chỗ AI dễ sai ở dự án này

- **Sửa test cho xanh thay vì sửa code.** Quy định không đổi kỳ vọng của test nếu không trích được mục tài liệu tương ứng.
- **Code đúng khi chạy tuần tự, sai khi chạy song song.** Bốn kịch bản tầng 2 phải chạy lặp nhiều lần, và người nên tự đọc phần khóa dòng.
- **Giả định thứ tự tin.** Mọi handler phải đúng với mọi thứ tự tin tới; mục 2 của thiết kế có ví dụ `CancelPending` tới sau `OrderFailed`.
- **Tự điền chỗ tài liệu im lặng.** Xem mục 5.1.

### 5.5 Nhiều agent

- **Subagent** cho việc sinh nhiều output: chạy test tầng 3 và chỉ báo test fail, đọc log, tìm trong code.
- **Ba worktree song song** cho Kho, Bếp và chi nhánh sau bước 1. Mỗi phiên sở hữu một module; thay đổi `common` hoặc thay đổi hợp đồng tin chỉ làm trên nhánh chính.
- **Agent Teams** dùng một lần trước demo, ba teammate rà theo ba góc: chống trùng (BR4, BR8, BR12), tranh chấp (BR7, BR9) và hạch toán tồn. Tính năng này còn thử nghiệm và tốn nhiều token hơn hẳn một phiên thường.

### 5.6 Tự động hóa

- **CI:** test tầng 1 và 2 trên mỗi PR, tầng 3 chạy đêm.
- **Review PR tự động** bằng `claude-code-action`, với `REVIEW.md` chứa 12 điều "không được làm", có giới hạn số lượt và timeout.
- **Rà chất lượng test:** một script `claude -p` chạy cho từng kịch bản, chỉ đọc, trả JSON cho biết bài test `KB-<số>` có kiểm tra đủ các vế của cột "Kết quả mong đợi" hay không.
- **Không cần cho dự án:** MCP server (dùng `psql` và CLI của Kafka qua Bash là đủ), Agent SDK và OpenTelemetry.

## 6. Rủi ro còn mở

- **Chưa thấy B5.** Tên gọi, ngôn ngữ và cách nhân bản về Central Order Replica đều có thể đổi sau khi đọc B5.
- **Cách Kafka chia partition cho hai instance Inventory** cần chạy thử (điểm 6 ở mục 8 của thiết kế). Kịch bản 42 vì vậy lấy bài test mức handler làm bài chính.
- **Tài nguyên máy demo.** Với hai POS site, một máy phải chạy cùng lúc ba broker, sáu DB PostgreSQL và bảy tiến trình dịch vụ; cần đo sớm ở bước 0.
- **Kịch bản tầng 3 dễ chập chờn.** Chúng phụ thuộc thời điểm dừng và bật container; cần điểm chờ rõ ràng thay cho chờ theo thời gian.
