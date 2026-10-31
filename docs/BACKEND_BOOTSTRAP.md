# Hướng dẫn dựng base backend Java và Spring Boot

Ngày lập: 30/10/2026. Cập nhật 01/11/2026: đã tạo backend B0 tại `backend/` và PostgreSQL Compose tại `infra/compose.base.yaml`. Xem [cách chạy hiện tại](../backend/README.md); kết quả kiểm tra được ghi riêng trong `docs/evidence/`.

## 1. Làm gì trước tiên

Dựng một Order Service đại diện POS01, kết nối PostgreSQL riêng, chạy migration và đọc được menu thật từ DB. Đây là mốc B0 để làm quen Java/Spring trước khi viết nghiệp vụ đặt đơn và tích hợp Kafka. Không dựng tất cả service cùng lúc.

```text
HTTP client -> Order Service POS01 -> PostgreSQL POS01
                 |
                 +-> Flyway tạo bảng và seed
                 +-> Actuator health
```

Base hoàn thành khi clone project, chọn đúng JDK, bật DB rồi build/run được; `GET /actuator/health` và `GET /api/menu` trả dữ liệu thật; restart vẫn còn dữ liệu. Base chưa chứng minh một tính chất phân tán nào. Kafka, outbox nghiệp vụ, consumer, Inventory, Kitchen và replication sẽ được thêm theo các mốc tiếp theo.

Hướng dẫn này bổ sung chi tiết cho chặng 1 của [thiết kế dự án](PROJECT_SPEC.md). Các mẫu bên dưới mô tả cách dựng B0; code hiện có là bản triển khai của mốc này. Lịch sử kiểm tra ngày 30/10 bên dưới được giữ để phân biệt với kết quả chạy thực tế sau khi tạo code.

## 2. Công cụ và version

| Thành phần | Phương án B0 |
|---|---|
| Java target | 21, kế thừa đề xuất của PROJECT_SPEC |
| Spring Boot | Đề xuất pin `4.1.2`, đã đối chiếu tài liệu chính thức tại thời điểm lập hướng dẫn |
| Maven | Dùng Maven Wrapper đi kèm project; giữ version được ghi trong wrapper properties |
| PostgreSQL | Ví dụ pin image `postgres:18.6` cho base, có trong danh sách tag chính thức khi đối chiếu; cả nhóm dùng cùng tag sau khi kiểm tra pull/build thực tế |
| IDE | IntelliJ IDEA hoặc VS Code có hỗ trợ Java; không phụ thuộc IDE để build |
| Docker | Docker Desktop có engine hoạt động và Docker Compose |

Máy hiện tại đã được kiểm tra read-only ngày 30/10/2026: `java` và `javac` là Temurin 25.0.4.1; Docker client/server 29.8.2; Compose v5.5.1. Chưa tìm thấy `mvn` trong PATH; việc đó không cản trở dùng wrapper sau khi tạo project. Chưa có wrapper, project Spring hoặc PostgreSQL của dự án để kiểm tra build/kết nối.

Spring Boot 4.1.2 hỗ trợ Java 21 và JDK 25 theo bảng yêu cầu hệ thống. Java target trong POM và JDK chạy Maven là hai thứ khác nhau. Để nhóm mới học dễ đồng nhất, ưu tiên chọn JDK 21 cho project; không cần gỡ JDK 25. Nếu tạm build bằng JDK 25, POM vẫn đặt target 21, nhưng chưa coi đó là bằng chứng đã chạy app trên runtime 21.

Kiểm tra trong PowerShell trước khi bắt đầu:

```powershell
Set-Location 'D:\distributed-order-system'
java -version
javac -version
docker version
docker compose version
```

Sau khi đã cài JDK 21 nếu cần, chọn Project SDK/Maven runner JDK 21 trong IDE. Với terminal, có thể đặt `JAVA_HOME` cho riêng phiên PowerShell tới đường dẫn JDK 21 thực tế, thêm thư mục `bin` đó vào đầu PATH rồi kiểm tra lại. Không chép một đường dẫn cài đặt minh họa khi chưa xác nhận nó tồn tại. `mvnw.cmd -version` ở bước sau mới cho biết JDK mà Maven thực sự dùng.

## 3. Cấu trúc nhỏ nhất cần tạo

```text
backend/
  pom.xml                         # parent và danh sách module
  mvnw
  mvnw.cmd
  .mvn/wrapper/maven-wrapper.properties
  order-service/
    pom.xml
    src/main/java/vn/edu/orders/order/
      OrderServiceApplication.java
      config/
        SiteProperties.java
      menu/
        api/MenuController.java
        api/MenuItemResponse.java
        domain/MenuItem.java
        infrastructure/MenuItemRepository.java
    src/main/resources/
      application.yml
      db/migration/
        V1__create_menu_items.sql
        V2__seed_demo_menu.sql
    src/test/java/vn/edu/orders/order/
infra/
  compose.base.yaml
```

Chỉ khai báo module `order-service` ở B0. Khi cần Kafka mới thêm `event-contracts`; sau đó thêm Inventory/Kitchen/Query. Không liệt kê các module chưa có POM vì Maven sẽ lỗi ngay. Cấu trúc đầy đủ ở PROJECT_SPEC là hướng phát triển, không phải yêu cầu tạo mọi module rỗng ngay hôm đầu.

Trong module, gom code theo chức năng như `menu`, sau này thêm `orders`, `outbox`. Controller nhận/trả HTTP; service xử lý nghiệp vụ khi có nghiệp vụ; repository truy cập DB. API đọc menu rất đơn giản không cần thêm interface/base class hoặc đủ mọi tầng chỉ để có khuôn mẫu.

## 4. Tạo project và Maven Wrapper

1. Mở [Spring Initializr](https://start.spring.io/), chọn Maven, Java, Spring Boot 4.1.2 nếu vẫn có trong danh sách stable. Không chọn Snapshot/RC. Nếu danh sách đã thay đổi, chọn và pin một bản stable tương thích rồi cập nhật toàn bộ tài liệu/POM cùng lúc.
2. Group `vn.edu.orders`; artifact `order-service`; package `vn.edu.orders.order`; packaging Jar; Java 21.
3. Chọn Spring Web, Spring Data JPA, Validation, PostgreSQL Driver, Flyway Migration, Spring Boot Actuator. Kafka chưa thêm ở B0.
4. Giải nén vào thư mục tạm trong workspace. Đưa `src/` và POM của app vào `backend/order-service/`; đưa wrapper `mvnw`, `mvnw.cmd`, `.mvn/` ra `backend/`.
5. Tạo parent POM và sửa POM con theo mẫu dưới. Không giữ đồng thời hai Spring Boot parent khác version.
6. Giữ main class do Initializr tạo ở package gốc. Các package config/menu/orders nằm bên dưới để được component scan.

Wrapper lần chạy đầu cần tải Maven và dependencies qua mạng. Không cần cài Maven toàn máy chỉ để chạy project này. Khi đưa vào Git, giữ wrapper và migration, bỏ `target/`, `.env` cục bộ, file IDE cá nhân và log khỏi các file theo dõi; không bỏ `.mvn/`.

### 4.1. Parent POM `backend/pom.xml`

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <parent>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-parent</artifactId>
    <version>4.1.2</version>
    <relativePath/>
  </parent>
  <groupId>vn.edu.orders</groupId>
  <artifactId>distributed-order-backend</artifactId>
  <version>0.0.1-SNAPSHOT</version>
  <packaging>pom</packaging>
  <properties>
    <java.version>21</java.version>
  </properties>
  <modules>
    <module>order-service</module>
  </modules>
</project>
```

### 4.2. Module POM `backend/order-service/pom.xml`

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <parent>
    <groupId>vn.edu.orders</groupId>
    <artifactId>distributed-order-backend</artifactId>
    <version>0.0.1-SNAPSHOT</version>
    <relativePath>../pom.xml</relativePath>
  </parent>
  <artifactId>order-service</artifactId>
  <dependencies>
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-webmvc</artifactId>
    </dependency>
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-data-jpa</artifactId>
    </dependency>
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-validation</artifactId>
    </dependency>
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-actuator</artifactId>
    </dependency>
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-flyway</artifactId>
    </dependency>
    <dependency>
      <groupId>org.flywaydb</groupId>
      <artifactId>flyway-database-postgresql</artifactId>
    </dependency>
    <dependency>
      <groupId>org.postgresql</groupId>
      <artifactId>postgresql</artifactId>
      <scope>runtime</scope>
    </dependency>
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-webmvc-test</artifactId>
      <scope>test</scope>
    </dependency>
  </dependencies>
  <build>
    <plugins>
      <plugin>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-maven-plugin</artifactId>
      </plugin>
    </plugins>
  </build>
</project>
```

Mẫu dùng tên starter của Spring Boot 4; không ghép tùy ý dependency từ tutorial Boot 2/3. Chưa cần Lombok, MapStruct, Spring Cloud, Security hoặc thư viện response dùng chung. Version phụ thuộc do Boot quản lý; mẫu POM chưa được resolve/build tại thời điểm viết tài liệu.

## 5. PostgreSQL riêng cho POS01

Tạo `infra/compose.base.yaml` theo mẫu:

```yaml
name: distributed-order-base
services:
  pos01-db:
    image: postgres:18.6
    environment:
      POSTGRES_DB: pos01_orders
      POSTGRES_USER: order_app
      POSTGRES_PASSWORD: order_demo
    ports:
      - "127.0.0.1:5433:5432"
    volumes:
      - pos01-data:/var/lib/postgresql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U order_app -d pos01_orders"]
      interval: 3s
      timeout: 3s
      retries: 20
volumes:
  pos01-data:
```

Thông tin tài khoản trên chỉ là dữ liệu local demo. Dùng volume `/var/lib/postgresql` theo bố trí image PostgreSQL 18, không sao chép mount path của image 17 mà không kiểm tra. B0 chỉ cần một DB; cấu hình mạng cô lập POS, replication và các DB khác sẽ thêm ở bước tương ứng.

Chạy từ root repository sau khi file đã được tạo:

```powershell
Set-Location 'D:\distributed-order-system'
docker compose -f infra/compose.base.yaml config
docker compose -f infra/compose.base.yaml up -d --wait pos01-db
docker compose -f infra/compose.base.yaml ps
docker compose -f infra/compose.base.yaml exec pos01-db pg_isready -U order_app -d pos01_orders
```

Kết quả cần thấy: DB healthy và accepting connections. Container ở trạng thái running chưa đủ để khẳng định DB đã sẵn sàng. Nếu cổng 5433 bị chiếm, đổi mapping và DB_URL đồng bộ. Không dùng `down -v` để restart vì thao tác đó xóa volume dữ liệu.

## 6. Cấu hình Spring và migration

### 6.1. `application.yml`

```yaml
server:
  port: ${SERVER_PORT:8081}
spring:
  application:
    name: order-service
  datasource:
    url: ${DB_URL:jdbc:postgresql://localhost:5433/pos01_orders}
    username: ${DB_USERNAME:order_app}
    password: ${DB_PASSWORD:order_demo}
  jpa:
    open-in-view: false
    hibernate:
      ddl-auto: validate
    properties:
      hibernate:
        default_schema: ${DB_SCHEMA:pos01}
  flyway:
    enabled: true
    create-schemas: true
    default-schema: ${DB_SCHEMA:pos01}
    schemas: ${DB_SCHEMA:pos01}
site:
  id: ${SITE_ID:POS01}
  source-type: ${SOURCE_TYPE:POS}
management:
  endpoints:
    web:
      exposure:
        include: health
```

Tạo `SiteProperties` để bind `site.id` và `site.source-type` bằng `@ConfigurationProperties`, đăng ký qua configuration properties scan hoặc enable tương ứng. Kiểm tra giá trị hợp lệ lúc startup. Sau này thông tin nguồn được lấy từ cấu hình server, không lấy từ body do client tự khai.

`DB_URL` ở ví dụ dành cho Java chạy trên host. Nếu Java chạy trong container, dùng tên service DB và cổng nội bộ 5432; `localhost` lúc đó là chính container app. Flyway tạo schema trước, Hibernate chỉ validate, không dùng `ddl-auto=update` để tự sửa bảng ngoài migration. Đây là cấu hình PostgreSQL thật, không dùng H2 thay cho các bài thử khóa và transaction sau này.

### 6.2. Migration B0

`V1__create_menu_items.sql`:

```sql
CREATE TABLE menu_items (
    dish_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE
);
```

`V2__seed_demo_menu.sql`:

```sql
INSERT INTO menu_items (dish_id, name, active) VALUES
('COM_GA', 'Cơm gà', TRUE),
('CHAO_GA', 'Cháo gà', TRUE);
```

Flyway dùng schema cấu hình `pos01`; migration không hardcode schema POS01 để cùng code có thể chạy nguồn khác sau này. Không ghi data seed hai lần bằng cả Flyway lẫn CommandLineRunner. Sau khi migration đã được dùng chung, thêm migration mới thay vì sửa nội dung migration cũ làm checksum lệch.

## 7. API nhỏ đầu tiên

Tạo các file menu theo các yêu cầu cụ thể:

1. `MenuItem`: JPA entity map bảng `menu_items`, khóa String `dishId`, trường name và active, có constructor không tham số theo yêu cầu JPA. Dùng `jakarta.persistence`, không dùng package `javax.persistence` cũ.
2. `MenuItemRepository`: extends `JpaRepository<MenuItem, String>`, có query lấy món active với thứ tự dishId cố định.
3. `MenuItemResponse`: Java record chứa dishId và name; không trả JPA entity trực tiếp ra API.
4. `MenuController`: `@RestController`, route `GET /api/menu`, dùng constructor injection, lấy dữ liệu repository và map sang response. Không cần lớp service chỉ bọc một lần gọi repository cho API đọc đơn giản này.

Response mong đợi từ seed B0:

```json
[
  { "dishId": "CHAO_GA", "name": "Cháo gà" },
  { "dishId": "COM_GA", "name": "Cơm gà" }
]
```

Chưa cần một kiểu `ApiResponse<T>` cho mọi endpoint, generic CRUD service, base repository, AOP hoặc middleware tự viết. Khi có POST tạo đơn mới thêm validation và handler trả lỗi tương ứng. Chỉ cấu hình CORS cho origin React dev thực tế khi bắt đầu gọi từ frontend; B0 có thể kiểm tra bằng PowerShell trước.

## 8. Build, chạy và đối chứng

Sau khi tạo wrapper và các file ở trên, mở terminal tại backend:

```powershell
Set-Location 'D:\distributed-order-system\backend'
.\mvnw.cmd -version
.\mvnw.cmd -pl order-service -am clean verify
.\mvnw.cmd -pl order-service spring-boot:run
```

`verify` phải có test đã được cấu hình đúng. Nếu giữ `@SpringBootTest` do Initializr sinh, DB phải hoạt động khi test load context; đó là kiểm thử tích hợp phụ thuộc DB thật, không gọi nhầm là unit test độc lập. Không tắt hết test chỉ để build xanh. B0 cần kiểm tra thực sự migration/startup/menu; chưa cần viết nhiều test cho getter hay code mapping đơn giản.

Lệnh run ở B0 chỉ chọn module runnable, không thêm `-am` để goal `spring-boot:run` chạy nhầm parent hoặc module thư viện. Khi có `event-contracts` sau này, build/install dependency trước rồi chạy service đích. Nếu chạy bằng IDE, chọn main class của Order Service và kiểm tra cùng env/JDK với terminal.

Mở terminal khác để kiểm tra:

```powershell
Invoke-RestMethod 'http://localhost:8081/actuator/health'
Invoke-RestMethod 'http://localhost:8081/api/menu'
Set-Location 'D:\distributed-order-system'
docker compose -f infra/compose.base.yaml exec pos01-db psql -U order_app -d pos01_orders -c 'SELECT dish_id, name, active FROM pos01.menu_items ORDER BY dish_id;'
docker compose -f infra/compose.base.yaml exec pos01-db psql -U order_app -d pos01_orders -c 'SELECT version, description, success FROM pos01.flyway_schema_history ORDER BY installed_rank;'
```

Cần thấy health UP, menu có hai món, DB có cùng dữ liệu và hai migration thành công. Health UP chỉ cho biết những thành phần được health indicator kiểm tra đang khỏe; nó không chứng minh mọi bảng hoặc chức năng đúng. Kết hợp cả API và truy vấn SQL.

Dừng app bằng Ctrl+C, restart DB bằng `docker compose -f infra/compose.base.yaml restart pos01-db` từ root, chờ ready rồi chạy app và kiểm tra lại. Dữ liệu phải còn; migration không được seed lặp. Ghi kết quả thực tế thay vì coi các response ví dụ ở tài liệu là kết quả đã chạy.

## 9. Checklist hoàn tất base B0

- [x] Root Maven có parent/version/Java target rõ, wrapper dùng được.
- [x] Order Service khởi động từ terminal, không chỉ chạy được trong IDE người viết.
- [x] DB POS01 riêng hoạt động, volume giữ dữ liệu sau restart.
- [x] Flyway quản lý schema; Hibernate validate không báo lệch entity/bảng.
- [x] Site config bind đúng POS01/POS và có thể đổi qua env.
- [x] Health UP và menu lấy dữ liệu thật từ PostgreSQL.
- [x] Không có auth/token/login dependency hoặc endpoint bắt buộc đăng nhập.
- [x] Chưa có Kafka thì app không cố kết nối Kafka hoặc chờ broker.
- [x] Build/test và kiểm tra thủ công có kết quả lưu lại, không được đánh dấu đạt khi chưa chạy.

Bằng chứng chạy ngày 01/11/2026: [B0-2026-11-01](evidence/B0-2026-11-01/README.md).
Build và runtime hiện dùng JDK 25 với compiler target 21; chưa kiểm chứng runtime JDK 21.

## 10. Sau B0 mới làm gì

| Mốc | Đầu ra |
|---|---|
| B0 base | App + DB + migration + menu + health chạy được |
| B1 đơn local | POST/GET đơn; orders, items và outbox ghi cùng transaction; clientRequestId; trạng thái Pending; test rollback khi ghi outbox lỗi |
| B2 messaging | Kafka 3 broker + worker outbox + event contract; ngắt Kafka không mất đơn |
| B3 Inventory | Công thức, giữ hàng, dedup và Reserved/Failed |
| B4 Kitchen và status | Phiếu duy nhất, Done, cập nhật trạng thái về đúng nguồn |
| B5 nhiều nguồn và trung tâm | POS02/Online, nhiều instance, replication và dashboard |

B1 đã phải ghi outbox cùng đơn dù worker chưa chạy; không tạo một luồng đặt đơn bỏ qua outbox rồi coi đó là phiên bản phân tán hoàn chỉnh. Base B0 không có endpoint tạo đơn nên không vi phạm nguyên tắc này.

## 11. Lỗi thường gặp

| Hiện tượng | Kiểm tra đầu tiên |
|---|---|
| Không có `mvnw.cmd` | Chưa tạo/đưa wrapper đúng về thư mục backend |
| Release version 21 not supported | Maven đang chạy bằng JDK thấp hơn target; xem wrapper -version |
| Parent/module không resolve | Parent relativePath, group/version và module đã có POM chưa |
| Connection refused | DB healthy chưa; app trên host dùng 5433; app trong container dùng pos01-db:5432 |
| Password authentication failed | Credentials cấu hình và DB đã khởi tạo trước đó; đổi env không tự đổi user trong volume cũ |
| Schema/table không tồn tại | Flyway PostgreSQL module, schema cấu hình, vị trí migration và startup log |
| Flyway checksum mismatch | Có sửa migration đã chạy không; bổ sung migration mới thay vì che lỗi bằng repair tùy tiện |
| Hibernate validation lỗi | Tên bảng/cột/kiểu dữ liệu giữa entity và SQL |
| 404 ở menu | Controller có dưới package gốc, annotation và mapping đúng chưa |
| 401 hoặc trang login | Đã vô tình thêm Spring Security; auth ngoài phạm vi B0 |
| Port 8081 đã dùng | Process đang chiếm cổng hoặc SERVER_PORT cần đổi |
| API PowerShell được nhưng React lỗi | CORS, base URL và interceptor/auth guard của frontend; chưa phải lỗi Kafka |

## 12. Nguồn kỹ thuật

Các snippet và thứ tự triển khai là phương án của dự án. Đã đối chiếu những đặc tính nền tảng với tài liệu chính thức, nhưng chưa chạy các snippet thành một project:

- [Spring Boot System Requirements](https://docs.spring.io/spring-boot/system-requirements.html): JDK và build tool được hỗ trợ.
- [Spring Boot Maven Plugin](https://docs.spring.io/spring-boot/maven-plugin/using.html): parent, dependency management và plugin.
- [Spring Boot Build Systems](https://docs.spring.io/spring-boot/reference/using/build-systems.html): quản lý version dependency qua Boot.
- [Spring Boot Database Initialization](https://docs.spring.io/spring-boot/how-to/data-initialization.html): Flyway, module PostgreSQL và migration.
- [Spring Boot Actuator Endpoints](https://docs.spring.io/spring-boot/reference/actuator/endpoints.html): health endpoint.
- [Apache Maven Wrapper](https://maven.apache.org/tools/wrapper/): dùng wrapper để chạy Maven của project.
- [Docker Compose Startup Order](https://docs.docker.com/compose/how-tos/startup-order/): phân biệt running và ready.
- [PostgreSQL Docker Official Image](https://hub.docker.com/_/postgres): image, biến khởi tạo và bố trí volume theo major version.
