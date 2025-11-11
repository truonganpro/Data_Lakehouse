# 🧩 SQL Thủ công - Query Window Enhancement

## ✅ Đã hoàn thành

### 1. 📝 Tính năng SQL Thủ công

#### A. Chế độ chuyển đổi
- **Radio button** trong sidebar: "Trình dựng (GUI)" ↔ "SQL thủ công"
- **Mặc định**: "Trình dựng (GUI)" (giữ nguyên chức năng cũ)
- **Khi chọn "SQL thủ công"**: Hiển thị giao diện SQL thủ công và dừng phần GUI

#### B. Text Area cho SQL
- **Khung nhập SQL**: Text area với height 260px
- **Sample SQL**: Có sẵn template mẫu với CTE và JOIN
- **Tooltip**: Hướng dẫn sử dụng placeholder `:start`, `:end`, `:month`

#### C. Tham số nhanh
- **Start Date**: Date input với giá trị mặc định từ `COVER_MIN`
- **End Date**: Date input với giá trị mặc định từ `COVER_MAX`
- **Month**: Text input với format `YYYY-MM`
- **Tooltips**: Giải thích từng tham số

#### D. Tùy chọn
- **Auto LIMIT**: Checkbox để tự động thêm LIMIT 10000 nếu thiếu
- **EXPLAIN**: Checkbox để hiển thị execution plan trước khi chạy
- **Tooltips**: Giải thích từng tùy chọn

---

### 2. 🔒 Rào chắn An toàn

#### A. Kiểm tra SELECT/WITH
- **Regex**: `^\s*(WITH|SELECT)\b`
- **Hành động**: Hiển thị info message và dừng nếu không bắt đầu bằng SELECT/WITH
- **Mục đích**: Chỉ cho phép truy vấn đọc dữ liệu

#### B. Chặn DDL/DML
- **Từ khóa nguy hiểm**: `ALTER`, `DROP`, `TRUNCATE`, `INSERT`, `UPDATE`, `DELETE`, `CREATE`, `RENAME`, `CALL`, `GRANT`, `REVOKE`, `MERGE`, `EXEC`, `EXECUTE`
- **Regex**: `\b(ALTER|DROP|...)\b` (case-insensitive)
- **Hành động**: Hiển thị error message và dừng nếu phát hiện từ khóa nguy hiểm
- **Mục đích**: Ngăn chặn thay đổi dữ liệu hoặc cấu trúc

#### C. Kiểm tra Schema
- **Regex**: `\blakehouse\.(gold|platinum)\.`
- **Hành động**: Hiển thị warning (không chặn) nếu không tìm thấy schema hợp lệ
- **Mục đích**: Khuyến khích người dùng dùng đúng schema, nhưng không chặn hoàn toàn (có thể dùng subqueries/views)

#### D. Tự động thêm LIMIT
- **Điều kiện**: `auto_limit = True` và không có LIMIT trong SQL
- **Hành động**: Thêm `LIMIT 10000` vào cuối câu lệnh SQL
- **Mục đích**: Ngăn chặn query trả về quá nhiều dòng, gây quá tải

---

### 3. 🔄 Xử lý Placeholder

#### A. Thay thế Placeholder
- **`:start`**: Thay bằng `'{d_start.isoformat()}'` (ví dụ: `'2017-01-01'`)
- **`:end`**: Thay bằng `'{d_end.isoformat()}'` (ví dụ: `'2017-12-31'`)
- **`:month`**: Thay bằng `'{month_input}'` (ví dụ: `'2017-05'`)
- **An toàn**: Chỉ thay thế nếu placeholder có trong SQL

#### B. Hiển thị SQL đã xử lý
- **Luôn hiển thị**: Nếu có thay đổi (placeholder được thay thế hoặc LIMIT được thêm)
- **Collapsible**: Nếu không có thay đổi, hiển thị trong expander (đóng mặc định)
- **Format**: Syntax highlighting với `language="sql"`

---

### 4. 🔍 Tính năng EXPLAIN

#### A. Execution Plan
- **Checkbox**: "Hiện kế hoạch (EXPLAIN) trước khi chạy"
- **Button**: "🔍 EXPLAIN" (chỉ hiển thị khi checkbox được chọn)
- **Hành động**: Chạy `EXPLAIN {sql}` và hiển thị kết quả
- **Mục đích**: Giúp người dùng hiểu execution plan trước khi chạy query

#### B. Hiển thị kết quả
- **DataFrame**: Hiển thị execution plan trong dataframe
- **Height**: 280px
- **Error handling**: Hiển thị error message nếu EXPLAIN thất bại

---

### 5. ▶️ Chạy SQL và Hiển thị Kết quả

#### A. Xác định Schema
- **Ưu tiên**: Kiểm tra `lakehouse.platinum` trước, sau đó `lakehouse.gold`
- **Mặc định**: `gold` nếu không tìm thấy schema
- **Warning**: Hiển thị warning nếu không phát hiện schema trong SQL

#### B. Chạy Query
- **Spinner**: Hiển thị "⏳ Đang chạy truy vấn..." khi chạy
- **Error handling**: Hiển thị error message chi tiết nếu query thất bại
- **Empty result**: Hiển thị warning nếu không có dữ liệu trả về

#### C. Hiển thị Kết quả
- **DataFrame**: Hiển thị kết quả trong dataframe với height 500px
- **Success message**: Hiển thị số dòng trả về
- **Statistics**: Expander với thống kê tổng hợp (số dòng, số cột, kiểu dữ liệu)

#### D. Export
- **CSV**: Download button để tải CSV
- **Excel**: Download button để tải Excel (nếu có openpyxl)
- **Filename**: `custom_sql_result_{timestamp}.csv/xlsx`

---

### 6. 📚 Hướng dẫn và Ví dụ

#### A. Expander "Hướng dẫn SQL thủ công"
- **Cách sử dụng**: Hướng dẫn 3 bước cơ bản
- **Rào chắn an toàn**: Liệt kê các rào chắn và mục đích
- **Ví dụ SQL**: 3 ví dụ từ cơ bản đến nâng cao
- **Lưu ý**: Hướng dẫn về half-open interval, year-month columns, performance, EXPLAIN

#### B. Ví dụ SQL
1. **Truy vấn đơn giản**: SELECT với WHERE và LIMIT
2. **Với CTE**: Sử dụng WITH clause
3. **JOIN với dimension tables**: JOIN nhiều bảng

---

### 7. 🎨 UI/UX Improvements

#### A. Icons và Emojis
- **🔀 Chế độ**: Radio button để chọn chế độ
- **🧩 SQL thủ công**: Subheader với icon
- **📝 SQL Query**: Text area label
- **⚙️ Tham số nhanh**: Section header
- **📅 Start/End**: Date input labels
- **📆 Month**: Text input label
- **✅ Auto LIMIT**: Checkbox label
- **🔍 EXPLAIN**: Checkbox và button labels
- **📋 SQL đã xử lý**: Section header
- **▶️ Run SQL**: Button label
- **📊 Kết quả**: Subheader
- **📈 Thống kê**: Expander label
- **💾 Xuất dữ liệu**: Subheader
- **⬇️ Tải CSV/Excel**: Download button labels

#### B. Colors và Styling
- **Success**: Màu xanh lá (`✅`)
- **Error**: Màu đỏ (`❌`)
- **Warning**: Màu vàng (`⚠️`)
- **Info**: Màu xanh dương (`ℹ️`)
- **Primary button**: Streamlit primary button style

#### C. Layout
- **Columns**: Sử dụng columns để bố cục tham số và export options
- **Expanders**: Sử dụng expanders cho hướng dẫn và thống kê
- **Spacing**: Sử dụng markdown để tạo khoảng cách hợp lý

---

### 8. 🔧 Technical Details

#### A. Imports
- **`re`**: Để kiểm tra regex patterns
- **`datetime`**: Đã có sẵn, sử dụng cho date handling
- **Các imports khác**: Giữ nguyên từ code cũ

#### B. Functions
- **`run_query()`**: Giữ nguyên, sử dụng để chạy SQL
- **`get_date_coverage()`**: Sử dụng để lấy `COVER_MIN` và `COVER_MAX`

#### C. Variables
- **`mode`**: Radio button value ("Trình dựng (GUI)" hoặc "SQL thủ công")
- **`sql_input`**: Text area input
- **`sql`**: SQL đã xử lý (sau khi thay placeholder và thêm LIMIT)
- **`query_schema`**: Schema để chạy query (gold hoặc platinum)

#### D. Error Handling
- **Try-except**: Bọc tất cả các thao tác có thể gây lỗi
- **Error messages**: Hiển thị error message chi tiết
- **Helpful hints**: Hiển thị gợi ý dựa trên loại lỗi (TABLE_NOT_FOUND, SYNTAX_ERROR)

---

### 9. 📋 Checklist

- [x] Thêm import `re`
- [x] Thêm radio button để chọn chế độ
- [x] Thêm text area cho SQL input
- [x] Thêm tham số nhanh (start, end, month)
- [x] Thêm checkbox auto LIMIT
- [x] Thêm checkbox EXPLAIN
- [x] Thêm kiểm tra SELECT/WITH
- [x] Thêm chặn DDL/DML
- [x] Thêm kiểm tra schema
- [x] Thêm thay thế placeholder
- [x] Thêm tự động thêm LIMIT
- [x] Thêm hiển thị SQL đã xử lý
- [x] Thêm button EXPLAIN
- [x] Thêm button Run SQL
- [x] Thêm xác định schema từ SQL
- [x] Thêm hiển thị kết quả
- [x] Thêm thống kê tổng hợp
- [x] Thêm export CSV
- [x] Thêm export Excel
- [x] Thêm hướng dẫn và ví dụ
- [x] Thêm error handling
- [x] Thêm UI/UX improvements

---

### 10. 🎯 Kết quả

#### Trước khi cải thiện:
- ❌ Chỉ có GUI mode (Trình dựng)
- ❌ Không thể nhập SQL tùy chỉnh
- ❌ Không có rào chắn an toàn cho SQL
- ❌ Không có placeholder cho tham số

#### Sau khi cải thiện:
- ✅ Có 2 chế độ: GUI và SQL thủ công
- ✅ Có thể nhập SQL tùy chỉnh
- ✅ Có rào chắn an toàn đầy đủ (SELECT/WITH only, chặn DDL/DML, kiểm tra schema)
- ✅ Có placeholder cho tham số (`:start`, `:end`, `:month`)
- ✅ Tự động thêm LIMIT nếu thiếu
- ✅ Có tính năng EXPLAIN
- ✅ Có export CSV/Excel
- ✅ Có hướng dẫn và ví dụ

---

### 11. 📁 Files Modified

1. **`app/pages/1_📊_Query_Window.py`**:
   - Thêm import `re`
   - Thêm radio button để chọn chế độ
   - Thêm phần SQL thủ công (300+ dòng code)
   - Thêm rào chắn an toàn
   - Thêm xử lý placeholder
   - Thêm tính năng EXPLAIN
   - Thêm export options
   - Thêm hướng dẫn và ví dụ

---

### 12. 🚀 Cách sử dụng

#### A. Chuyển sang chế độ SQL thủ công
1. Mở sidebar
2. Chọn "SQL thủ công" trong radio button "🔀 Chế độ"
3. Giao diện SQL thủ công sẽ hiển thị

#### B. Nhập SQL
1. Gõ SQL vào text area "📝 SQL Query"
2. Sử dụng placeholder `:start`, `:end`, `:month` nếu cần
3. Điều chỉnh tham số nhanh nếu cần

#### C. Chạy SQL
1. Chọn tùy chọn (auto LIMIT, EXPLAIN)
2. Nhấn button "▶️ Run SQL"
3. Xem kết quả và export nếu cần

---

### 13. ⚠️ Lưu ý

#### A. Rào chắn an toàn
- **SELECT/WITH only**: Chỉ cho phép SELECT và WITH
- **Chặn DDL/DML**: Chặn tất cả các lệnh thay đổi dữ liệu
- **Kiểm tra schema**: Khuyến khích dùng `lakehouse.gold` hoặc `lakehouse.platinum`
- **Auto LIMIT**: Tự động thêm LIMIT 10000 nếu thiếu

#### B. Performance
- **LIMIT**: Luôn thêm LIMIT để tránh query quá nặng
- **EXPLAIN**: Dùng EXPLAIN để kiểm tra execution plan trước khi chạy
- **Schema detection**: Tự động phát hiện schema từ SQL

#### C. Error Handling
- **Try-except**: Tất cả các thao tác đều có error handling
- **Error messages**: Hiển thị error message chi tiết
- **Helpful hints**: Hiển thị gợi ý dựa trên loại lỗi

---

## ✅ Summary

**Đã hoàn thành việc thêm tính năng SQL thủ công vào Query Window:**

1. ✅ Chế độ chuyển đổi GUI ↔ SQL thủ công
2. ✅ Text area cho SQL input với sample template
3. ✅ Tham số nhanh (start, end, month)
4. ✅ Rào chắn an toàn (SELECT/WITH only, chặn DDL/DML, kiểm tra schema)
5. ✅ Xử lý placeholder (`:start`, `:end`, `:month`)
6. ✅ Tự động thêm LIMIT nếu thiếu
7. ✅ Tính năng EXPLAIN
8. ✅ Hiển thị kết quả và export CSV/Excel
9. ✅ Hướng dẫn và ví dụ
10. ✅ Error handling và UI/UX improvements

**Kết quả:** Query Window giờ có thể sử dụng cả GUI mode và SQL thủ công, với đầy đủ rào chắn an toàn và tính năng hỗ trợ.

---

**Last Updated:** $(date)

**File:** `app/pages/1_📊_Query_Window.py`

**Status:** ✅ Hoàn thành

