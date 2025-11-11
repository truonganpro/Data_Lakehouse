# 💬 Chat Service - Guard-Raised Answers (Phần 2)

## ✅ Đã hoàn thành

### Việc A: Chuẩn hóa mã lỗi (Error Codes)

#### 1. Tạo `chat_service/errors.py`
- **GuardCode Enum**: 8 loại error codes
  - `DISALLOWED_SCHEMA`: Schema ngoài gold|platinum
  - `STAR_PROJECTION`: SELECT * không được phép
  - `MISSING_LIMIT`: Không có LIMIT (outermost)
  - `MISSING_TIME_PRED`: Fact lớn thiếu filter thời gian
  - `BANNED_FUNC`: Hàm/stmt cấm (SHOW/EXPLAIN/CALL/ALTER/DELETE/UPDATE)
  - `NO_DATA`: Chạy OK nhưng 0 rows
  - `AMBIGUOUS_INTENT`: Câu hỏi mơ hồ (đòi SQL)
  - `NON_SQL_INTENT`: Small talk / about data / about project

- **GuardError Exception**: Custom exception với code và detail

#### 2. Cập nhật `enforce_sql_safety()`
- Thêm parameter `raise_guard_error: bool = True`
- Ném `GuardError` thay vì `HTTPException` khi `raise_guard_error=True`
- Giữ backward compatibility với `HTTPException` khi `raise_guard_error=False`

---

### Việc B: Bản đồ lỗi → Câu trả lời + Quick-Replies

#### 1. Tạo `chat_service/guard_message.py`
- **`message_and_suggestions()`**: Map error code → (message, suggestions)
- Mỗi error code có:
  - Thông điệp ngắn gọn, dễ hiểu
  - 2-3 gợi ý cụ thể (quick-replies)
  - Context-aware suggestions (dựa trên skill_meta nếu có)

#### 2. Cập nhật `main.ask()`
- Catch `GuardError` và trả về message + suggestions
- Append suggestions vào error message
- Log error với detail

---

### Việc C: Gắn "mã lỗi đúng" ở các chốt an toàn

#### 1. `_check_star_projection()`
- Kiểm tra `SELECT *` (không tính trong aggregate functions)
- Raise `GuardCode.STAR_PROJECTION` nếu phát hiện

#### 2. `_check_missing_time_predicate()`
- Kiểm tra query trên `fact_order`/`fact_order_item` không có time predicate
- Time columns: `full_date`, `year_month`, `order_date`
- Time functions: `date_trunc`, `date_parse`, `cast`, `date`
- Raise `GuardCode.MISSING_TIME_PRED` nếu phát hiện

#### 3. `_check_dangerous_keywords_with_ast()`
- Thêm `SHOW`, `EXPLAIN`, `CALL`, `EXEC`, `EXECUTE` vào danh sách dangerous
- Raise `GuardCode.BANNED_FUNC` nếu phát hiện

#### 4. `_parse_sql_schemas()`
- Kiểm tra schema whitelist (gold, platinum)
- Raise `GuardCode.DISALLOWED_SCHEMA` nếu schema ngoài whitelist

#### 5. `run_sql()`
- Thêm parameter `check_empty: bool = True`
- Raise `GuardCode.NO_DATA` nếu `rows == 0` và `check_empty=True`
- Wrap SQL execution errors thành `GuardError` với mã phù hợp

---

### Việc D: Trường hợp "không phải SQL" (Non-SQL Intent)

#### 1. Cập nhật `sql_templates.py`
- Thêm personal question triggers: `"bạn là ai"`, `"bạn biết tôi"`, `"tôi là ai"`, etc.
- Thêm hàm `_has_data_entities()`: Kiểm tra question có chứa data entities không
- Fallback logic: Nếu có social keywords nhưng không có data entities → `NON_SQL_INTENT`

#### 2. Cập nhật `main.ask()`
- Xử lý personal questions trong smalltalk response
- Trả lời: "Mình không lưu thông tin cá nhân và cũng không nhận diện người dùng"
- Gợi ý chuyển hướng sang câu hỏi về dữ liệu

---

### Việc E: Test Cases

#### 1. Small Talk
- **Input**: "xin chào" / "bạn biết tôi là ai không"
- **Expected**: `NON_SQL_INTENT` → Lời chào + 2 quick-replies

#### 2. Thiếu Thời Gian
- **Input**: "doanh thu theo tháng" (không nêu khoảng)
- **Expected**: `MISSING_TIME_PRED` → Gợi ý "3 tháng gần đây / Q3-2018"

#### 3. Không LIMIT
- **Input**: "liệt kê tất cả đơn hàng"
- **Expected**: `MISSING_LIMIT` → Gợi ý "Thêm LIMIT 100 / Tổng theo tháng"
- **Note**: Hiện tại tự động thêm LIMIT, không raise error (có thể thay đổi sau)

#### 4. Schema Không Hợp Lệ
- **Input**: "select * from system.runtime.nodes"
- **Expected**: `DISALLOWED_SCHEMA` → Gợi ý dùng gold/platinum

#### 5. 0 Dòng
- **Input**: "doanh thu 2099-01"
- **Expected**: `NO_DATA` → Gợi ý nới khoảng thời gian

---

## 📋 Files Created/Modified

### Created:
1. **`chat_service/errors.py`**:
   - `GuardCode` enum
   - `GuardError` exception

2. **`chat_service/guard_message.py`**:
   - `message_and_suggestions()` function
   - Error message mapping
   - Quick-reply suggestions

### Modified:
1. **`chat_service/main.py`**:
   - Updated `enforce_sql_safety()` to raise `GuardError`
   - Added `_check_star_projection()`
   - Improved `_check_missing_time_predicate()`
   - Updated `_check_dangerous_keywords_with_ast()`
   - Updated `run_sql()` to raise `GuardCode.NO_DATA`
   - Updated `ask()` endpoint to handle `GuardError`
   - Updated smalltalk response for personal questions

2. **`chat_service/sql_templates.py`**:
   - Added personal question triggers
   - Added `_has_data_entities()` function
   - Added fallback logic for non-SQL intent

---

## 🎯 Kết quả

### Trước khi cải thiện:
- ❌ Lỗi chung chung: "Mình chưa sinh được SQL an toàn"
- ❌ Không có gợi ý cụ thể
- ❌ Không phân biệt loại lỗi
- ❌ Personal questions bị ép sinh SQL

### Sau khi cải thiện:
- ✅ Lỗi cụ thể với mã code có ý nghĩa
- ✅ Thông điệp ngắn gọn, dễ hiểu
- ✅ Gợi ý cụ thể theo ngữ cảnh (2-3 quick-replies)
- ✅ Personal questions được xử lý đúng (smalltalk)
- ✅ Phân biệt rõ các loại lỗi (schema, time, limit, etc.)

---

## 🔧 Implementation Details

### Error Code Mapping:

| Error Code | Trigger | Message | Suggestions |
|------------|---------|---------|-------------|
| `MISSING_TIME_PRED` | Query large fact table without time filter | "⚠️ Mình không chạy truy vấn này vì thiếu điều kiện thời gian..." | "Doanh thu 3 tháng gần đây", "Doanh thu Q3-2018" |
| `MISSING_LIMIT` | Query without LIMIT | "⚠️ Truy vấn thiếu LIMIT..." | "Top 100 đơn hàng", "Doanh thu theo tháng" |
| `DISALLOWED_SCHEMA` | Schema outside whitelist | "⚠️ Câu hỏi đang chạm vào schema ngoài vùng an toàn..." | "Doanh thu theo tháng từ datamart", "Top 10 sản phẩm" |
| `STAR_PROJECTION` | SELECT * | "⚠️ Không cho phép `SELECT *`..." | "Chọn month, revenue, order_count" |
| `NO_DATA` | Query returns 0 rows | "📭 Không có dữ liệu khớp điều kiện..." | "Mở rộng khoảng thời gian", "Bỏ bớt bộ lọc" |
| `BANNED_FUNC` | Dangerous keywords | "⚠️ Câu lệnh chứa hàm không được phép..." | "Doanh thu theo tháng", "Top 10 sản phẩm" |
| `AMBIGUOUS_INTENT` | Unclear question | "❓ Mình cần rõ hơn bạn muốn xem chỉ số nào..." | "Doanh thu theo tháng", "Top 10 sản phẩm" |
| `NON_SQL_INTENT` | Small talk / about data / about project | "💬 Đây là câu hỏi không cần SQL..." | "Giới thiệu dataset", "Tóm tắt kiến trúc" |

---

## 🧪 Testing

### Test 1: Small Talk
```python
# Input: "xin chào"
# Expected: NON_SQL_INTENT → Smalltalk response
```

### Test 2: Personal Question
```python
# Input: "bạn biết tôi là ai không"
# Expected: NON_SQL_INTENT → Personal question response
```

### Test 3: Missing Time Predicate
```python
# Input: "doanh thu theo tháng" (no time range)
# Expected: MISSING_TIME_PRED → Time filter suggestions
```

### Test 4: No Data
```python
# Input: "doanh thu 2099-01"
# Expected: NO_DATA → Expand time range suggestions
```

### Test 5: Disallowed Schema
```python
# Input: "select * from system.runtime.nodes"
# Expected: DISALLOWED_SCHEMA → Use gold/platinum suggestions
```

---

## 📝 Notes

### 1. Backward Compatibility
- `enforce_sql_safety()` vẫn hỗ trợ `HTTPException` khi `raise_guard_error=False`
- Có thể tắt `GuardError` nếu cần (không khuyến khích)

### 2. Time Predicate Check
- Chỉ kiểm tra trên `fact_order` và `fact_order_item`
- Cho phép query small tables (dim tables) mà không cần time filter
- Có thể mở rộng danh sách large fact tables sau

### 3. LIMIT Handling
- Hiện tại tự động thêm LIMIT nếu thiếu (không raise error)
- Có thể thay đổi để raise error nếu user explicitly requests without LIMIT

### 4. Error Messages
- Messages được viết bằng tiếng Việt (có thể thêm English sau)
- Suggestions được tối ưu theo ngữ cảnh (skill_meta)
- Có thể mở rộng với more context-aware suggestions

---

## 🚀 Next Steps (Phần 3)

1. **Summary Answer + Header**: Thêm header "Nguồn: lakehouse.gold/platinum, batch 2016–2018"
2. **Quick-Replies theo Intent**: Gợi ý theo skill ngay trong `format_answer()`
3. **LLM Summarization**: Bật tóm tắt LLM cho answers
4. **Context-Aware Suggestions**: Suggestions dựa trên skill metadata và question context

---

**Last Updated:** $(date)

**Files:** 
- `chat_service/errors.py`
- `chat_service/guard_message.py`
- `chat_service/main.py`
- `chat_service/sql_templates.py`

**Status:** ✅ Hoàn thành Phần 2

