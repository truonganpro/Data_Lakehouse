# 💬 Chat Service - Summary Answer + Header + Quick-Replies (Phần 3)

## ✅ Đã hoàn thành

### Việc 1: Bật "tóm tắt kết quả" (Summary Text) kèm Insight nhanh

#### 1. Cập nhật `llm_summarize.py`
- **Cải thiện `PROMPT_SUMMARY`**: 
  - Yêu cầu rõ ràng: Phạm vi (1 câu), Xu hướng (1-2 câu), Điểm đáng chú ý (1 câu)
  - Thêm ví dụ cụ thể để LLM hiểu format
  - Giới hạn 2-4 câu (thay vì 2-5 câu)
  
- **`summarize_with_gemini()`**: 
  - Đã có sẵn, sử dụng `gemini-1.5-flash` cho summarization nhanh
  - Format table preview dưới dạng markdown table
  - Xử lý citations nếu có

#### 2. Cập nhật `format_answer()`
- **Thêm summary vào answer**: 
  - Gọi `summarize_with_gemini()` nếu có `rows_preview`
  - Hiển thị summary trong phần "📝 **Tóm tắt:**"
  - Fallback: Hiển thị số dòng nếu không có summary

---

### Việc 2: Thêm "Header nguồn dữ liệu" (Data Provenance)

#### 1. Tạo hàm `_parse_schema_from_sql()`
- **Parse schema từ SQL**: 
  - Kiểm tra `platinum` trước (more specific)
  - Kiểm tra `gold`
  - Default: `gold`

#### 2. Cập nhật `format_answer()`
- **Header với data provenance**: 
  - Format: `🗂️ **Nguồn:** lakehouse.{source_schema} • ⏱️ **Thời gian chạy:** {exec_ms}ms • 📦 {data_freshness}`
  - `data_freshness`: "Dữ liệu batch (2016-2018), không realtime" (fixed)
  - Chỉ hiển thị header khi có SQL và `rows_preview`

#### 3. Cập nhật `main.ask()`
- **Parse source schema**: 
  - Gọi `_parse_schema_from_sql()` để lấy schema từ SQL
  - Pass `source_schema` vào `format_answer()`

---

### Việc 3: Quick-Replies theo Intent/Skill

#### 1. Tạo `chat_service/suggestions.py`
- **`suggestions_for()`**: 
  - Priority 1: Guard code suggestions (error fixes)
  - Priority 2: Skill-based suggestions (revenue_timeseries, top_products, payment_mix, etc.)
  - Priority 3: Data-driven suggestions (based on rows_preview columns)
  - Priority 4: Question-based fallback
  - Default: Generic suggestions

- **`suggestions_for_non_sql()`**: 
  - Suggestions cho smalltalk, about_data, about_project
  - Default suggestions cho mỗi topic

#### 2. Cập nhật `main.ask()`
- **Generate suggestions**: 
  - Sau khi execute SQL thành công → generate suggestions từ skill_metadata và rows_preview
  - Khi có guard error → generate suggestions từ guard_code
  - Khi không có SQL → generate suggestions từ non-SQL metadata
  - Pass suggestions vào `AskResponse`

#### 3. Cập nhật `AskResponse` model
- **Thêm trường `suggestions`**: 
  - `suggestions: Optional[List[str]] = None`
  - Trả về cùng với answer, sql, rows_preview, etc.

#### 4. Cập nhật Frontend (`app/pages/2_💬_Chat.py`)
- **Hiển thị suggestions dưới dạng buttons**: 
  - Lấy `suggestions` từ API response
  - Hiển thị dưới assistant message
  - Format: "💡 Gợi ý câu hỏi tiếp theo:"
  - Buttons có thể click để gửi lại câu hỏi tương ứng
  - Store suggestions trong `st.session_state.last_suggestions`

---

## 📋 Files Created/Modified

### Created:
1. **`chat_service/suggestions.py`**:
   - `suggestions_for()`: Generate context-aware suggestions
   - `suggestions_for_non_sql()`: Generate suggestions for non-SQL responses

### Modified:
1. **`chat_service/llm_summarize.py`**:
   - Updated `PROMPT_SUMMARY` với yêu cầu rõ ràng hơn
   - Added `_parse_schema_from_sql()` function
   - Updated `format_answer()` để thêm header và summary

2. **`chat_service/main.py`**:
   - Updated `run_sql()` để raise `GuardError` với proper error codes
   - Updated `ask()` endpoint để parse source_schema và generate suggestions
   - Updated `AskResponse` model để thêm trường `suggestions`

3. **`app/pages/2_💬_Chat.py`**:
   - Updated để hiển thị suggestions từ API response
   - Added suggestions buttons dưới assistant message
   - Store suggestions trong session state

---

## 🎯 Kết quả

### Trước khi cải thiện:
- ❌ Không có summary text
- ❌ Không có header nguồn dữ liệu
- ❌ Suggestions chung chung, không theo ngữ cảnh
- ❌ Không có quick-replies trong UI

### Sau khi cải thiện:
- ✅ Summary text với insights (2-4 câu) từ LLM
- ✅ Header với data provenance (source, execution time, freshness)
- ✅ Context-aware suggestions (theo skill/intent/error)
- ✅ Quick-replies dưới dạng buttons trong UI
- ✅ Suggestions có thể click để gửi lại câu hỏi

---

## 🔧 Implementation Details

### 1. Summary Text Flow:
```
1. SQL executed successfully → rows_preview
2. Call summarize_with_gemini(question, rows_preview, citations)
3. If summary exists → display in "📝 **Tóm tắt:**"
4. If no summary → display "📊 **Kết quả:** {len(rows_preview)} dòng"
```

### 2. Header Flow:
```
1. Parse source_schema from SQL (_parse_schema_from_sql)
2. Get execution_time_ms from run_sql
3. Format header: "🗂️ **Nguồn:** lakehouse.{source_schema} • ⏱️ **Thời gian chạy:** {exec_ms}ms • 📦 {data_freshness}"
4. Display before summary text
```

### 3. Suggestions Flow:
```
1. After SQL execution → generate suggestions from skill_metadata + rows_preview
2. If guard error → generate suggestions from guard_code
3. If non-SQL → generate suggestions from topic
4. Return suggestions in AskResponse
5. Frontend displays suggestions as clickable buttons
```

---

## 🧪 Test Cases

### Test 1: Summary Text
- **Input**: "Doanh thu theo tháng gần đây?"
- **Expected**: 
  - Header: "🗂️ **Nguồn:** lakehouse.gold • ⏱️ **Thời gian chạy:** XXXms • 📦 Dữ liệu batch (2016-2018), không realtime"
  - Summary: "Doanh thu theo tháng từ 06-08/2018, giảm dần từ 1.23M → 987K. Tháng cao nhất là 07/2018 với 1.12M. Xu hướng giảm nhẹ nhưng ổn định."
  - Table preview

### Test 2: Header Data Provenance
- **Input**: "Top 10 sản phẩm bán chạy nhất?"
- **Expected**: 
  - Header với `source_schema="gold"` (hoặc "platinum" nếu query từ platinum)
  - `execution_time_ms` hợp lý
  - `data_freshness="Dữ liệu batch (2016-2018), không realtime"`

### Test 3: Context-Aware Suggestions
- **Input**: "Phương thức thanh toán nào phổ biến?"
- **Expected**: 
  - Suggestions: ["AOV theo phương thức thanh toán", "Xu hướng thanh toán theo tháng", "Tỷ lệ thanh toán trả góp"]
  - Buttons hiển thị dưới assistant message
  - Click button → gửi lại câu hỏi tương ứng

### Test 4: Error Suggestions
- **Input**: "Doanh thu theo tháng" (no time range)
- **Expected**: 
  - Error: `MISSING_TIME_PRED`
  - Suggestions: ["Doanh thu 3 tháng gần đây", "Doanh thu Q3-2018", "Doanh thu theo tháng từ 2017-01 đến 2017-12"]

### Test 5: Non-SQL Suggestions
- **Input**: "xin chào"
- **Expected**: 
  - Answer: Smalltalk response
  - Suggestions: ["Doanh thu 3 tháng gần đây", "Top 10 sản phẩm bán chạy", "Phương thức thanh toán phổ biến"]

---

## 📝 Notes

### 1. LLM Summarization
- **Provider**: Gemini (gemini-1.5-flash)
- **Fallback**: Nếu không có API key hoặc LLM fail → chỉ hiển thị số dòng
- **Prompt**: Đã được cải thiện với yêu cầu rõ ràng và ví dụ

### 2. Header Data Provenance
- **Source Schema**: Tự động parse từ SQL
- **Execution Time**: Từ `run_sql()` return value
- **Data Freshness**: Fixed string cho batch data (có thể mở rộng sau)

### 3. Context-Aware Suggestions
- **Priority Order**: 
  1. Guard code suggestions (error fixes)
  2. Skill-based suggestions (theo skill name)
  3. Data-driven suggestions (theo columns in rows_preview)
  4. Question-based fallback
  5. Default suggestions

### 4. Frontend Display
- **Suggestions Buttons**: Hiển thị dưới assistant message
- **Click Action**: Set `st.session_state.selected_example` và trigger rerun
- **History**: Suggestions chỉ hiển thị cho response mới nhất (không lưu trong history)

---

## 🚀 Next Steps (Phần 4)

1. **Context Memory**: Lưu context của conversation để suggestions chính xác hơn
2. **About Dataset/Project từ Metadata**: Tự động lấy thông tin từ metadata lakehouse
3. **Dynamic Data Freshness**: Parse từ metadata thay vì fixed string
4. **More Context-Aware Suggestions**: Suggestions dựa trên conversation history

---

**Last Updated:** $(date)

**Files:** 
- `chat_service/llm_summarize.py`
- `chat_service/main.py`
- `chat_service/suggestions.py` (NEW)
- `app/pages/2_💬_Chat.py`

**Status:** ✅ Hoàn thành Phần 3

