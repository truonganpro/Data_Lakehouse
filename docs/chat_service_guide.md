# Chat Service User Guide

## Giới thiệu

Chat Service là hệ thống hỏi đáp tương tác với Data Lakehouse bằng ngôn ngữ tự nhiên (tiếng Việt). Hệ thống kết hợp:

- **SQL Generation**: Tự động sinh câu lệnh SQL từ câu hỏi
- **RAG (Retrieval-Augmented Generation)**: Trích dẫn tài liệu liên quan

## Cách sử dụng

### 1. Truy cập Chat UI

Mở Streamlit app:
```
http://localhost:8501
```

Chọn trang "💬 Chat" từ sidebar.

### 2. Đặt câu hỏi

Nhập câu hỏi vào ô chat. Ví dụ:

**Về doanh thu:**
- "Doanh thu theo tháng gần đây?"
- "Doanh thu theo danh mục sản phẩm?"
- "Doanh thu theo vùng miền?"

**Về sản phẩm:**
- "Top 10 sản phẩm bán chạy nhất?"
- "Danh mục nào có doanh thu cao nhất?"
- "Sản phẩm nào có giá trị đơn hàng trung bình cao nhất?"

**Về khách hàng:**
- "Phân bố đơn hàng theo bang?"
- "Bang nào có nhiều khách hàng nhất?"

**Về thanh toán:**
- "Phương thức thanh toán phổ biến?"
- "Phân bố theo loại thanh toán?"

**Về giao hàng:**
- "Tỷ lệ giao hàng đúng hạn?"
- "Thời gian giao hàng trung bình?"

### 3. Xem kết quả

Sau khi hỏi, bạn sẽ nhận được:

1. **Câu trả lời text** từ assistant
2. **SQL Query** đã được sinh ra (có thể xem/copy)
3. **Data Preview** (50 dòng đầu)
4. **Tài liệu tham khảo** (RAG citations)

### 4. Export dữ liệu

Nhấn nút "📥 Download CSV" để tải kết quả về.

## Tính năng nâng cao

### Câu hỏi mẫu

Sidebar có sẵn các câu hỏi mẫu. Click để sử dụng.

### Reset Chat

Nhấn "🔄 Reset Chat" để xóa lịch sử và bắt đầu mới.

### Statistics

Nhấn "📈 Show Statistics" để xem thống kê mô tả của dữ liệu.

## Giới hạn & An toàn

### Giới hạn

- **Read-only**: Chỉ SELECT/WITH, không cho phép INSERT/UPDATE/DELETE
- **Row limit**: Tối đa 5,000 dòng
- **Timeout**: 45 giây mỗi query
- **Preview**: Chỉ hiển thị 50 dòng đầu trong UI

### An toàn

- ✅ SQL được validate trước khi chạy
- ✅ Whitelist schemas (chỉ gold, platinum)
- ✅ Tự động thêm LIMIT nếu thiếu
- ✅ Log đầy đủ vào database

## Troubleshooting

### "Query took too long to execute"

**Nguyên nhân:** Query quá phức tạp hoặc dữ liệu lớn

**Giải pháp:**
- Thu hẹp khoảng thời gian (ví dụ: 3 tháng thay vì 1 năm)
- Thêm điều kiện WHERE cụ thể hơn
- Hỏi top N thay vì tất cả

### "Schema không nằm trong whitelist"

**Nguyên nhân:** Câu hỏi yêu cầu dữ liệu từ Bronze/Silver layer

**Giải pháp:** Chỉ hỏi về dữ liệu trong Gold hoặc Platinum

### "Không rõ yêu cầu"

**Nguyên nhân:** Câu hỏi chưa đủ rõ ràng

**Giải pháp:**
- Nêu rõ KPI (doanh thu, số đơn, AOV...)
- Chỉ định khung thời gian (tháng này, 3 tháng gần đây...)
- Thêm điều kiện lọc nếu cần (theo vùng, theo danh mục...)

## API Usage (Advanced)

### Health Check

```bash
curl http://localhost:8001/health
```

### Get Examples

```bash
curl http://localhost:8001/examples
```

### Ask Question

```bash
curl -X POST http://localhost:8001/ask \
  -H "Content-Type: application/json" \
  -d '{
    "question": "Doanh thu theo tháng?",
    "prefer_sql": true
  }'
```

## Tips & Best Practices

1. **Bắt đầu đơn giản:** Hỏi các câu cơ bản trước, sau đó mở rộng
2. **Sử dụng ví dụ:** Tham khảo câu hỏi mẫu trong sidebar
3. **Kiểm tra SQL:** Luôn xem SQL được sinh ra để hiểu logic
4. **Export ngay:** Download CSV nếu cần phân tích sâu hơn
5. **Feedback:** Nếu kết quả không đúng, hãy diễn đạt lại câu hỏi

## Support

Nếu gặp vấn đề, liên hệ Data Team hoặc xem logs:

```bash
docker logs chat_service -f
```

---

**Happy Querying! 💬**

