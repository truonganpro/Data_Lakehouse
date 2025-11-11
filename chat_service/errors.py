"""
Error codes and exceptions for SQL guardrails
"""
from enum import Enum


class GuardCode(str, Enum):
    """Error codes for SQL guardrails"""
    DISALLOWED_SCHEMA = "DISALLOWED_SCHEMA"     # Schema ngoài gold|platinum
    STAR_PROJECTION = "STAR_PROJECTION"         # SELECT * không được phép
    MISSING_LIMIT = "MISSING_LIMIT"             # Không có LIMIT (outermost)
    MISSING_TIME_PRED = "MISSING_TIME_PRED"     # Fact lớn thiếu filter thời gian
    BANNED_FUNC = "BANNED_FUNC"                 # Hàm/stmt cấm: SHOW/EXPLAIN/CALL/ALTER/DELETE/UPDATE
    NO_DATA = "NO_DATA"                         # Chạy OK nhưng 0 rows
    AMBIGUOUS_INTENT = "AMBIGUOUS_INTENT"       # Câu hỏi mơ hồ (đòi SQL)
    NON_SQL_INTENT = "NON_SQL_INTENT"           # Small talk / about data / about project


class GuardError(Exception):
    """Exception raised when SQL guardrail is triggered"""
    
    def __init__(self, code: GuardCode, detail: str = ""):
        self.code = code
        self.detail = detail
        super().__init__(f"{code.value}: {detail}")

