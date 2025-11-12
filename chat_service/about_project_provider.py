# -*- coding: utf-8 -*-
"""
About Project Provider
Returns project architecture and tech stack information
"""
from typing import Dict


def get_about_project_card() -> str:
    """
    Generate formatted card about project architecture
    """
    return (
        "**🏗️ Kiến trúc Lakehouse - Brazilian E-commerce Data Platform**\n\n"
        "**🎨 Presentation Layer (UI):**\n"
        "  • **Streamlit Dashboard** (http://localhost:8501)\n"
        "    - Executive Dashboard với 11 tabs (Revenue, Growth, Category, Geography, Seller, Operations, Customer, Finance, Forecast, Data Quality, Insights)\n"
        "    - Query Window (GUI builder + Manual SQL)\n"
        "    - Chat Interface (trợ lý AI)\n"
        "    - Forecast Explorer (ML predictions)\n"
        "  • **Metabase BI** (http://localhost:3000) - Business Intelligence tool\n"
        "  • **Dagster Dagit** (http://localhost:3001) - Data pipeline orchestration UI\n"
        "  • **Chat Service API** (http://localhost:8001) - REST API cho chatbot\n\n"
        "**⚙️ Processing Layer:**\n"
        "  • **Trino** (SQL query engine) - Distributed SQL queries trên Delta Lake\n"
        "  • **Apache Spark** (ETL processing) - Transform data Bronze → Silver → Gold → Platinum\n"
        "  • **MLflow** (ML model tracking) - Track forecasting models (LightGBM)\n"
        "  • **Chat Service** (FastAPI) - SQL generation + RAG + LLM summarization\n"
        "    - Intent router với 12 skills (Revenue, Products, Geography, Payment, Cohort, etc.)\n"
        "    - Guardrails (read-only, schema whitelist, auto LIMIT, timeout)\n"
        "    - Gemini integration (SQL generation + result summarization)\n\n"
        "**💾 Storage Layer:**\n"
        "  • **Delta Lake** trên MinIO (S3-compatible object storage)\n"
        "    - Bronze: Raw CSV data\n"
        "    - Silver: Cleaned data (Parquet format)\n"
        "    - Gold: Fact & Dimension tables (Delta format)\n"
        "    - Platinum: Pre-aggregated datamarts (Delta format)\n"
        "  • **MySQL** (Hive Metastore + Chat logging)\n"
        "  • **Qdrant** (Vector DB) - RAG embeddings cho document search\n\n"
        "**🔒 Security & Guardrails:**\n"
        "  • **Read-only** SQL queries (chỉ SELECT/WITH, không DDL/DML)\n"
        "  • **Schema whitelist** (chỉ `lakehouse.gold` và `lakehouse.platinum`)\n"
        "  • **Auto LIMIT** (mặc định 10,000 rows, có thể override)\n"
        "  • **Query timeout** (30 giây)\n"
        "  • **RAG với citations** (trích dẫn nguồn tài liệu)\n"
        "  • **AST parsing** (phát hiện SELECT *, dangerous functions)\n\n"
        "**📊 Use Cases:**\n"
        "  • **Business Analytics**: Revenue analysis, product performance, customer segmentation\n"
        "  • **Operational Metrics**: SLA tracking, seller KPI, logistics optimization\n"
        "  • **Forecasting**: Demand prediction với confidence intervals\n"
        "  • **Self-Service BI**: Natural language queries → SQL → Insights\n\n"
        "**💡 Tech Stack:**\n"
        "  • **Languages**: Python 3.10, SQL (Trino dialect)\n"
        "  • **Frameworks**: FastAPI, Streamlit, Dagster\n"
        "  • **Data**: Delta Lake, Apache Spark, Trino\n"
        "  • **ML**: LightGBM, MLflow, Google Gemini API\n"
        "  • **Infrastructure**: Docker, Docker Compose\n"
        "  • **Vector DB**: Qdrant\n\n"
        "**🚀 Để bắt đầu:**\n"
        "  • Hỏi mình về dữ liệu: \"Dataset của bạn gồm gì?\"\n"
        "  • Truy vấn số liệu: \"Doanh thu theo tháng gần đây?\"\n"
        "  • Khám phá dashboard: Truy cập http://localhost:8501"
    )


if __name__ == "__main__":
    # Test
    print("="*60)
    print("Testing About Project Provider")
    print("="*60)
    card = get_about_project_card()
    print(card)

