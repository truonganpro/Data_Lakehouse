"""
Schema introspection tools for Trino catalog
Used to generate schema documentation for LLM prompts
"""
import os
from typing import Dict, List
import trino
from trino.dbapi import connect


def get_trino_connection():
    """Create Trino connection from environment variables"""
    return connect(
        host=os.getenv("TRINO_HOST", "trino"),
        port=int(os.getenv("TRINO_PORT", "8080")),
        user=os.getenv("TRINO_USER", "chatbot"),
        catalog=os.getenv("TRINO_CATALOG", "lakehouse"),
        schema=os.getenv("TRINO_DEFAULT_SCHEMA", "gold"),
        http_scheme="http",
    )


def dump_schema(catalog: str = "lakehouse", schema: str = "gold") -> Dict[str, List[Dict]]:
    """
    Dump schema information for all tables
    
    Args:
        catalog: Trino catalog name
        schema: Schema name
        
    Returns:
        Dictionary mapping table names to column information
    """
    result = {}
    
    try:
        with get_trino_connection() as conn:
            cur = conn.cursor()
            
            # Get all tables
            cur.execute(f"SHOW TABLES FROM {catalog}.{schema}")
            tables = [row[0] for row in cur.fetchall()]
            
            # Get columns for each table
            for table in tables:
                cur.execute(f"SHOW COLUMNS FROM {catalog}.{schema}.{table}")
                columns = []
                for row in cur.fetchall():
                    columns.append({
                        "name": row[0],
                        "type": row[1],
                        "nullable": row[2] if len(row) > 2 else None,
                        "comment": row[3] if len(row) > 3 else None,
                    })
                result[table] = columns
                
    except Exception as e:
        print(f"Error dumping schema: {e}")
        
    return result


def format_schema_for_llm(schema_dict: Dict[str, List[Dict]]) -> str:
    """
    Format schema information as text for LLM prompt
    
    Args:
        schema_dict: Schema dictionary from dump_schema()
        
    Returns:
        Formatted string describing the schema
    """
    lines = ["# Database Schema\n"]
    
    for table_name, columns in schema_dict.items():
        lines.append(f"\n## Table: {table_name}")
        lines.append("Columns:")
        for col in columns:
            col_desc = f"  - {col['name']}: {col['type']}"
            if col.get('comment'):
                col_desc += f" -- {col['comment']}"
            lines.append(col_desc)
    
    return "\n".join(lines)


def get_table_sample(table: str, catalog: str = "lakehouse", schema: str = "gold", limit: int = 5) -> List[Dict]:
    """
    Get sample rows from a table
    
    Args:
        table: Table name
        catalog: Trino catalog
        schema: Schema name
        limit: Number of sample rows
        
    Returns:
        List of dictionaries representing rows
    """
    try:
        with get_trino_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"SELECT * FROM {catalog}.{schema}.{table} LIMIT {limit}")
            
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            
            return [dict(zip(columns, row)) for row in rows]
            
    except Exception as e:
        print(f"Error getting table sample: {e}")
        return []


def get_lakehouse_metadata() -> str:
    """
    Get formatted metadata for entire lakehouse
    
    Returns:
        Markdown-formatted string describing the data lakehouse
    """
    catalog = os.getenv("TRINO_CATALOG", "lakehouse")
    
    output = [f"# {catalog.upper()} Data Lakehouse Metadata\n"]
    
    for schema in ["gold", "platinum"]:
        output.append(f"\n## Schema: {schema}\n")
        
        try:
            schema_dict = dump_schema(catalog, schema)
            for table_name, columns in schema_dict.items():
                output.append(f"\n### {schema}.{table_name}")
                output.append(f"Columns: {len(columns)}")
                output.append("```")
                for col in columns[:10]:  # Limit to first 10 columns for brevity
                    output.append(f"{col['name']}: {col['type']}")
                if len(columns) > 10:
                    output.append(f"... and {len(columns) - 10} more columns")
                output.append("```")
        except Exception as e:
            output.append(f"Error reading schema {schema}: {e}")
    
    return "\n".join(output)


if __name__ == "__main__":
    # Test schema dumping
    print("🔍 Dumping lakehouse schema...\n")
    
    for schema in ["gold", "platinum"]:
        print(f"\n{'='*60}")
        print(f"Schema: {schema}")
        print('='*60)
        
        schema_dict = dump_schema("lakehouse", schema)
        
        for table, columns in schema_dict.items():
            print(f"\n📊 {table} ({len(columns)} columns)")
            for col in columns[:5]:  # Show first 5 columns
                print(f"  - {col['name']}: {col['type']}")
            if len(columns) > 5:
                print(f"  ... and {len(columns) - 5} more")

