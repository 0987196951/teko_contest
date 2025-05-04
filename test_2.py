import re
import yaml

def extract_tables(sql_text):
    input_tables = []
    output_tables = []
    dropped_tables = []
    seen_input_tables = set()
    seen_output_tables = set()

    # loại bỏ comments
    sql_text = re.sub(r'--.*', '', sql_text)

    sql_text = sql_text.lower()

    # Split = ;
    statements = re.split(r';\s*', sql_text)

    drop_pattern = re.compile(r'\bdrop table\s+([a-z0-9_\.]+)')
    create_insert_update_pattern = re.compile(r'\b(create table|insert into|update)\s+([a-z0-9_\.]+)')
    from_join_pattern = re.compile(r'\b(from|join)\s+([a-z0-9_\.]+)')

    def extract_tables_from_statement(stmt):
        # DROP TABLE
        drop_match = drop_pattern.findall(stmt)
        for table in drop_match:
            dropped_tables.append(table.strip())

        # CREATE TABLE, INSERT INTO, UPDATE 
        create_match = create_insert_update_pattern.findall(stmt)
        for _, table in create_match:
            table = table.strip()
            if table not in seen_output_tables:
                output_tables.append(table)
                seen_output_tables.add(table)

        # FROM or JOIN 
        from_join_matches = from_join_pattern.findall(stmt)
        for _, table in from_join_matches:
            table = table.strip()
            if table not in seen_input_tables:
                input_tables.append(table)
                seen_input_tables.add(table)

    for stmt in statements:
        extract_tables_from_statement(stmt)

    # loại bỏ các bảng bị drop
    for table in dropped_tables:
        if table in input_tables:
            input_tables.remove(table)
        if table in output_tables:
            output_tables.remove(table)

    return {
        "inputs": input_tables,
        "outputs": output_tables
    }

# Đọc file input.sql
with open("input.sql", "r") as f:
    sql_text = f.read()

result = extract_tables(sql_text)

# Ghi ra file YAML
with open("output.yaml", "w") as f:
    yaml.dump(result, f, default_flow_style=False)

print("Đã xuất ra file output.yaml.")