import pymysql
from urllib.parse import urlparse

uri = "mysql://root:tien123@localhost:3306/test"

# Phân tích URI
parsed_uri = urlparse(uri)
username = parsed_uri.username
password = parsed_uri.password
host = parsed_uri.hostname
port = parsed_uri.port
database = parsed_uri.path.lstrip("/")

# Tạo kết nối
connection = pymysql.connect(
    host=host,
    user=username,
    password=password,
    database=database,
    port=port,
    cursorclass=pymysql.cursors.DictCursor
)

try:
    with connection.cursor() as cursor:
        cursor.execute("""
        INSERT INTO Transactions (TransactionID, CustomerID, TransactionAmount, TransactionDate)
        VALUES
            (1, 100, 50.00, '2023-01-01'),
            (2, 100, 75.00, '2023-01-05'),
            (3, 101, 20.00, '2023-01-02'),
            (4, 102, 100.00, '2023-01-03'),
            (5, 102, 150.00, '2023-01-04');
        """)
        connection.commit()
        query = """
            SELECT CustomerID, TransactionID, TransactionAmount, TransactionDate
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY CustomerID ORDER BY TransactionDate) AS rn
                FROM Transactions
            ) t
            WHERE rn = 1
            ORDER BY CustomerID;
        """
        cursor.execute(query)
        results = cursor.fetchall()
        for row in results:
            print(row)
finally:
    connection.close()
