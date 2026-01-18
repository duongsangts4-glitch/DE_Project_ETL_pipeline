from pyspark.sql.types import LongType
from config.Spark_config import Set_SparkSession
from pyspark.sql.functions import col, explode, to_timestamp, lit, coalesce

def test_spark():
    jars = ["mysql:mysql-connector-java:8.0.33","org.mongodb.spark:mongo-spark-connector_2.12:10.5.0"]
    spark_connect = Set_SparkSession(
                app_name='spark_test',
                master_url='local[*]',
                executor_cores=3,
                executor_memory='2g',
                driver_memory='1g',
                executor_nums=1,
                jar_packages=jars,
                spark_conf={"spark.mongodb.write.connection.uri": "mongodb://admin:secret123@localhost:27019"},
                log_level='ERROR'
    )
    return spark_connect

# def database_table():
#     table = {
#         'T_USERS': "users",
#         'T_REPOS': "repositories",
#         'T_EVENTS': "events",
#         'T_ISSUES': "issues",
#         'T_COMMENTS': "comments",
#         'T_LABELS': "labels",
#         'T_ISSUE_LABELS': "issue_labels",
#         'T_PAYLOADS': "payloads"
#     }
#     return table
def dataframe_transform():
    # data = test_spark().spark.read.option("multiline", True).json('../../data/2015-03-01-0.json')
    df_raw = test_spark().spark.read.option("multiLine", "true").json('../../data/small_json.json')
    df = df_raw.withColumn("event_id", col("id").cast(LongType())) \
        .withColumn("created_at_ts", to_timestamp(col("created_at")))
    # ====================================================
    # XỬ LÝ TỪNG BẢNG (MAPPING CỘT CHO KHỚP SCHEMA CÓ SẴN)
    # ====================================================
    # 1. BẢNG USERS
    # User xuất hiện ở actor, issue.user, và comment.user. Cần gộp lại.
    def extract_user(prefix):
        return df.select(
            col(f"{prefix}.id").cast(LongType()).alias("id"),
            col(f"{prefix}.login").alias("login"),
            col(f"{prefix}.avatar_url").alias("avatar_url"),
            col(f"{prefix}.url").alias("url"),
            col(f"{prefix}.type").alias("type"),
            lit(False).alias("site_admin")  # Giả định mặc định nếu thiếu
        ).filter(col("id").isNotNull())

    df_users = extract_user("actor") \
        .unionByName(extract_user("payload.issue.user")) \
        .unionByName(extract_user("payload.comment.user")) \
        .dropDuplicates(["id"])

    # 2. BẢNG REPOSITORIES
    df_repos = df.select(
        col("repo.id").cast(LongType()).alias("id"),
        col("repo.name").alias("name"),
        col("repo.url").alias("url")
    ).dropDuplicates(["id"])

    # 3. BẢNG ISSUES
    # Cần lấy repo_id từ root và user_id từ payload.issue.user
    df_issues = df.select(
        col("payload.issue.id").cast(LongType()).alias("id"),
        col("repo.id").cast(LongType()).alias("repo_id"),  # FK tới repositories
        col("payload.issue.user.id").cast(LongType()).alias("user_id"),  # FK tới users
        col("payload.issue.number").cast(LongType()).alias("number"),
        col("payload.issue.title").alias("title"),
        col("payload.issue.body").alias("body"),
        col("payload.issue.state").alias("state"),
        col("payload.issue.locked").alias("locked"),
        col("payload.issue.comments").alias("comments_count"),
        to_timestamp(col("payload.issue.created_at")).alias("created_at"),
        to_timestamp(col("payload.issue.updated_at")).alias("updated_at"),
        to_timestamp(col("payload.issue.closed_at")).alias("closed_at")
    ).filter(col("id").isNotNull()).dropDuplicates(["id"])

    # 4. BẢNG COMMENTS
    df_comments = df.select(
        col("payload.comment.id").cast(LongType()).alias("id"),
        col("payload.issue.id").cast(LongType()).alias("issue_id"),  # FK tới issues
        col("payload.comment.user.id").cast(LongType()).alias("user_id"),  # FK tới users
        col("payload.comment.body").alias("body"),
        to_timestamp(col("payload.comment.created_at")).alias("created_at"),
        to_timestamp(col("payload.comment.updated_at")).alias("updated_at")
    ).filter(col("id").isNotNull()).dropDuplicates(["id"])

    # 5. BẢNG LABELS & 6. ISSUE_LABELS
    # Explode mảng labels ra
    df_exploded = df.select(
        col("payload.issue.id").cast(LongType()).alias("issue_id"),
        explode(col("payload.issue.labels")).alias("lbl")
    ).filter(col("issue_id").isNotNull())

    # Bảng LABELS (Danh mục)
    df_labels = df_exploded.select(
        col("lbl.name").alias("name"),
        col("lbl.color").alias("color"),
        col("lbl.url").alias("url")
    ).dropDuplicates(["name"])

    # Bảng ISSUE_LABELS (Quan hệ n-n)
    # Lưu ý: Kiểm tra schema MySQL xem cột này lưu 'label_name' hay 'label_id'
    df_issue_labels = df_exploded.select(
        col("issue_id"),
        col("lbl.name").alias("label_name")
    ).dropDuplicates(["issue_id", "label_name"])

    # 7. BẢNG EVENTS
    # Bảng này lưu thông tin sự kiện chính
    df_events = df.select(
        col("event_id").alias("id"),
        col("type"),
        col("actor.id").cast(LongType()).alias("actor_id"),  # FK tới users
        col("repo.id").cast(LongType()).alias("repo_id"),  # FK tới repositories
        col("public"),
        col("created_at_ts").alias("created_at")
    )

    # 8. BẢNG PAYLOADS
    # Thường bảng này lưu action hoặc JSON string của payload
    df_payloads = df.select(
        col("event_id").alias("event_id"),  # FK tới events
        col("payload.action").alias("action")
        # Nếu bảng payloads có cột lưu toàn bộ json, bạn có thể thêm:
        # , to_json(col("payload")).alias("payload_json")
    ).filter(col("event_id").isNotNull())

    # Hàm ghi dữ liệu an toàn
    def write_safe(dataframe, table_name):
        if dataframe.count() == 0:
            return
        print(f"-> Đang ghi vào bảng: {table_name}")
        try:
            # Dùng "append" để thêm vào bảng đã có sẵn schema
            # Lưu ý: Nếu dữ liệu đã tồn tại (trùng Primary Key), append mặc định sẽ gây lỗi.
            # Để bỏ qua dòng trùng lặp, MySQL JDBC hỗ trợ "INSERT IGNORE" thông qua cấu hình,
            # nhưng Spark chuẩn không hỗ trợ trực tiếp "INSERT IGNORE".
            # Cách đơn giản nhất ở đây là dùng try-catch hoặc lọc trước dữ liệu nếu cần.

            dataframe.write.jdbc(url=jdbc_url, table=table_name, mode="append", properties=db_props)
            print("   Thành công.")
        except Exception as e:
            print(f"   LỖI (Có thể do trùng khóa chính hoặc sai tên cột): {e}")

    # Thứ tự ghi để đảm bảo khóa ngoại (Foreign Keys) được thỏa mãn:
    # 1. Các bảng danh mục (độc lập)
    write_safe(df_users, "users")
    write_safe(df_repos, "repositories")
    write_safe(df_labels, "labels")

    # 2. Bảng Issues (phụ thuộc users, repos)
    write_safe(df_issues, "issues")

    # 3. Bảng Events (phụ thuộc users, repos)
    write_safe(df_events, "events")

    # 4. Các bảng con phụ thuộc vào Issues/Events
    write_safe(df_comments, "comments")
    write_safe(df_issue_labels, "issue_labels")
    write_safe(df_payloads, "payloads")

