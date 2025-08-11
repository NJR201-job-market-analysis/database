from datetime import datetime
from sqlalchemy import create_engine, ForeignKey, UniqueConstraint, Table, inspect, text
from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    String,
    Text,
    MetaData,
    Integer,
)
import logging
from sqlalchemy.exc import SQLAlchemyError

from config import (
    MYSQL_ACCOUNT,
    MYSQL_HOST,
    MYSQL_PASSWORD,
    MYSQL_PORT,
    MYSQL_DATABASE,
)

# 使用更標準的日誌設定方式
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class Database:
    def __init__(self):
        self.engine = self._get_database_connection()
        self.metadata = MetaData()
        self._define_tables()
        self._sync_schema()
        logger.info("✅ 資料庫初始化與結構同步完成。")

    def _get_database_connection(self):
        try:
            address = f"mysql+pymysql://{MYSQL_ACCOUNT}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
            engine = create_engine(address)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("✅ 資料庫連接成功")
            return engine
        except SQLAlchemyError as e:
            logger.warning(
                "⚠️ 無法連接到資料庫 %s (%s)，嘗試自動創建...", MYSQL_DATABASE, e
            )
            try:
                server_address = f"mysql+pymysql://{MYSQL_ACCOUNT}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}"
                server_engine = create_engine(server_address)
                with server_engine.connect() as conn:
                    conn.execute(
                        text(
                            f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DATABASE}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                        )
                    )
                    logger.info("✅ 資料庫 %s 創建成功", MYSQL_DATABASE)
                server_engine.dispose()

                engine = create_engine(address)
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                logger.info("✅ 成功連接到新創建的資料庫 %s", MYSQL_DATABASE)
                return engine
            except SQLAlchemyError as create_error:
                logger.error("❌ 自動創建或連接資料庫失敗: %s", create_error)
                raise ConnectionError(
                    f"無法建立資料庫連接: {create_error}"
                ) from create_error

    def _define_tables(self):
        jobs_columns = [
            Column("id", BigInteger, primary_key=True, autoincrement=True),
            Column("job_title", String(200), nullable=False),
            Column("company_name", String(200), nullable=False),
            Column("job_description", Text),
            Column("work_type", String(100)),
            Column("required_skills", Text),
            Column("salary_min", Integer),
            Column("salary_max", Integer),
            Column("salary_type", String(20)),
            Column("salary_text", String(100)),
            Column("experience_text", String(100)),
            Column("experience_min", Integer),
            Column("city", String(50)),
            Column("district", String(50)),
            Column("location", String(200)),
            Column("job_url", String(500), nullable=False, unique=True),
            Column("platform", String(100)),
            Column("created_at", DateTime, default=datetime.now),
            Column("updated_at", DateTime, default=datetime.now, onupdate=datetime.now),
        ]

        categories_columns = [
            Column("id", BigInteger, primary_key=True, autoincrement=True),
            Column("name", String(200), nullable=False, unique=True),
            Column("created_at", DateTime, default=datetime.now),
            Column("updated_at", DateTime, default=datetime.now, onupdate=datetime.now),
        ]

        jobs_categories_columns = [
            Column("id", BigInteger, primary_key=True, autoincrement=True),
            Column(
                "category_id",
                BigInteger,
                ForeignKey("categories.id", ondelete="CASCADE"),
                nullable=False,
            ),
            Column(
                "job_id",
                BigInteger,
                ForeignKey("jobs.id", ondelete="CASCADE"),
                nullable=False,
            ),
            Column("created_at", DateTime, default=datetime.now),
            Column("updated_at", DateTime, default=datetime.now, onupdate=datetime.now),
        ]

        skills_columns = [
            Column("id", BigInteger, primary_key=True, autoincrement=True),
            Column("name", String(200), nullable=False, unique=True),
            Column("created_at", DateTime, default=datetime.now),
            Column("updated_at", DateTime, default=datetime.now, onupdate=datetime.now),
        ]

        jobs_skills_columns = [
            Column("id", BigInteger, primary_key=True, autoincrement=True),
            Column(
                "job_id",
                BigInteger,
                ForeignKey("jobs.id", ondelete="CASCADE"),
                nullable=False,
            ),
            Column(
                "skill_id",
                BigInteger,
                ForeignKey("skills.id", ondelete="CASCADE"),
                nullable=False,
            ),
            Column("created_at", DateTime, default=datetime.now),
            Column("updated_at", DateTime, default=datetime.now, onupdate=datetime.now),
        ]

        Table("jobs", self.metadata, *jobs_columns)
        Table("categories", self.metadata, *categories_columns)
        Table(
            "jobs_categories",
            self.metadata,
            *jobs_categories_columns,
            UniqueConstraint("job_id", "category_id", name="uix_job_category"),
        )
        Table("skills", self.metadata, *skills_columns)
        Table(
            "jobs_skills",
            self.metadata,
            *jobs_skills_columns,
            UniqueConstraint("job_id", "skill_id", name="uix_job_skill"),
        )

    def _sync_schema(self):
        logger.info("正在開始同步資料庫結構...")
        self.metadata.create_all(self.engine)  # 首先確保所有表都已建立
        logger.info("✅ 已確保所有資料表都存在。")

        try:
            with self.engine.begin() as connection:  # 使用事務來確保操作的原子性
                inspector = inspect(self.engine)
                for table_name, table_obj in self.metadata.tables.items():
                    db_columns = {
                        col["name"]: col for col in inspector.get_columns(table_name)
                    }
                    model_columns = {col.name: col for col in table_obj.c}

                    # 1. 新增欄位
                    for col_name, model_col in model_columns.items():
                        if col_name not in db_columns:
                            add_stmt = f"ALTER TABLE `{table_name}` ADD COLUMN {model_col.compile(dialect=self.engine.dialect)}"
                            logger.info(
                                f"正在新增欄位 '{col_name}' 到資料表 '{table_name}'..."
                            )
                            connection.execute(text(add_stmt))

                    # 2. 修改欄位類型 (謹慎操作)
                    for col_name, model_col in model_columns.items():
                        if col_name in db_columns:
                            db_col_type = db_columns[col_name]["type"]
                            model_col_type = model_col.type

                            db_type_str = str(db_col_type).upper()
                            model_type_str = str(
                                model_col_type.compile(self.engine.dialect)
                            ).upper()

                            # 比較型別或可否為空 (nullability) 是否有變化
                            if (
                                db_type_str != model_type_str
                                or db_columns[col_name]["nullable"]
                                != model_col.nullable
                            ):
                                logger.warning(
                                    f"偵測到資料表 '{table_name}' 的欄位 '{col_name}' 定義不一致。將會嘗試修改..."
                                )
                                # **【修正處】**：手動組合出正確的欄位定義字串
                                # 例如: `job_title` VARCHAR(200) NOT NULL
                                column_definition = f"`{model_col.name}` {model_col_type.compile(self.engine.dialect)}"
                                if model_col.nullable is False:
                                    column_definition += " NOT NULL"

                                # 組合出最終的、語法正確的 SQL
                                modify_stmt = f"ALTER TABLE `{table_name}` MODIFY COLUMN {column_definition}"
                                connection.execute(text(modify_stmt))

            logger.info("✅ 資料庫結構同步完成。")
        except SQLAlchemyError as e:
            logger.error("❌ 同步資料庫結構時發生錯誤: %s", e)
            raise


if __name__ == "__main__":
    try:
        Database()
    except ConnectionError as e:
        logger.error("初始化程序因資料庫連接問題而終止。")
