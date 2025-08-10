from datetime import datetime
from sqlalchemy import create_engine, ForeignKey, UniqueConstraint, Table
from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    String,
    Text,
    MetaData,
    Integer,
    inspect,
)
import logging
from sqlalchemy.exc import SQLAlchemyError

from .config import (
    MYSQL_ACCOUNT,
    MYSQL_HOST,
    MYSQL_PASSWORD,
    MYSQL_PORT,
    MYSQL_DATABASE,
)

logger = logging.getLogger(__name__)


class Database:
    def __init__(self):
        self.engine = self.create_database_connection()
        if self.engine is None:
            logger.error("無法建立資料庫連接")
            return

        self.metadata = MetaData()
        self._create_tables()

    def create_database_connection(self):
        try:
            address = f"mysql+pymysql://{MYSQL_ACCOUNT}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
            engine = create_engine(address)
            with engine.connect() as conn:
                conn.execute("SELECT 1")
            logger.info("✅ 資料庫連接成功")
            return engine
        except SQLAlchemyError as e:
            logger.warning("⚠️ 無法連接到資料庫 %s，嘗試自動創建: %s", MYSQL_DATABASE, e)
            try:
                server_address = f"mysql+pymysql://{MYSQL_ACCOUNT}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}"
                server_engine = create_engine(server_address)
                with server_engine.connect() as conn:
                    conn.execute(
                        f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DATABASE}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                    )
                    logger.info("✅ 資料庫 %s 創建成功", MYSQL_DATABASE)
                server_engine.dispose()
                address = f"mysql+pymysql://{MYSQL_ACCOUNT}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
                engine = create_engine(address)
                with engine.connect() as conn:
                    conn.execute("SELECT 1")
                logger.info("✅ 成功連接到新創建的資料庫: %s", MYSQL_DATABASE)
                return engine
            except SQLAlchemyError as create_error:
                logger.error("❌ 自動創建資料庫失敗: %s", create_error)
                return None

    def _create_tables(self):
        try:
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
                Column("category_id", String(200), nullable=False),
                Column("category_name", Text),
                Column("created_at", DateTime, default=datetime.now),
                Column("updated_at", DateTime, default=datetime.now, onupdate=datetime.now),
            ]

            jobs_categories_columns = [
                Column("id", BigInteger, primary_key=True, autoincrement=True),
                Column("category_id", BigInteger, ForeignKey("categories.id", ondelete="CASCADE"), nullable=False),
                Column("job_id", BigInteger, ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False),
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
                Column("job_id", BigInteger, ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False),
                Column("skill_id", BigInteger, ForeignKey("skills.id", ondelete="CASCADE"), nullable=False),
                Column("created_at", DateTime, default=datetime.now),
                Column("updated_at", DateTime, default=datetime.now, onupdate=datetime.now),
            ]

            self.jobs_table = Table("jobs", self.metadata, *jobs_columns)
            self.categories_table = Table("categories", self.metadata, *categories_columns, UniqueConstraint('category_id'))
            self.jobs_categories_table = Table("jobs_categories", self.metadata, *jobs_categories_columns, UniqueConstraint('job_id', 'category_id', name='uix_job_category'))
            self.skills_table = Table("skills", self.metadata, *skills_columns)
            self.jobs_skills_table = Table("jobs_skills", self.metadata, *jobs_skills_columns, UniqueConstraint('job_id', 'skill_id', name='uix_job_skill'))

            self.metadata.create_all(self.engine)
            self._update_table("jobs", jobs_columns)
            self._update_table("categories", categories_columns)
            self._update_table("jobs_categories", jobs_categories_columns)
            self._update_table("skills", skills_columns)
            self._update_table("jobs_skills", jobs_skills_columns)

            logger.info("✅ 資料表建立/檢查完成")
        except SQLAlchemyError as e:
            logger.error("❌ 建立資料表失敗: %s", e)

    def _update_table(self, table_name, new_columns):
        try:
            with self.engine.connect() as connection:
                inspector = inspect(self.engine)
                if not inspector.has_table(table_name):
                    logger.warning("資料表 %s 不存在，無法更新", table_name)
                    return False
                existing_columns = inspector.get_columns(table_name)
                existing_column_names = {col["name"] for col in existing_columns}
                for new_col in new_columns:
                    if new_col.name not in existing_column_names:
                        alter_stmt = f"ALTER TABLE {table_name} ADD COLUMN {new_col.compile(dialect=self.engine.dialect)}"
                        connection.execute(alter_stmt)
                        logger.info("✅ 已新增欄位: %s 到表格 %s", new_col.name, table_name)
                for new_col in new_columns:
                    if new_col.name in existing_column_names:
                        existing_col = next(col for col in existing_columns if col["name"] == new_col.name)
                        if str(existing_col["type"]) != str(new_col.type):
                            alter_stmt = f"ALTER TABLE {table_name} MODIFY COLUMN {new_col.compile(dialect=self.engine.dialect)}"
                            connection.execute(alter_stmt)
                            logger.info("✅ 已更新欄位類型: %s 在表格 %s", new_col.name, table_name)
                connection.commit()
                logger.info("✅ 表格 %s 結構更新完成", table_name)
                return True
        except SQLAlchemyError as e:
            logger.error("❌ 更新表格結構時發生錯誤: %s", e)
            return False
