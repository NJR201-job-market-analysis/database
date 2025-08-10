import logging
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

# 假設您已經在某處初始化了 Database 實例
# from .init_db import Database
# db = Database()

logger = logging.getLogger(__name__)


def add_job(db, job_details, skills_names, categories_data):
    """
    在單一交易中，插入一筆職缺及其關聯的分類和技能。

    :param db: 從 init_db.py 初始化的 Database 物件。
    :param job_details: 一個包含職缺主要資訊的字典。
    :param skills_names: 一個包含技能名稱字串的列表 (e.g., ['Python', 'SQL'])。
    :param categories_data: 一個包含多個分類字典的列表。
    :return: 新增職缺的 ID，若失敗則回傳 None。
    """
    with db.engine.connect() as conn:
        # conn.begin() 會開啟一個交易，並在區塊結束時自動 commit，
        # 或在發生錯誤時自動 rollback。
        with conn.begin() as transaction:
            try:
                # 步驟 1: 插入職缺並取得 ID
                job_insert_stmt = db.jobs_table.insert().values(**job_details)
                result = conn.execute(job_insert_stmt)
                job_id = result.inserted_primary_key[0]
                logger.info(
                    "新增職缺 '%s'，ID 為: %s", job_details.get("job_title"), job_id
                )

                for skill_name in skills_names:
                    # 查詢技能是否存在
                    select_skill_stmt = select(db.skills_table.c.id).where(
                        db.skills_table.c.name == skill_name
                    )
                    skill_row = conn.execute(select_skill_stmt).first()

                    if skill_row:
                        skill_id = skill_row.id
                    else:
                        # 建立新技能
                        insert_skill_stmt = db.skills_table.insert().values(
                            name=skill_name
                        )
                        skill_result = conn.execute(insert_skill_stmt)
                        skill_id = skill_result.inserted_primary_key[0]
                        logger.info("新增技能 '%s'，ID 為: %s", skill_name, skill_id)

                    # 建立職缺與技能的關聯
                    try:
                        insert_job_skill_stmt = db.jobs_skills_table.insert().values(
                            job_id=job_id, skill_id=skill_id
                        )
                        conn.execute(insert_job_skill_stmt)
                    except IntegrityError:
                        # 如果因為唯一約束而失敗，表示關聯已存在，可以安全地忽略
                        logger.warning(
                            "職缺 %s 和技能 %s 的關聯已存在，跳過。", job_id, skill_id
                        )

                for cat_data in categories_data:
                    # 根據唯一約束查詢分類是否存在
                    select_cat_stmt = select(db.categories_table.c.id).where(
                        (db.categories_table.c.platform == cat_data["platform"])
                        & (db.categories_table.c.category_id == cat_data["category_id"])
                        & (
                            db.categories_table.c.sub_category_id
                            == cat_data["sub_category_id"]
                        )
                    )
                    cat_row = conn.execute(select_cat_stmt).first()

                    if cat_row:
                        category_id = cat_row.id
                    else:
                        # 建立新分類
                        insert_cat_stmt = db.categories_table.insert().values(
                            **cat_data
                        )
                        cat_result = conn.execute(insert_cat_stmt)
                        category_id = cat_result.inserted_primary_key[0]
                        logger.info(
                            "新增分類 '%s'，ID 為: %s",
                            cat_data.get("sub_category_name"),
                            category_id,
                        )

                    # 建立職缺與分類的關聯
                    try:
                        insert_job_cat_stmt = db.jobs_categories_table.insert().values(
                            job_id=job_id, category_id=category_id
                        )
                        conn.execute(insert_job_cat_stmt)
                    except IntegrityError:
                        logger.warning(
                            "職缺 %s 和分類 %s 的關聯已存在，跳過。",
                            job_id,
                            category_id,
                        )

                logger.info("✅ 成功寫入職缺 %s 及其所有關聯", job_id)
                # 交易區塊順利結束，會自動 commit
                return job_id

            except SQLAlchemyError as e:
                logger.error("❌ 寫入職缺時發生錯誤，交易已復原: %s", e)
                # 交易區塊發生錯誤，會自動 rollback
                return None


# --- 使用範例 ---
def main_example():
    # 假設這是您的 Database 物件
    # from database.init_db import Database
    # db = Database()

    # 為了能獨立執行，這裡做一個假的 db 物件
    from unittest.mock import MagicMock

    db = MagicMock()

    # 1. 準備您的職缺資料
    job_data = {
        "job_title": "前端工程師",
        "company_name": "範例科技",
        "job_url": "https://example.com/job/frontend-engineer-123",
        "platform": "cakeresume",
        # ... 其他職缺欄位
    }

    # 2. 準備技能列表
    skills = ["JavaScript", "TypeScript", "React"]

    # 3. 準備分類資料
    # 您的 categories 表有比較詳細的欄位，所以我們用字典列表來表示
    categories = [
        {
            "platform": "cakeresume",
            "category_id": "tech-frontend",
            "category_name": "技術",
            "sub_category_id": "frontend-dev",
            "sub_category_name": "前端工程師",
        },
        {
            "platform": "cakeresume",
            "category_id": "tech-all",
            "category_name": "技術",
            "sub_category_id": "software-eng",
            "sub_category_name": "軟體工程師",
        },
    ]

    # 4. 呼叫函數
    # 假設 db 物件已正確初始化
    # add_job_with_details(db, job_data, skills, categories)
    print("範例資料準備完成，您可以將 `add_job_with_details` 函數整合到您的專案中。")


if __name__ == "__main__":
    # 設定基本的 logging 以便看到過程
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    main_example()
