"""
Flink Streaming Job: Чтение из Kafka, трансформация в модель "звезда", запись в PostgreSQL.
"""

import json
import logging
import traceback
from datetime import datetime

from pyflink.common import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
)
from pyflink.datastream.functions import MapFunction
from pyflink.common.serialization import SimpleStringSchema
import psycopg2

# --- Конфигурация ---
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "petstore_sales"
KAFKA_GROUP_ID = "flink-petstore-consumer"

POSTGRES_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "database": "petstore",
    "user": "petstore_user",
    "password": "petstore_pass",
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_db_connection():
    """Создать подключение к PostgreSQL."""
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    conn.autocommit = True
    return conn


def parse_date(value):
    """Парсинг даты из разных форматов. Возвращает 'YYYY-MM-DD' или None."""
    if not value:
        return None
    value = str(value).strip()
    if not value:
        return None
    
    # Форматы дат
    formats = [
        '%Y-%m-%d',     # 2021-02-27
        '%m/%d/%Y',     # 2/27/2021
        '%m/%d/%y',     # 2/27/21
        '%d/%m/%Y',     # 27/2/2021
        '%Y/%m/%d',     # 2021/02/27
        '%d-%m-%Y',     # 27-02-2021
        '%m-%d-%Y',     # 02-27-2021
    ]
    
    for fmt in formats:
        try:
            dt = datetime.strptime(value, fmt)
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            continue
    
    return None


def safe_int(value, default=None):
    """Безопасное приведение к int."""
    if value is None or value == '':
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def safe_float(value, default=None):
    """Безопасное приведение к float."""
    if value is None or value == '':
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def safe_str(value, default=""):
    """Безопасное приведение к строке."""
    if value is None:
        return default
    return str(value).strip()


class StarSchemaTransformer(MapFunction):
    """MapFunction для трансформации JSON в модель "звезда"."""

    def open(self, runtime_context):
        logger.info("Opening PostgreSQL connection...")
        self.conn = get_db_connection()
        logger.info("PostgreSQL connection established.")

    def close(self):
        if self.conn:
            self.conn.close()

    def map(self, value: str):
        try:
            record = json.loads(value)
            self.process_record(record)
        except Exception as e:
            logger.error(f"Error: {e}")
            logger.error(traceback.format_exc())
        return "ok"

    def process_record(self, rec: dict):
        cur = self.conn.cursor()

        # Извлечение полей
        customer_country = safe_str(rec.get("customer_country"))
        seller_country = safe_str(rec.get("seller_country"))
        store_country = safe_str(rec.get("store_country"))
        supplier_country = safe_str(rec.get("supplier_country"))

        store_city = safe_str(rec.get("store_city"))
        store_state = safe_str(rec.get("store_state"))
        supplier_city = safe_str(rec.get("supplier_city"))
        store_location = safe_str(rec.get("store_location"))
        store_phone = safe_str(rec.get("store_phone"))
        store_email = safe_str(rec.get("store_email"))
        supplier_address = safe_str(rec.get("supplier_address"))

        cust_postal = safe_str(rec.get("customer_postal_code"))
        seller_postal = safe_str(rec.get("seller_postal_code"))

        pet_type = safe_str(rec.get("customer_pet_type"))
        pet_breed = safe_str(rec.get("customer_pet_breed"))
        pet_category = safe_str(rec.get("pet_category"))
        pet_name = safe_str(rec.get("customer_pet_name"))

        product_category = safe_str(rec.get("product_category"))
        product_brand = safe_str(rec.get("product_brand"))
        product_name = safe_str(rec.get("product_name"))
        product_color = safe_str(rec.get("product_color"))
        product_size = safe_str(rec.get("product_size"))
        product_material = safe_str(rec.get("product_material"))
        product_description = safe_str(rec.get("product_description"))

        supplier_name = safe_str(rec.get("supplier_name"))
        supplier_contact = safe_str(rec.get("supplier_contact"))
        supplier_email = safe_str(rec.get("supplier_email"))
        supplier_phone = safe_str(rec.get("supplier_phone"))

        store_name = safe_str(rec.get("store_name"))

        cust_first = safe_str(rec.get("customer_first_name"))
        cust_last = safe_str(rec.get("customer_last_name"))
        cust_email = safe_str(rec.get("customer_email"))
        cust_age = safe_int(rec.get("customer_age"))

        seller_first = safe_str(rec.get("seller_first_name"))
        seller_last = safe_str(rec.get("seller_last_name"))
        seller_email = safe_str(rec.get("seller_email"))

        customer_id = safe_int(rec.get("sale_customer_id"))
        seller_id = safe_int(rec.get("sale_seller_id"))
        product_id = safe_int(rec.get("sale_product_id"))

        product_price = safe_float(rec.get("product_price"))
        product_weight = safe_float(rec.get("product_weight"))
        product_rating = safe_float(rec.get("product_rating"))
        product_reviews = safe_int(rec.get("product_reviews"))

        sale_date_str = parse_date(rec.get("sale_date"))
        product_release_date = parse_date(rec.get("product_release_date"))
        product_expiry_date = parse_date(rec.get("product_expiry_date"))

        sale_quantity = safe_int(rec.get("sale_quantity"), 0)
        sale_total_price = safe_float(rec.get("sale_total_price"), 0.0)

        # ========== dim_country ==========
        country_ids = {}
        for c_name in {customer_country, seller_country, store_country, supplier_country}:
            if not c_name:
                continue
            cur.execute(
                "INSERT INTO petstore_dw.dim_country (country_name) VALUES (%s) "
                "ON CONFLICT (country_name) DO UPDATE SET country_name = EXCLUDED.country_name "
                "RETURNING country_id",
                (c_name,),
            )
            country_ids[c_name] = cur.fetchone()[0]

        cust_country_id = country_ids.get(customer_country)
        seller_country_id = country_ids.get(seller_country)
        store_country_id = country_ids.get(store_country)
        supplier_country_id = country_ids.get(supplier_country)

        # ========== dim_city ==========
        city_ids = {}
        if store_city and store_country_id:
            cur.execute(
                "INSERT INTO petstore_dw.dim_city (city_name, state_name, country_id) VALUES (%s, %s, %s) "
                "ON CONFLICT (city_name, COALESCE(state_name, ''), country_id) "
                "DO UPDATE SET state_name = EXCLUDED.state_name "
                "RETURNING city_id",
                (store_city, store_state or None, store_country_id),
            )
            city_ids[("store", store_city)] = cur.fetchone()[0]

        if supplier_city and supplier_country_id:
            cur.execute(
                "INSERT INTO petstore_dw.dim_city (city_name, state_name, country_id) VALUES (%s, NULL, %s) "
                "ON CONFLICT (city_name, COALESCE(state_name, ''), country_id) "
                "DO UPDATE SET state_name = EXCLUDED.state_name "
                "RETURNING city_id",
                (supplier_city, supplier_country_id),
            )
            city_ids[("supplier", supplier_city)] = cur.fetchone()[0]

        # ========== dim_location ==========
        location_ids = {}
        if cust_country_id:
            cur.execute(
                "INSERT INTO petstore_dw.dim_location (postal_code, country_id) VALUES (%s, %s) "
                "RETURNING location_id",
                (cust_postal or None, cust_country_id),
            )
            location_ids["customer"] = cur.fetchone()[0]

        if seller_country_id:
            cur.execute(
                "INSERT INTO petstore_dw.dim_location (postal_code, country_id) VALUES (%s, %s) "
                "RETURNING location_id",
                (seller_postal or None, seller_country_id),
            )
            location_ids["seller"] = cur.fetchone()[0]

        store_city_id = city_ids.get(("store", store_city))
        if store_country_id:
            cur.execute(
                "INSERT INTO petstore_dw.dim_location (address, postal_code, city_id, country_id) "
                "VALUES (%s, NULL, %s, %s) RETURNING location_id",
                (store_location or None, store_city_id, store_country_id),
            )
            location_ids["store"] = cur.fetchone()[0]

        supplier_city_id = city_ids.get(("supplier", supplier_city))
        if supplier_country_id:
            cur.execute(
                "INSERT INTO petstore_dw.dim_location (address, city_id, country_id) VALUES (%s, %s, %s) "
                "RETURNING location_id",
                (supplier_address or None, supplier_city_id, supplier_country_id),
            )
            location_ids["supplier"] = cur.fetchone()[0]

        # ========== dim_date ==========
        date_id = None
        if sale_date_str:
            cur.execute(
                """
                INSERT INTO petstore_dw.dim_date (full_date, day, month, month_name, quarter, year, day_of_week, day_name)
                VALUES (%s,
                    EXTRACT(DAY FROM DATE %s)::SMALLINT,
                    EXTRACT(MONTH FROM DATE %s)::SMALLINT,
                    TO_CHAR(DATE %s, 'FMMonth'),
                    EXTRACT(QUARTER FROM DATE %s)::SMALLINT,
                    EXTRACT(YEAR FROM DATE %s)::SMALLINT,
                    EXTRACT(ISODOW FROM DATE %s)::SMALLINT,
                    TO_CHAR(DATE %s, 'FMDay'))
                ON CONFLICT (full_date) DO UPDATE SET full_date = EXCLUDED.full_date
                RETURNING date_id
                """,
                tuple([sale_date_str] * 8),
            )
            date_id = cur.fetchone()[0]

        # ========== dim_pet_breed ==========
        breed_id = None
        if pet_breed and pet_type:
            cur.execute(
                "INSERT INTO petstore_dw.dim_pet_breed (breed_name, pet_type_name, pet_category_name) "
                "VALUES (%s, %s, %s) "
                "ON CONFLICT (breed_name, pet_type_name) DO UPDATE SET pet_category_name = EXCLUDED.pet_category_name "
                "RETURNING breed_id",
                (pet_breed, pet_type, pet_category or "Unknown"),
            )
            breed_id = cur.fetchone()[0]

        pet_id = None
        if breed_id:
            cur.execute(
                "INSERT INTO petstore_dw.dim_pet (pet_name, breed_id) VALUES (%s, %s) "
                "RETURNING pet_id",
                (pet_name or None, breed_id),
            )
            pet_id = cur.fetchone()[0]

        # ========== dim_product_category ==========
        product_category_id = None
        if product_category:
            cur.execute(
                "INSERT INTO petstore_dw.dim_product_category (product_category_name) VALUES (%s) "
                "ON CONFLICT (product_category_name) DO UPDATE SET product_category_name = EXCLUDED.product_category_name "
                "RETURNING product_category_id",
                (product_category,),
            )
            product_category_id = cur.fetchone()[0]

        # ========== dim_brand ==========
        brand_id = None
        if product_brand:
            cur.execute(
                "INSERT INTO petstore_dw.dim_brand (brand_name) VALUES (%s) "
                "ON CONFLICT (brand_name) DO UPDATE SET brand_name = EXCLUDED.brand_name "
                "RETURNING brand_id",
                (product_brand,),
            )
            brand_id = cur.fetchone()[0]

        # ========== dim_product ==========
        if product_id and product_category_id:
            cur.execute(
                """
                INSERT INTO petstore_dw.dim_product (
                    product_id, product_name, product_category_id, brand_id,
                    price, weight, color, size, material, description,
                    rating, reviews, release_date, expiry_date
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (product_id) DO UPDATE SET
                    product_name = EXCLUDED.product_name,
                    product_category_id = EXCLUDED.product_category_id,
                    brand_id = EXCLUDED.brand_id,
                    price = EXCLUDED.price,
                    weight = EXCLUDED.weight,
                    color = EXCLUDED.color,
                    size = EXCLUDED.size,
                    material = EXCLUDED.material,
                    description = EXCLUDED.description,
                    rating = EXCLUDED.rating,
                    reviews = EXCLUDED.reviews,
                    release_date = EXCLUDED.release_date,
                    expiry_date = EXCLUDED.expiry_date
                """,
                (
                    product_id, product_name, product_category_id, brand_id,
                    product_price, product_weight,
                    product_color or None, product_size or None,
                    product_material or None, product_description or None,
                    product_rating, product_reviews,
                    product_release_date, product_expiry_date,
                ),
            )

        # ========== dim_customer ==========
        if customer_id:
            cur.execute(
                """
                INSERT INTO petstore_dw.dim_customer (customer_id, first_name, last_name, age, email, location_id, pet_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (customer_id) DO UPDATE SET
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    age = EXCLUDED.age,
                    email = EXCLUDED.email,
                    location_id = EXCLUDED.location_id,
                    pet_id = EXCLUDED.pet_id
                """,
                (
                    customer_id,
                    cust_first or None, cust_last or None,
                    cust_age, cust_email or None,
                    location_ids.get("customer"), pet_id,
                ),
            )

        # ========== dim_seller ==========
        if seller_id:
            cur.execute(
                """
                INSERT INTO petstore_dw.dim_seller (seller_id, first_name, last_name, email, location_id)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (seller_id) DO UPDATE SET
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    email = EXCLUDED.email,
                    location_id = EXCLUDED.location_id
                """,
                (
                    seller_id,
                    seller_first or None, seller_last or None,
                    seller_email or None, location_ids.get("seller"),
                ),
            )

        # ========== dim_store ==========
        store_id = None
        if store_name and location_ids.get("store"):
            cur.execute(
                """
                INSERT INTO petstore_dw.dim_store (store_name, phone, email, location_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (store_name, location_id) DO UPDATE SET
                    phone = EXCLUDED.phone,
                    email = EXCLUDED.email
                RETURNING store_id
                """,
                (
                    store_name,
                    store_phone or None, store_email or None,
                    location_ids.get("store"),
                ),
            )
            store_id = cur.fetchone()[0]

        # ========== dim_supplier ==========
        supplier_id = None
        if supplier_name:
            cur.execute(
                """
                INSERT INTO petstore_dw.dim_supplier (supplier_name, contact_name, email, phone, location_id)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (supplier_name, COALESCE(email, '')) DO UPDATE SET
                    contact_name = EXCLUDED.contact_name,
                    phone = EXCLUDED.phone,
                    location_id = EXCLUDED.location_id
                RETURNING supplier_id
                """,
                (
                    supplier_name,
                    supplier_contact or None, supplier_email or None,
                    supplier_phone or None, location_ids.get("supplier"),
                ),
            )
            supplier_id = cur.fetchone()[0]

        # ========== fact_sales ==========
        if all([date_id, customer_id, seller_id, product_id, store_id, supplier_id]):
            cur.execute(
                """
                INSERT INTO petstore_dw.fact_sales
                    (date_id, customer_id, seller_id, product_id, store_id, supplier_id, quantity, total_price)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    date_id, customer_id, seller_id, product_id,
                    store_id, supplier_id, sale_quantity, sale_total_price,
                ),
            )

        cur.close()


def main():
    logger.info("Starting Flink PetStore Streaming Job...")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        .set_topics(KAFKA_TOPIC)
        .set_group_id(KAFKA_GROUP_ID)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    ds = env.from_source(
        kafka_source,
        WatermarkStrategy.for_monotonous_timestamps(),
        "Kafka Source",
    )

    ds.map(StarSchemaTransformer()).name("Star Schema Transformer")

    logger.info("Submitting Flink job...")
    env.execute("Flink PetStore Streaming to PostgreSQL")


if __name__ == "__main__":
    main()