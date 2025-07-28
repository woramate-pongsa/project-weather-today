# 1. เลือก Base Image ที่มี Python version ตรงกับใน pyproject.toml (>=3.11)
FROM apache/airflow:2.9.2-python3.11

# --- ส่วนที่ต้องทำในฐานะ ROOT ---
USER root

# 2. ติดตั้ง System Dependencies ที่จำเป็น (เพิ่ม curl สำหรับดาวน์โหลด)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# --- ส่วนที่ต้องทำในฐานะ AIRFLOW USER ---
USER airflow

# 3. ติดตั้ง Poetry โดยใช้ Official Installer แทน pip
# วิธีนี้จะติดตั้ง poetry ไปที่ /home/airflow/.local/bin ซึ่งเป็นวิธีที่แนะนำและเสถียรที่สุด
RUN curl -sSL https://install.python-poetry.org | python3 -

# 4. เพิ่ม Path ของ poetry เข้าไปใน Environment Variable
# เพื่อให้ระบบสามารถหาคำสั่ง 'poetry' เจอในขั้นตอนถัดไป
ENV PATH="/home/airflow/.local/bin:${PATH}"

# 5. กำหนด Working Directory
WORKDIR /opt/airflow

# 6. คัดลอกไฟล์จัดการ dependency และตั้งค่า ownership ให้ถูกต้อง
# ใช้ --chown=airflow:root เพื่อให้แน่ใจว่า user 'airflow' เป็นเจ้าของไฟล์
COPY --chown=airflow:root pyproject.toml poetry.lock* ./

# 7. ใช้ Poetry ติดตั้ง dependencies ของโปรเจค (ยังคงเป็น user 'airflow')
RUN poetry config virtualenvs.create false && \
    poetry install --no-root --no-dev --no-interaction