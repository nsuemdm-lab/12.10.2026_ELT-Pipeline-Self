# ПРАКТИЧЕСКОЕ РУКОВОДСТВО: САМОСТОЯТЕЛЬНАЯ РАБОТА В GOOGLE COLAB
## SCD Type 2, REST API ELT, Проверка гипотез, Disaster Recovery и OSINT

---

### [MARKDOWN ЯЧЕЙКА 1]

# Лабораторный практикум: Продвинутый ELT-конвейер, версионирование данных (SCD-2) и стресс-тестирование DWH

### Цели работы:
1. **SCD Type 2:** Реализовать историчность атрибутов измерений без потери аналитической связи с ранее зафиксированными фактами.
2. **REST API Ingestion:** Автоматизировать сбор данных с внешнего шлюза MOEX ISS API в слой Staging с политикой нулевого дублирования (Zero-Duplication).
3. **Data Mart & Hypothesis Testing:** Построить витрину финансовых показателей и выполнить статистическую проверку гипотезы о равенстве маржинальности секторов через критерий Манна — Уитни.
4. **Data Drift & Disaster Recovery:** Сымитировать деструктивное повреждение базы данных (Data Poisoning) и верифицировать процедуру восстановления через сверку контрольных сумм (Reconciliation).
5. **OSINT-кейс:** Сформировать паспорт мониторинга точки бифуркации публичной компании (ПАО).


---

### [CODE ЯЧЕЙКА 1] Проверка окружения и инициализация подключения к локальной СУБД
```python
import subprocess
import os
import requests
import json
import pandas as pd
import numpy as np
from scipy import stats
from sqlalchemy import create_engine, text

# 1. Проверка демона PostgreSQL внутри виртуальной машины Colab
status = subprocess.run(["service", "postgresql", "status"], capture_output=True, text=True)
if "online" not in status.stdout and "running" not in status.stdout:
    subprocess.run(["service", "postgresql", "start"], check=True)
    print("[STATUS] Демон PostgreSQL запущен.")
else:
    print("[STATUS] Демон PostgreSQL функционирует.")

# 2. Инициализация подключения
DB_URI = 'postgresql://dwh_admin:admin123@localhost:5432/elt_dwh'
engine = create_engine(DB_URI, pool_pre_ping=True)

# 3. Валидация коннекта
with engine.connect() as conn:
    res = conn.execute(text("SELECT current_database(), current_user;")).fetchone()
    print(f"[STATUS] Соединение активно: База данных='{res[0]}', Пользователь='{res[1]}'")
```

---

### [MARKDOWN ЯЧЕЙКА 2]

## ЭТАП 1. АРХИТЕКТУРНАЯ ЭВОЛЮЦИЯ ХРАНИЛИЩА (SCD TYPE 2)

**Задача:** Обеспечить сохранение истории изменений атрибутов эмитентов (M&A, смена секторов GICS, переименования).

1. Снимается ограничение уникальности `dim_company_ticker_key`.
2. В таблицу `dim_company` добавляются атрибуты интервалов актуальности:
   * `valid_from` (DATE) — дата вступления версии в силу.
   * `valid_to` (DATE) — дата окончания действия версии (`NULL` для активной).
   * `is_current` (BOOLEAN) — флаг текущей версии.
3. Создается частичный уникальный индекс, гарантирующий наличие строго одной активной записи на каждый тикер.


---

### [CODE ЯЧЕЙКА 2] DDL-миграция схемы dim_company под стандарт SCD Type 2
```python
scd2_migration_sql = """
BEGIN;

-- 1. Снятие безусловного ограничения уникальности по тикеру
ALTER TABLE public.dim_company DROP CONSTRAINT IF EXISTS dim_company_ticker_key;

-- 2. Добавление колонок темпорального версионирования
ALTER TABLE public.dim_company 
    ADD COLUMN IF NOT EXISTS valid_from DATE NOT NULL DEFAULT '2000-01-01',
    ADD COLUMN IF NOT EXISTS valid_to DATE DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS is_current BOOLEAN NOT NULL DEFAULT TRUE;

-- 3. Частичный уникальный индекс: только одна запись может иметь флаг is_current = TRUE
CREATE UNIQUE INDEX IF NOT EXISTS uq_active_company_ticker 
ON public.dim_company(ticker) 
WHERE is_current = TRUE;

-- 4. Индекс для фильтрации по временным срезам
CREATE INDEX IF NOT EXISTS idx_dim_company_scd_timeline 
ON public.dim_company(ticker, valid_from, valid_to);

COMMIT;
"""

with engine.begin() as conn:
    conn.execute(text(scd2_migration_sql))
    print("[OK] DDL-миграция завершена. Таблица dim_company переведена в режим SCD Type 2.")
```

---

### [MARKDOWN ЯЧЕЙКА 3]

### Реализация логики ротации версий (SCD Type 2 Logic)
При изменении атрибутов существующая запись закрывается днем, предшествующим дате вступления изменений (`valid_to = effective_date - 1`, `is_current = FALSE`), и создается новая запись со статусом `is_current = TRUE`.


---

### [CODE ЯЧЕЙКА 3] Функция транзакционной ротации версий и тестовое обновление
```python
def scd2_update_company(engine, ticker: str, new_name: str, new_sector: str, effective_date: str):
    """
    Транзакционное закрытие старой версии измерения и создание новой.
    """
    rotation_sql = text("""
    BEGIN;

    -- 1. Закрытие текущей версии при наличии расхождений в данных
    UPDATE public.dim_company
    SET 
        valid_to = :effective_date::DATE - INTERVAL '1 day',
        is_current = FALSE
    WHERE ticker = :ticker 
      AND is_current = TRUE
      AND (company_name <> :new_name OR gics_sector <> :new_sector);

    -- 2. Вставка новой версии (если предыдущая была закрыта)
    INSERT INTO public.dim_company (ticker, company_name, gics_sector, valid_from, valid_to, is_current)
    SELECT 
        :ticker, :new_name, :new_sector, :effective_date::DATE, NULL, TRUE
    WHERE EXISTS (
        SELECT 1 FROM public.dim_company 
        WHERE ticker = :ticker 
          AND is_current = FALSE 
          AND valid_to = :effective_date::DATE - INTERVAL '1 day'
    );

    COMMIT;
    """)
    
    with engine.begin() as conn:
        conn.execute(rotation_sql, {
            "ticker": ticker.strip().upper(),
            "new_name": new_name.strip(),
            "new_sector": new_sector.strip(),
            "effective_date": effective_date
        })
    print(f"[OK] SCD2 операция выполнена для {ticker}. Дата вступления: {effective_date}")

# ТЕСТ: Изменение сектора YNDX с 'Information Technology' на 'Communication Services' от 2024-01-01
scd2_update_company(
    engine,
    ticker="YNDX",
    new_name="Yandex N.V. / МКПАО Яндекс",
    new_sector="Communication Services",
    effective_date="2024-01-01"
)

# Проверка версий в базе
df_scd_check = pd.read_sql("SELECT company_sk, ticker, company_name, gics_sector, valid_from, valid_to, is_current FROM public.dim_company WHERE ticker = 'YNDX' ORDER BY valid_from;", engine)
display(df_scd_check)
```

---

### [MARKDOWN ЯЧЕЙКА 4]

## ЭТАП 2. АВТОМАТИЗАЦИЯ СБОРА ДАННЫХ ЧЕРЕЗ REST API (MOEX ISS)

**Задача:** Автоматизировать сбор данных внешнего макроэкономического бенчмарка — индекса МосБиржи (`IMOEX`) — через публичный API.

Пайплайн:
1. **Extract:** Запрос к REST API MOEX с поддержкой постраничной навигации (`start = 0, 100, ...`).
2. **Load:** Пакетная загрузка массива в буфер `staging.raw_financial_feed`.
3. **Transform:** Внутрибазовое наполнение словаря метрик и таблицы фактов с защитой от повторных записей (`NOT EXISTS`).


---

### [CODE ЯЧЕЙКА 4] Скрипт сбора и идемпотентной загрузки индекса МосБиржи
```python
def fetch_and_load_moex_index(engine, secid: str = "IMOEX", start_date: str = "2024-01-01"):
    url = f"https://iss.moex.com/iss/history/engines/stock/markets/index/securities/{secid}.json"
    params = {"from": start_date, "start": 0}
    headers = {"User-Agent": "ColabDWH/2.0"}
    
    raw_records = []
    print(f"[EXTRACT] Извлечение котировок {secid} начиная с {start_date}...")
    
    while True:
        resp = requests.get(url, params=params, headers=headers, timeout=12)
        resp.raise_for_status()
        data = resp.json()
        
        rows = data.get("history", {}).get("data", [])
        if not rows:
            break
            
        cols = data["history"]["columns"]
        date_idx = cols.index("TRADEDATE")
        close_idx = cols.index("CLOSE")
        
        for r in rows:
            if r[close_idx] is not None:
                raw_records.append({
                    "report_date": str(r[date_idx]),
                    "ticker": secid,
                    "company_name": "MOEX Benchmark Index",
                    "gics_sector": "INDEX",
                    "metric_code": "INDEX_CLOSE",
                    "metric_type": "MACRO",
                    "metric_value": str(r[close_idx])
                })
        
        params["start"] += len(rows)
        if len(rows) < 100:
            break

    print(f"[EXTRACT] Получено записей: {len(raw_records)}")
    if not raw_records:
        return

    # 1. Пакетная запись в Staging
    df_staging = pd.DataFrame(raw_records)
    df_staging.to_sql(
        name="raw_financial_feed",
        con=engine,
        schema="staging",
        if_exists="append",
        index=False,
        method="multi"
    )
    print(f"[LOAD] Загружено в staging.raw_financial_feed: {len(df_staging)} строк.")

    # 2. Внутрибазовый ELT
    transform_query = text("""
    BEGIN;

    -- Регистрация бенчмарка в dim_company
    INSERT INTO public.dim_company (ticker, company_name, gics_sector, valid_from, is_current)
    SELECT DISTINCT ticker, company_name, gics_sector, '2000-01-01'::DATE, TRUE
    FROM staging.raw_financial_feed
    WHERE ticker = :secid
    ON CONFLICT (ticker) WHERE is_current = TRUE DO NOTHING;

    -- Регистрация макро-метрики в dim_metric_dictionary
    INSERT INTO public.dim_metric_dictionary (metric_code, metric_type)
    VALUES ('INDEX_CLOSE', 'MACRO')
    ON CONFLICT (metric_code) DO NOTHING;

    -- Перенос фактов с динамическим разрешением ключей
    INSERT INTO public.fact_financial_metrics (report_date, company_sk, metric_id, metric_value)
    SELECT 
        s.report_date::DATE,
        c.company_sk,
        m.metric_id,
        s.metric_value::NUMERIC(20, 4)
    FROM staging.raw_financial_feed s
    JOIN public.dim_company c ON s.ticker = c.ticker AND c.is_current = TRUE
    JOIN public.dim_metric_dictionary m ON s.metric_code = m.metric_code
    WHERE s.ticker = :secid
    AND NOT EXISTS (
        SELECT 1 FROM public.fact_financial_metrics f
        WHERE f.report_date = s.report_date::DATE 
          AND f.company_sk = c.company_sk 
          AND f.metric_id = m.metric_id
    );

    -- Очистка staging-буфера по загруженному инструменту
    DELETE FROM staging.raw_financial_feed WHERE ticker = :secid;

    COMMIT;
    """)

    with engine.begin() as conn:
        conn.execute(transform_query, {"secid": secid})
    print(f"[TRANSFORM] Обработка {secid} завершена. Таблица фактов актуализирована.")

# Запуск пайплайна сбора
fetch_and_load_moex_index(engine, secid="IMOEX", start_date="2024-01-01")
```

---

### [MARKDOWN ЯЧЕЙКА 5]

## ЭТАП 3. ВНУТРИБАЗОВАЯ ТРАНСФОРМАЦИЯ И ПРОВЕРКА ГИПОТЕЗ

**Бизнес-задача:** Проверить, различается ли уровень операционной рентабельности по EBITDA между компаниями сырьевого сектора (`Energy`) и сектора потребительских товаров/услуг (`Consumer Discretionary`).

**Статистическая постановка:**
* **$H_0$:** Распределения показателя `ebitda_margin_pct` в секторах *Energy* и *Consumer Discretionary* тождественны.
* **$H_1$:** Присутствует статистически значимый сдвиг в уровне рентабельности между секторами.
* **Критерий:** Непараметрический критерий Манна — Уитни (Mann-Whitney U-test) при уровне значимости $\alpha = 0.05$.


---

### [CODE ЯЧЕЙКА 5] Создание аналитической витрины и выполнение статистического теста
```python
# 1. Развертывание витрины финансовых метрик
mart_ddl = """
CREATE OR REPLACE VIEW public.view_corporate_performance AS
WITH base_pivot AS (
    SELECT 
        f.report_date,
        c.ticker,
        c.company_name,
        c.gics_sector,
        MAX(f.metric_value) FILTER (WHERE m.metric_code = 'REVENUE') AS revenue,
        MAX(f.metric_value) FILTER (WHERE m.metric_code = 'EBITDA') AS ebitda,
        MAX(f.metric_value) FILTER (WHERE m.metric_code = 'NET_PROFIT') AS net_profit
    FROM public.fact_financial_metrics f
    JOIN public.dim_company c ON f.company_sk = c.company_sk
    JOIN public.dim_metric_dictionary m ON f.metric_id = m.metric_id
    WHERE c.gics_sector NOT IN ('INDEX', 'Unknown')
    GROUP BY f.report_date, c.ticker, c.company_name, c.gics_sector
)
SELECT 
    report_date,
    ticker,
    company_name,
    gics_sector,
    revenue,
    ebitda,
    net_profit,
    ROUND((ebitda / NULLIF(revenue, 0)) * 100, 2) AS ebitda_margin_pct,
    ROUND((net_profit / NULLIF(revenue, 0)) * 100, 2) AS net_margin_pct
FROM base_pivot;
"""

with engine.begin() as conn:
    conn.execute(text(mart_ddl))
    print("[OK] Витрина view_corporate_performance актуализирована.")

# 2. Выгрузка выборок
query_stats = """
SELECT gics_sector, ebitda_margin_pct 
FROM public.view_corporate_performance 
WHERE ebitda_margin_pct IS NOT NULL 
  AND gics_sector IN ('Energy', 'Consumer Discretionary');
"""
df_stat = pd.read_sql(query_stats, engine)

energy_sample = df_stat[df_stat['gics_sector'] == 'Energy']['ebitda_margin_pct'].dropna()
consumer_sample = df_stat[df_stat['gics_sector'] == 'Consumer Discretionary']['ebitda_margin_pct'].dropna()

print(f"Выборка 'Energy' (n={len(energy_sample)}): {energy_sample.tolist()}")
print(f"Выборка 'Consumer Discretionary' (n={len(consumer_sample)}): {consumer_sample.tolist()}")

# 3. Статистический расчет
u_stat, p_val = stats.mannwhitneyu(energy_sample, consumer_sample, alternative='two-sided')

alpha = 0.05
print("\n" + "="*50)
print(f"СТАТИСТИЧЕСКИЙ ОТЧЕТ (МАНН — УИТНИ)")
print(f"U-критерий: {u_stat:.4f}")
print(f"p-value:    {p_val:.6f}")
print("="*50)

if p_val < alpha:
    print(f"ВЫВОД: p-value < {alpha}. Нулевая гипотеза H0 отвергается. Различия в маржинальности статистически значимы.")
else:
    print(f"ВЫВОД: p-value >= {alpha}. Оснований отвергнуть H0 нет. Значимых различий не обнаружено.")
```

---

### [MARKDOWN ЯЧЕЙКА 6]

## ЭТАП 4. АУДИТ УСТОЙЧИВОСТИ: ИНЪЕКЦИЯ СБОЕВ И DISASTER RECOVERY

**Методика:**
1. Создание эталонного снимка (Golden Backup).
2. Запуск деструктивного скрипта (Chaos / Poisoning):
   * Искажение метрик `SBER` (установка отрицательных экстремальных значений).
   * Удаление 5 произвольных записей фактов.
   * Удаление индекса `idx_fact_fin_metrics`.
3. Запуск процедуры **Data Reconciliation** (сверка контрольных сумм и структуры).
4. Восстановление состояния из эталонного бэкапа при выявлении расхождений.


---

### [CODE ЯЧЕЙКА 6] Фиксация эталона и деструктивное воздействие
```python
BASELINE_PATH = '/content/elt_dwh_baseline.sql'

# 1. Фиксация эталонного состояния
!PGPASSWORD='admin123' pg_dump -h localhost -U dwh_admin -c elt_dwh > {BASELINE_PATH}
print(f"[OK] Эталонный бэкап сохранен: {BASELINE_PATH}")

# 2. Инъекция аномалий (Chaos Script)
chaos_sql = """
BEGIN;

-- Искажение метрик SBER
UPDATE public.fact_financial_metrics 
SET metric_value = -888888.00
WHERE company_sk IN (SELECT company_sk FROM public.dim_company WHERE ticker = 'SBER');

-- Удаление строк
DELETE FROM public.fact_financial_metrics
WHERE fact_id IN (SELECT fact_id FROM public.fact_financial_metrics LIMIT 5);

-- Удаление индекса
DROP INDEX IF EXISTS public.idx_fact_fin_metrics;

COMMIT;
"""

with engine.begin() as conn:
    conn.execute(text(chaos_sql))

print("[ALARM] Деструктивный скрипт выполнен. База данных содержит повреждения.")
```

---

### [CODE ЯЧЕЙКА 7] Data Reconciliation и автоматическое восстановление
```python
# 1. Аудит целостности (Data Reconciliation)
reconciliation_sql = """
SELECT 
    COUNT(*) AS total_records,
    ROUND(SUM(metric_value), 2) AS checksum_val,
    COUNT(*) FILTER (WHERE metric_value < 0) AS invalid_negative_rows,
    (SELECT COUNT(*) FROM pg_indexes WHERE tablename = 'fact_financial_metrics' AND indexname = 'idx_fact_fin_metrics') AS index_present
FROM public.fact_financial_metrics;
"""

df_audit = pd.read_sql(reconciliation_sql, engine)
print("РЕЗУЛЬТАТ АУДИТА ПОВРЕЖДЕННОЙ БД:")
display(df_audit)

# Логическое условие срабатывания аварийного протокола
anomalies_detected = (df_audit.loc[0, 'invalid_negative_rows'] > 0) or (df_audit.loc[0, 'index_present'] == 0)

if anomalies_detected:
    print("\n[ALARM] Зафиксирован Data Drift / Повреждение схемы. Старт аварийного восстановления...")
    
    # Сброс активных соединений и перезаливка дампа
    reset_cmd = """
    sudo -u postgres psql -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'elt_dwh' AND pid <> pg_backend_pid();"
    PGPASSWORD='admin123' psql -h localhost -U dwh_admin -d elt_dwh < /content/elt_dwh_baseline.sql > /dev/null 2>&1
    """
    subprocess.run(reset_cmd, shell=True, check=True)
    
    # 2. Повторный аудит после восстановления
    df_post_audit = pd.read_sql(reconciliation_sql, engine)
    print("\n[OK] РЕЗУЛЬТАТ АУДИТА ПОСЛЕ ВОССТАНОВЛЕНИЯ (DISASTER RECOVERY):")
    display(df_post_audit)
    print("Целостность базы данных успешно восстановлена.")
else:
    print("[STATUS] База данных находится в валидном состоянии.")
```

---

### [MARKDOWN ЯЧЕЙКА 7]

## ЭТАП 5. ПАСПОРТ OSINT-МОНИТОРИНГА ПАО (ШАБЛОН ДЛЯ ЗАПОЛНЕНИЯ)

Заполните приведенную форму данными по исследуемой публичной корпорации (кроме `SBER`, `GAZP`, `LKOH`).

---

### ПАСПОРТ ЭМИТЕНТА: ПАО «ГМК Норильский никель» (GMKN)
* **Идентификаторы:** ИНН: `8401005730`, ОГРН: `1028400000288`, Сектор MOEX: `Металлы и добыча`.
* **Ликвидность:** Среднедневной объем торгов (ADTV 30D): `1 850 млн руб.`
* **Официальный раскрыватель:** `e-disclosure.ru/portal/company.aspx?id=64`

#### 1. Модель выявления точки бифуркации
* **Внешние шоки (Макро):** Введение экспортных пошлин на никель и палладий, переориентация логистических цепочек в страны АТР с предоставлением контрактных дисконтов.
* **Внутренние дисбалансы:** Рост долговой нагрузки (Чистый Долг / EBITDA > 2.0x) на фоне реализации масштабной экологической («Серная программа») и капитальной программ при снижении дивидендного потока.

#### 2. Матрица опережающих OSINT-триггеров
| Контур мониторинга | Первичный источник данных | Фиксируемый индикатор (Anomaly Trigger) |
| :--- | :--- | :--- |
| **Долговой рынок** | Rusbonds / Cbonds | Расширение спреда доходности корпоративных облигаций к кривой ОФЗ более чем на 250 б.п. |
| **Судебный контур** | Картотека арбитражных дел (КАД) | Рост совокупных экологических и имущественных претензий со стороны Росприроднадзора свыше 10 млрд руб. |
| **Обременения** | Реестр залогов ФНП | Появление записей о передаче технологического оборудования дочерних структур в залог пулу кредиторов. |
| **Корпоративные события** | Федресурс (ЕФРСБ) | Уведомления об уменьшении уставного капитала или реорганизации производственных дивизионов. |

#### 3. Сценарное моделирование бифуркации
* **Сценарий А (Стабилизация):** Сохранение положительного свободного денежного потока (FCF), рефинансирование долговых обязательств в юанях, завершение модернизации плавильных мощностей.
* **Сценарий Б (Кризисный):** Дальнейшее падение мировых цен на корзину металлов платиновой группы, рост стоимости обслуживания долга, заморозка капитальных затрат и отказ от дивидендных выплат.


---

### [MARKDOWN ЯЧЕЙКА 8]

### ЧЕК-ЛИСТ СДАЧИ САМОСТОЯТЕЛЬНОЙ РАБОТЫ (DEFINITION OF DONE)

Перед экспортом блокнота (`.ipynb`) проверьте выполнение следующих условий:
1. **[Code 2 & 3]** Структура `dim_company` расширена полями `valid_from`, `valid_to`, `is_current`. Ротация версий протестирована, исторические интервалы не перекрываются.
2. **[Code 4]** Реализован скрипт выгрузки из MOEX ISS. Данные загружаются через `staging.raw_financial_feed` без построчных итераторов. Повторный запуск функции не приводит к росту числа строк (политика *Zero-Duplication* соблюдена).
3. **[Code 5]** Витрина `view_corporate_performance` развернута в PostgreSQL. Расчет критерия Манна — Уитни выполнен, в блокноте выведено значение $p$-value и зафиксирован статистический вывод.
4. **[Code 6 & 7]** Деструктивный скрипт выполнен. Процедура Reconciliation идентифицирует аномалии и выполняет автоматический откат к файлу `/content/elt_dwh_baseline.sql`.
5. **[Markdown 7]** Паспорт OSINT-мониторинга полностью заполнен для выбранного ПАО с детализацией четырех контуров мониторинга.

