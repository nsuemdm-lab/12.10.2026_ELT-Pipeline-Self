# МЕТОДИЧЕСКОЕ ПОСОБИЕ ПО ВЫПОЛНЕНИЮ САМОСТОЯТЕЛЬНОЙ РАБОТЫ

**Тема:** Проектирование версионируемых хранилищ данных (SCD Type 2), автоматизация ELT-конвейеров с внешними REST API, статистический анализ финансовых точек бифуркации и аудит устойчивости (Disaster Recovery).  
**Среда исполнения:** Google Colab (виртуальная машина Ubuntu, локальный демон PostgreSQL 14+).  
**Параметры подключения:** `postgresql://dwh_admin:admin123@localhost:5432/elt_dwh`.

---

## 1. СТРУКТУРНО-МЕТОДИЧЕСКИЙ ПЛАН

Работа состоит из 5 последовательных инженерно-аналитических модулей:
1. **Модуль 1 (SCD Type 2):** Модификация структуры `dim_company` для отслеживания версионности изменений атрибутов корпораций во времени.
2. **Модуль 2 (Automated Ingestion):** Разработка конвейера сбора данных с внешнего REST API (MOEX ISS / ЦБ РФ) с записью в макроэкономические метрики.
3. **Модуль 3 (Pushdown Transform & Hypothesis Testing):** Построение аналитической SQL-витрины и статистическая проверка гипотез через непараметрические критерии в `scipy.stats`.
4. **Модуль 4 (Data Drift Injection & DR Audit):** Моделирование преднамеренного сбоя данных и верификация консистентности через сверку контрольных сумм.
5. **Модуль 5 (OSINT Case Study):** Оформление аналитического паспорта ПАО с фиксацией индикаторов точки бифуркации.

---

## 2. МОДУЛЬ 1: РЕАЛИЗАЦИЯ МЕХАНИЗМА SCD TYPE 2

### 2.1. Теоретический базис
В модели SCD Type 1 (Slowly Changing Dimensions) обновление атрибута (например, смена отрасли или юрисдикции) перезаписывает текущую строку, уничтожая ретроспективу. Это приводит к искажению исторических отчетов. 

В модели **SCD Type 2** изменение любого отслеживаемого версионируемого поля порождает новую запись с новым суррогатным ключом (`company_sk`), при этом:
* Предыдущая запись закрывается: `valid_to` выставляется в дату изменения, флаг `is_current` переводится в `FALSE`.
* Новая запись открывается: `valid_from` равен дате изменения, `valid_to` равен `NULL` (или технической бесконечности `9999-12-31`), `is_current = TRUE`.
* Таблица фактов хранит `company_sk`, связывающий транзакцию именно с той версией атрибутов, которая действовала на момент `report_date`.

### 2.2. Задание 1.1: Миграция DDL схемы

Выполните миграцию существующей схемы `dim_company`. Ограничение `UNIQUE(ticker)` должно быть заменено составным уникальным индексом или снято, так как один эмитент теперь представлен несколькими строками.

```sql
BEGIN;

-- 1. Удаление старого уникального ограничения по тикеру
ALTER TABLE public.dim_company DROP CONSTRAINT IF EXISTS dim_company_ticker_key;

-- 2. Добавление колонок темпорального версионирования
ALTER TABLE public.dim_company 
    ADD COLUMN IF NOT EXISTS valid_from DATE NOT NULL DEFAULT '2000-01-01',
    ADD COLUMN IF NOT EXISTS valid_to DATE DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS is_current BOOLEAN NOT NULL DEFAULT TRUE;

-- 3. Создание частичного уникального индекса: только одна строка по тикеру может быть активной
CREATE UNIQUE INDEX IF NOT EXISTS uq_active_company_ticker 
ON public.dim_company(ticker) 
WHERE is_current = TRUE;

-- 4. Индекс для оптимизации поиска исторических срезов
CREATE INDEX IF NOT EXISTS idx_dim_company_scd_lookup 
ON public.dim_company(ticker, valid_from, valid_to);

COMMIT;
```

### 2.3. Задание 1.2: Реализация транзакционного закрытия и открытия версий (Python/SQL)

Реализуйте функцию `scd2_update_company`, которая принимает измененные атрибуты компании и выполняет ротацию версий.

```python
from sqlalchemy import text

def scd2_update_company(engine, ticker: str, new_company_name: str, new_sector: str, effective_date: str):
    """
    Выполняет ротацию версий SCD Type 2 в рамках единой транзакции.
    """
    scd_transaction_sql = text("""
    BEGIN;

    -- Проверяем, изменились ли атрибуты у текущей активной записи
    WITH current_record AS (
        SELECT company_sk, company_name, gics_sector
        FROM public.dim_company
        WHERE ticker = :ticker AND is_current = TRUE
        FOR UPDATE
    )
    -- 1. Закрываем старую версию, если зафиксировано изменение атрибутов
    UPDATE public.dim_company
    SET 
        valid_to = :effective_date::DATE - INTERVAL '1 day',
        is_current = FALSE
    WHERE company_sk IN (
        SELECT company_sk FROM current_record
        WHERE company_name <> :new_company_name OR gics_sector <> :new_sector
    );

    -- 2. Вставляем новую версию, если предыдущая была закрыта
    INSERT INTO public.dim_company (ticker, company_name, gics_sector, valid_from, valid_to, is_current)
    SELECT 
        :ticker, :new_company_name, :new_sector, :effective_date::DATE, NULL, TRUE
    WHERE EXISTS (
        SELECT 1 FROM public.dim_company 
        WHERE ticker = :ticker AND is_current = FALSE AND valid_to = :effective_date::DATE - INTERVAL '1 day'
    );

    COMMIT;
    """)

    with engine.begin() as conn:
        conn.execute(scd_transaction_sql, {
            "ticker": ticker.strip().upper(),
            "new_company_name": new_company_name.strip(),
            "new_sector": new_sector.strip(),
            "effective_date": effective_date
        })
    print(f"[OK] SCD2 транзакция обработана для эмитента {ticker} от даты {effective_date}")

# ТЕСТОВЫЙ ВЫЗОВ: Смена сектора YNDX с IT на Communication Services
scd2_update_company(
    engine, 
    ticker="YNDX", 
    new_company_name="Yandex N.V. / МКПАО Яндекс", 
    new_sector="Communication Services", 
    effective_date="2024-01-01"
)
```

### 2.4. Верификация исторического JOIN
Проверьте корректность сопоставления фактов с измерениями во времени:

```sql
SELECT 
    f.report_date,
    c.ticker,
    c.gics_sector AS historical_sector,
    c.valid_from,
    c.valid_to,
    c.is_current,
    m.metric_code,
    f.metric_value
FROM public.fact_financial_metrics f
JOIN public.dim_company c 
    ON f.company_sk = c.company_sk
JOIN public.dim_metric_dictionary m 
    ON f.metric_id = m.metric_id
WHERE c.ticker = 'YNDX'
ORDER BY f.report_date;
```
*Критерий успеха:* Исторические факты за 2023 год ссылаются на `company_sk` со старым сектором, а факты после `2024-01-01` — на новую запись.

---

## 3. МОДУЛЬ 2: АВТОМАТИЗАЦИЯ INGESTION ЧЕРЕЗ REST API

### 3.1. Задание 2.1: Сбор макроэкономических индикаторов (Extract)
Необходимо организовать автоматизированную выгрузку индекса RGBI (индекс государственных облигаций Московской биржи) или индекса МосБиржи через MOEX ISS API в качестве внешнего фактора среды.

```python
import requests
import json
import pandas as pd
from sqlalchemy import text

def fetch_moex_market_index(secid: str = "IMOEX", start_date: str = "2023-01-01") -> list:
    """
    Извлекает исторические значения индекса через публичный REST API Московской Биржи.
    """
    url = f"https://iss.moex.com/iss/history/engines/stock/markets/index/securities/{secid}.json"
    params = {"from": start_date, "start": 0}
    headers = {"User-Agent": "AcademicELT/2.0"}

    records = []
    while True:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        history_data = data.get("history", {}).get("data", [])
        if not history_data:
            break
            
        columns = data["history"]["columns"]
        date_idx = columns.index("TRADEDATE")
        close_idx = columns.index("CLOSE")
        
        for row in history_data:
            trade_date = row[date_idx]
            close_val = row[close_idx]
            if close_val is not None:
                records.append({
                    "report_date": trade_date,
                    "ticker": secid,
                    "company_name": "MOEX Benchmark Index",
                    "gics_sector": "INDEX",
                    "metric_code": "INDEX_CLOSE",
                    "metric_type": "MACRO",
                    "metric_value": str(close_val)
                })
        
        # Сдвиг курсора пагинации (MOEX отдает блоки по 100 строк)
        params["start"] += len(history_data)
        if len(history_data) < 100:
            break

    print(f"[OK] Извлечено {len(records)} записей по индексу {secid}")
    return records
```

### 3.2. Задание 2.2: Идемпотентная загрузка в DWH (Load & Transform)
Загрузка должна производиться через буфер `staging.raw_financial_feed` с последующей векторной обработкой:

```python
def ingest_macro_indicators_to_dwh(engine, macro_data: list):
    """
    Загружает макро-показатели в DWH с динамической регистрацией в словарях.
    """
    df_macro = pd.DataFrame(macro_data)
    
    # 1. Загрузка в Staging
    df_macro.to_sql(
        name="raw_financial_feed",
        con=engine,
        schema="staging",
        if_exists="append",
        index=False,
        method="multi"
    )

    # 2. Внутрибазовый ELT
    ingest_sql = text("""
    BEGIN;

    -- Регистрация фиктивного эмитента/индекса в dim_company
    INSERT INTO public.dim_company (ticker, company_name, gics_sector, valid_from, is_current)
    SELECT DISTINCT 
        ticker, company_name, gics_sector, '2000-01-01'::DATE, TRUE
    FROM staging.raw_financial_feed
    WHERE metric_type = 'MACRO'
    ON CONFLICT (ticker) WHERE is_current = TRUE DO NOTHING;

    -- Регистрация макро-метрики в словаре
    INSERT INTO public.dim_metric_dictionary (metric_code, metric_type)
    SELECT DISTINCT metric_code, metric_type
    FROM staging.raw_financial_feed
    WHERE metric_type = 'MACRO'
    ON CONFLICT (metric_code) DO NOTHING;

    -- Перенос фактов с контролем уникальности
    INSERT INTO public.fact_financial_metrics (report_date, company_sk, metric_id, metric_value)
    SELECT 
        s.report_date::DATE,
        c.company_sk,
        m.metric_id,
        s.metric_value::NUMERIC(20, 4)
    FROM staging.raw_financial_feed s
    JOIN public.dim_company c ON s.ticker = c.ticker AND c.is_current = TRUE
    JOIN public.dim_metric_dictionary m ON s.metric_code = m.metric_code
    WHERE s.metric_type = 'MACRO'
    AND NOT EXISTS (
        SELECT 1 FROM public.fact_financial_metrics f
        WHERE f.report_date = s.report_date::DATE 
          AND f.company_sk = c.company_sk 
          AND f.metric_id = m.metric_id
    );

    -- Очистка staging
    DELETE FROM staging.raw_financial_feed WHERE metric_type = 'MACRO';

    COMMIT;
    """)

    with engine.begin() as conn:
        conn.execute(ingest_sql)
    print("[OK] Пакетная загрузка макроэкономических рядов завершена.")

# Запуск пайплайна
macro_records = fetch_moex_market_index(secid="IMOEX", start_date="2024-01-01")
ingest_macro_indicators_to_dwh(engine, macro_records)
```

---

## 4. МОДУЛЬ 3: ВНУТРИБАЗОВАЯ ТРАНСФОРМАЦИЯ И СТАТИСТИЧЕСКИЙ АНАЛИЗ

### 4.1. Задание 3.1: Построение аналитической витрины (Data Mart)

Создайте в PostgreSQL представление `view_corporate_performance`, рассчитывающее производные бизнес-показатели:
* $\text{EBITDA Margin} = \frac{\text{EBITDA}}{\text{Revenue}} \times 100\%$
* $\text{Net Margin} = \frac{\text{Net Profit}}{\text{Revenue}} \times 100\%$

```sql
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
    WHERE c.gics_sector <> 'INDEX'
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
```

### 4.2. Задание 3.2: Проверка статистических гипотез (Hypothesis Testing)

#### Постановка задачи:
* **Контекст:** Оценка операционной эффективности секторов национальной экономики в условиях санкционного давления.
* **Нулевая гипотеза ($H_0$):** Распределения показателя `ebitda_margin_pct` в секторах *Energy* и *Consumer Discretionary* идентичны (статистически значимые различия отсутствуют).
* **Альтернативная гипотеза ($H_1$):** Распределения показателей операционной маржинальности указанных секторов имеют систематический сдвиг.
* **Инструмент:** Непараметрический критерий Манна — Уитни ($U$-тест) для несвязанных выборок малого объема (`scipy.stats.mannwhitneyu`). Уровень значимости: $\alpha = 0.05$.

```python
import numpy as np
from scipy import stats
import pandas as pd

# 1. Извлечение подготовленной выборки из DWH
query = """
SELECT gics_sector, ebitda_margin_pct 
FROM public.view_corporate_performance 
WHERE ebitda_margin_pct IS NOT NULL 
  AND gics_sector IN ('Energy', 'Consumer Discretionary');
"""
df_stat = pd.read_sql(query, engine)

energy_sample = df_stat[df_stat['gics_sector'] == 'Energy']['ebitda_margin_pct'].dropna()
consumer_sample = df_stat[df_stat['gics_sector'] == 'Consumer Discretionary']['ebitda_margin_pct'].dropna()

print(f"Выборка Energy (n={len(energy_sample)}): {energy_sample.values}")
print(f"Выборка Consumer (n={len(consumer_sample)}): {consumer_sample.values}")

# 2. Выполнение критерия Манна-Уитни
u_stat, p_value = stats.mannwhitneyu(energy_sample, consumer_sample, alternative='two-sided')

# 3. Формирование формального вывода
alpha = 0.05
print("\nРЕЗУЛЬТАТЫ СТАТИСТИЧЕСКОГО ТЕСТИРОВАНИЯ:")
print(f"U-statistic = {u_stat:.4f}")
print(f"p-value = {p_value:.6f}")

if p_value < alpha:
    print(f"ВЕРДИКТ: Отклонить H0 на уровне значимости {alpha}. Обнаружены статистически значимые различия в рентабельности по EBITDA.")
else:
    print(f"ВЕРДИКТ: Не удалось отклонить H0 на уровне значимости {alpha}. Статистически значимых различий не обнаружено.")
```

---

## 5. МОДУЛЬ 4: ИМИТАЦИЯ СБОЕВ И АУДИТ DISASTER RECOVERY (DRIFT INJECTION)

### 5.1. Задание 4.1: Снятие эталонного состояния и инъекция сбоя (Data Poisoning)

```python
import subprocess

# 1. Фиксация эталонного бэкапа
!PGPASSWORD='admin123' pg_dump -h localhost -U dwh_admin -c elt_dwh > /content/elt_dwh_baseline.sql
print("[OK] Эталонный снимок /content/elt_dwh_baseline.sql сохранен.")

# 2. Деструктивный скрипт (Chaos Injection)
corruption_sql = """
BEGIN;

-- Искажение ключевых метрик
UPDATE public.fact_financial_metrics 
SET metric_value = -9999999.00
WHERE company_sk IN (SELECT company_sk FROM public.dim_company WHERE ticker = 'SBER');

-- Случайное удаление части строк
DELETE FROM public.fact_financial_metrics
WHERE fact_id IN (
    SELECT fact_id FROM public.fact_financial_metrics LIMIT 5
);

-- Удаление обязательного индекса
DROP INDEX IF EXISTS public.idx_fact_fin_metrics;

COMMIT;
"""

with engine.begin() as conn:
    conn.execute(text(corruption_sql))

print("[ALARM] Инъекция аномалий завершена: данные повреждены, индекс удален.")
```

### 5.2. Задание 4.2: Data Reconciliation (Сверка контрольных сумм)

Студент обязан разработать проверочный скрипт, выявляющий инцидент до запуска восстановления:

```python
reconciliation_sql = """
SELECT 
    COUNT(*) AS total_fact_records,
    ROUND(SUM(metric_value), 2) AS checksum_metric_value,
    COUNT(*) FILTER (WHERE metric_value < 0) AS negative_values_count,
    (SELECT COUNT(*) FROM pg_indexes WHERE indexname = 'idx_fact_fin_metrics') AS index_exists
FROM public.fact_financial_metrics;
"""

df_audit = pd.read_sql(reconciliation_sql, engine)
print("РЕЗУЛЬТАТ АУДИТА ЦЕЛОСТНОСТИ ДАННЫХ:")
display(df_audit)

# Логическое условие срабатывания аварийного протокола
needs_recovery = (df_audit.loc[0, 'negative_values_count'] > 0) or (df_audit.loc[0, 'index_exists'] == 0)
print(f"Требуется аварийный откат базы (Disaster Recovery): {needs_recovery}")
```

### 5.3. Задание 4.3: Отработка алгоритма восстановления
Примените скрипт автоматического отката к дампу `/content/elt_dwh_baseline.sql` (из Этапа 7 базовой инструкции) и повторите запрос сверки.
*Критерий успеха:* `negative_values_count = 0`, `index_exists = 1`, контрольная сумма `checksum_metric_value` вернулась к эталонному значению.

---

## 6. МОДУЛЬ 5: ПАСПОРТ ИНДИВИДУАЛЬНОГО OSINT-МОНИТОРИНГА ПАО

Каждый студент выбирает одно ПАО из первого или второго котировального списка Московской Биржи (за исключением эталонных `SBER`, `GAZP`, `LKOH`) и оформляет аналитический раздел по шаблону:

### Форма аналитического паспорта

```markdown
### ПАСПОРТ ПАО: [КРАТКОЕ НАИМЕНОВАНИЕ / ТИКЕР]
* **Идентификаторы:** ИНН: [XXXXXXXXXX], ОГРН: [XXXXXXXXXXXXX], Сектор MOEX: [Сектор].
* **Ликвидность:** Среднедневной объем торгов (ADTV 30D): [Значение в млн руб.].
* **Официальный раскрыватель:** [Интерфакс-ЦРКИ / Скрин / e-disclosure].

#### 1. Модель выявления точки бифуркации
Определите конкретный момент времени (или прогнозируемый горизонт), в который компания переходит в режим нестабильности под влиянием внутренних или внешних шоков.

* **Внешние шоки (Макро):** [например: экспортные пошлины, трансграничные дисконты Urals/ESPO, изменение ставки ЦБ].
* **Внутренние дисбалансы:** [например: соотношение Чистый Долг / EBITDA > 3.5x, отрицательный операционный денежный поток].

#### 2. Матрица опережающих OSINT-триггеров
| Контур мониторинга | Первичный источник данных | Фиксируемый индикатор (Anomaly Trigger) |
| :--- | :--- | :--- |
| **Долговой рынок** | Cbonds / Rusbonds | Рост доходности облигаций (YTM spread > 500 bps к ОФЗ), требование оферты |
| **Судебный контур** | Картотека арбитражных дел (КАД) | Сумма исков кредиторов > 15% от стоимости чистых активов за последние 30 дней |
| **Обременения** | Реестр уведомлений залогов (ФНП) | Передача ключевых основных средств / контрольных долей ДЗО в залог банкам |
| **Корпоративные события** | Федресурс (ЕФРСБ) | Сообщения о намерении обратиться с заявлением о банкротстве, смена реестродержателя |

#### 3. Сценарное моделирование бифуркации
* **Сценарий А (Оптимистичный / Реструктуризация):** Условия реализации сценария, действия менеджмента, триггер снятия долговой нагрузки.
* **Сценарий Б (Пессимистичный / Делистинг / Дефолт):** Каскад событий, приводящий к списанию акционерного капитала или санации.
```

---

## 7. КРИТЕРИИ ПРИЕМКИ РАБОТЫ (DEFINITION OF DONE)

Работа признается выполненной при соблюдении следующих критериев:

1. **Архитектура SCD Type 2:**
   * В таблице `dim_company` отсутствуют жесткие ограничения `UNIQUE(ticker)` без учета флага актуальности.
   * Вызов функции `scd2_update_company` закрывает текущую запись и создает новую с новым `company_sk`.
   * Факты корректно связываются с историческими версиями через внешний ключ.
2. **REST API Pipeline:**
   * Реализован автоматический опрос публичного REST API с пагинацией.
   * Данные загружаются через буфер `staging` без применения медленных построчных итераторов `df.iterrows()`.
   * Реализована политика *Zero-duplication*: повторный вызов скрипта не изменяет размер таблицы фактов.
3. **Аналитика и тестирование гипотез:**
   * Построено SQL-представление `view_corporate_performance` с вычислением относительных метрик на уровне СУБД.
   * В блокноте выполнен статистический тест (`mannwhitneyu`), корректно сформулированы гипотезы $H_0$ и $H_1$, приведен расчет `p-value` и экономическая интерпретация.
4. **Disaster Recovery:**
   * Продемонстрирован лог выявления расхождений (*Data Drift Detection*) после деструктивного воздействия.
   * Выполнен автоматический откат, подтвержденный совпадением контрольных сумм и проверкой системных каталогов `pg_indexes`.
5. **OSINT-паспорт:**
   * Заполнены все поля паспорта исследуемого эмитента, включая матрицу из 4 контуров внешних данных.
