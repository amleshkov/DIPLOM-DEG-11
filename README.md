# Димпломная работа по специализации DEG-11

**Цель работы:** демонстрация навыков проектирования ETL-процесса и подключеия BI-инструмента.

1. [Описание и запуск стенда](#stand)
2. [Описание ETL-процесса](#etl)
3. [Описание объектов БД](#db)
4. [Описание дашбордов](#bi)
5. [Ссылки на используемые материалы](#reference)

### Описание и запуск стенда <a name="stand"></a>

Для данной работы выбраны только широкоиспользуемые opensource продукты. В качестве основного хранилища для данных NDS и DDS выбрана RDBMS PostgreSQL.
Для формирования быстрой витрины используется OLAP DBMS Clikhouse. ETL-процесс построен с использованием только SQL и PL/pgSQL и оркестрован с помощью Apache Airflow. В качестве BI-инструмента выбран Apache Superset.

Для удобства воспроизведения стенд спроектирован с помощью docker-compose и включает в себя следующие компоненты:
* экземпляр PostgreSQL - хранилище для рабочей БД dwh со схемами stage, nds, dwh, хранилище метаданных airflow и superset;
* экземпляр Clickhouse - хранилище для итоговой плоской витрины;
* экземпляры airflow-webserver и airflow-scheduler - оркестратор ETL
* экземпляры superset-app, superset-worker, superset-worker-beat - BI инструмент
* экземпляр Redis - кеш для результатов Superset

Также автоматически настроены все необходимые интеграции и установка библиотек

Для запуска стенда необходимо склонироват репозиторий и выполнить `docker-compose up`.
**Реквизиты для подключения к сервисам**

| Сервис     | Подключение               | Логин | Пароль   |
|------------|---------------------------|-------|----------|
| postgres   | localhost:5432/dwh        | dwh   | dwh      |
| clickhouse | localhost:8321/sales_mart | bi    | password |
| airflow    | http://localhost:8080     | admin | admin    |
| superset   | http://localhost:8088     | admin | admin    |

После завершения запуска стенда необходимо зайти в интерфейс Airflow по ссылке
http://localhost:8080 с логином admin и паролем admin, затем активировать DAG "ETL" (переключатель Pause/Unpause DAG) и дождаться завершения процесса загрузки.

Далее необходимо загрузить подготовленные дашборды в Superset. Для этого войти в интерфейс Superset по ссылке http://localhost:8088 c логином admin и паролем admin. Затем затем необходимо загрузить метаданные для датасета: меню Datasets -> кнопка Import Dataset -> Select File и далее выбрать файл `./superset-exports/dataset_export.zip`. На запрос пароля ввести 'password'. После этого необходимо загрузить дашборд аналогичным образом: меню Dashboards -> кнопка Import Dashboards -> Select file и далее выбрать `./superset-exports/dashboard_export.zip`

### Описание ETL-процесса <a name="etl"></a>
Схематично ETL-процесс изображен на рисунке

![ETL](https://github.com/amleshkov/DIPLOM-DEG-11/blob/157be63030457f2514e43d254b4e93217d90d837/images/etl.png)

Процесс реализован с помощью Airflow, SQL и PL/pgSQL процедур. Все скрипты находятся в директории `./dags`.

**populate_postgres** 
Шаг, создающий схемы satage, nds и dwh в БД dwh. В схемах создаются необходимые таблицы, функции, триггеры и финальное представление.

**populate_clickhouse**
Шаг, создающий таблицу витрины в Clickhouse

**load_csv**
На данном шаге производится загрузка CSV файла в таблицу `stage.raw_sales`. При этом все поля имеют тип `TEXT`, что исключает ошибку загрузки (за исключением несовпадения количества столбцов)

**validate_raw_data**
На данном шаге производится загрузка данных из таблицы `stage.raw_sales` в `stage.valid_sales` путем вызова специальной функции `stage.validate_insert()`. Таблица `stage.valid_sales` создана таким образом, что все колонки имеют определенный тип, не могут иметь значения `NULL`. Помимо того на колонку `invoice_id` наложено ограничени `UNIQUE`. Ошибки, возникающие в процессе загрузки из-за данных ограничений, обрабатываются функцией `stage.validate_insert()` и ошибочные строки заносятся в специальную таблицу `stage.errors`. Таким образом осуществляется базовая проверка на качество данных.

**load_nds**
На данном шаге производится загрузка данных в нормализованную схему `nds`. Загрузка реализована на функциях `nds.*_lookup_update` таким образом, что справочники заполняются автоматически с генерацией ключей для основной таблицы `nds.sales`. Данный подход позволяет переиспользовать шаги  load_csv, validate_raw_data, load_nds, load_dwh для батч-загрузки с автоматическим обновлением всех справочников и измерений. При этом работа данной цепочки идемпотентна.

**load_dwh**
На данном шаге производится JOIN всех талиц из схемы `nds` и загрузка данных в таблицу `dwh.fact_sales`. Загрузка осуществляется с применением аналогичных функций `dwh.*_lookup_update`, которые автоматически создают новые записи в таблицах измерений, возвращая технический ключ. На таблицы измерений повешены триггеры, которые вызывают функцию `dwh.*_scd2()`, обеспечивающую автоматическое поддержание SCD2 для данных таблиц на процедуре INSERT.

**load_mart**
На данном этапе производится загрузка из представления `dwh.fact_sales_w` через специальный объект подключения `postgres` в плоскую витрину `fact_sales_mart` Clickhouse для дальнейшего использования в BI системе.

### Описание объектов БД dwh Postgresql <a name="db"></a>

**Схема stage**

![stage](https://github.com/amleshkov/DIPLOM-DEG-11/blob/dfd225f26548ec6cd59070059e8e76b904f1deae/images/stage.png)

Слой сырых данных

`stage.raw_sales` - таблица с типом данных TEXT для загрузки CSV. Используется для предварительной загрузки сырых данных, т.к. обработка ошибок невозможна на функции LOAD.

`stage.valid_sales` - таблица с ограничениями и типизацией. Используется в процессе валидации данных, для последующей загрузки в схему `nds`

`stage.errors` - служебная таблица для сохранения ошибочных данных и сообщений об ошибках.

`stage.validate_insert` - Функция, выполняющая вставку данных из `stage.raw_sales` в `stage.valid_sales` с перехватом ошибок и вставкой идентификатора `invoice_id` с расшифровкой ошибки в `stage.errors`.

**Схема nds**

![nds](https://github.com/amleshkov/DIPLOM-DEG-11/blob/dfd225f26548ec6cd59070059e8e76b904f1deae/images/nds.png)

Слой нормализованных данных. Может выступать как эмуляция операционной базы (ODS).

`nds.sales` - основная нормализованная таблица данных

`nds.*` - талицы-справочники

`nds.*_lookup_update()` - соответствующие справочникам функции, при вызове которых со значением справочника производится поиск этого значения в справочнике. Если значение найдено, то возвращается `id`, если нет, то значение записывается и возвращается `id` нового значения. Таким образом можно в один проход заполнить таблицу `nds.sales` и одновременно заполнить все справочники.

**Схема dwh**

![dwh](https://github.com/amleshkov/DIPLOM-DEG-11/blob/dfd225f26548ec6cd59070059e8e76b904f1deae/images/dwh.png)

Слой хранилища, построен по схеме "звезда" с SCD2 для таблиц измерений.

`dwh.fact_sales` - талица фактов.

`dwh.dim_calendar` - сгенерированная таблица календаря.

`dwh.dim_*` - таблицы справочников.

`dwh.dim_*_scd2()` - функция, поддерживающая SCD2 в таблицах измерений. В общем случае запись в измерении состоит из технического ключа, "натурального" ключа (на самом деле он просто взят из справочника nds, и строго говоря не является натуральным) и собственно значения. Если при вставке данных для натурального ключа меняется значение, то данные вставляются, как новая версия с изменением атрибутов `start_ts`, `end_ts` и `is_current`. Если значение является новым, то производится вставка с выставлением полей `start_ts` = текущая дата, `end_ts = 2999-01-01` и `is_current=True`. При полном совпадении вставка не производится. Данная функция вызывается на триггере `dim_*_trigger`.

`dim_*_trigger` - триггер, вызывающий `dwh.dim_*_scd2()` до INSERT в `dwh.dim_*`.

`dwh.dim_*_lookup_update` - аналогично `nds.*_lookup_update()` для таблиц измерений.

`dwh.fact_sales_w` - представление с плоской витриной для перегрузки в Clickhouse

### Описание объектов в БД sales_mart Clickhouse
`fact_sales_mart` - талица плоской витрины.

`postgresql` - объект подключения к базе dwh Postgresql.

### Описание дашбордов <a name="bi"></a>
Дашборд "Supermarket Sales" состоит из двух вкладок - "Sales overview" и "Stats"

**Sales overview**
Основной отчет

![sales](https://github.com/amleshkov/DIPLOM-DEG-11/blob/dfd225f26548ec6cd59070059e8e76b904f1deae/images/sales_dashboard.png)

- Total Gross Income - Общая гросс-прибыль за весь период
- Gross Income by Week - Общая прибыль с разбивкой по неделям (из-за ограниченного периода в исходном файле пришлось выбрать такую разбивку для наибольшей наглядности).
- Total Items Sold - Общее количество проданных единиц товара.
- Quantity Sold by Product Line - общее количество единиц проданного товара с разбивкой по типу товара.
- Gross Income by Week and Product Line, аналогично Gross Income by Week, но с сегментацией по типу товара.

**Stats**
Отчет со статистикой для отдела маркетинга

![stats](https://github.com/amleshkov/DIPLOM-DEG-11/blob/dfd225f26548ec6cd59070059e8e76b904f1deae/images/stats_dashboard.png)

- Gender to Customer Type by Price Total - соотношение пол - тип покупателя по сумме чеков
- Gender to Payment Type by Price Total - соотношение пол - тип платежной системы по сумме чеков
- Payment Type to Product Line by Total Sum - тепловая карта корелляции тип товара - тип платежной системы по сумме чеков.
- Activity by Hour - количество всех покупок в зависимости от часа дня.
- Activity by Day of Week - количество всех покупок в зависимости от дня недели.

### Ссылки на используемые материалы <a name="reference"></a>
https://hub.docker.com/_/postgres

https://github.com/mrts/docker-postgresql-multiple-databases

https://hub.docker.com/r/clickhouse/clickhouse-server

https://towardsdatascience.com/apache-airflow-and-postgresql-with-docker-and-docker-compose-5651766dfa96

https://superset.apache.org/docs/installation/installing-superset-using-docker-compose/
