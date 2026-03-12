-- CTE для підрахунку метрик по акаунтах
WITH account_metrics AS (
    SELECT
        DATE(s.date) AS date,
        sp.country,
        a.send_interval,
        a.is_verified,
        a.is_unsubscribed,
        COUNT(a.id) AS account_cnt,
        0 AS sent_msg,
        0 AS open_msg,
        0 AS visit_msg,
    FROM data-analytics-mate.DA.account a
    JOIN data-analytics-mate.DA.account_session asess ON a.id = asess.account_id    
    JOIN data-analytics-mate.DA.session s ON asess.ga_session_id = s.ga_session_id    
    JOIN data-analytics-mate.DA.session_params sp ON s.ga_session_id = sp.ga_session_id
    GROUP BY s.date, sp.country, a.send_interval, a.is_verified, a.is_unsubscribed
),

-- CTE для підрахунку метрик по емейлах
email_metrics AS (
    SELECT
        DATE_ADD(s.date, INTERVAL es.sent_date DAY) AS date,
        sp.country,
        a.send_interval,
        a.is_verified,
        a.is_unsubscribed,
        0 AS account_cnt,
        COUNT(es.id_message) AS sent_msg,
        COUNT(eo.id_message) AS open_msg,
        COUNT(ev.id_message) AS visit_msg
    FROM data-analytics-mate.DA.email_sent es
    JOIN data-analytics-mate.DA.account a ON es.id_account = a.id
    JOIN data-analytics-mate.DA.account_session asess ON a.id = asess.account_id    
    JOIN data-analytics-mate.DA.session s ON asess.ga_session_id = s.ga_session_id    
    JOIN data-analytics-mate.DA.session_params sp ON s.ga_session_id = sp.ga_session_id    
    LEFT JOIN data-analytics-mate.DA.email_open eo ON es.id_message = eo.id_message AND es.id_account = eo.id_account
    LEFT JOIN data-analytics-mate.DA.email_visit ev ON es.id_message = ev.id_message AND es.id_account = ev.id_account
    GROUP BY s.date, es.sent_date, sp.country, a.send_interval, a.is_verified, a.is_unsubscribed
),

-- Об'єднання результатів
combined_data AS (
    SELECT * FROM account_metrics
    UNION ALL
    SELECT * FROM email_metrics
),

-- Агрегація основних метрик
aggregated_data AS (
    SELECT
        date,
        country,
        send_interval,
        is_verified,
        is_unsubscribed,
        SUM(account_cnt) AS total_account_cnt,
        SUM(sent_msg) AS total_sent_msg,
        SUM(open_msg) AS total_open_msg,
        SUM(visit_msg) AS total_visit_msg,
        SUM(SUM(account_cnt)) OVER (PARTITION BY country) AS total_country_account_cnt,
        SUM(SUM(sent_msg)) OVER (PARTITION BY country) AS total_country_sent_cnt
    FROM combined_data
    GROUP BY date, country, send_interval, is_verified, is_unsubscribed
),

-- Ранжування за країнами
ranked_data AS (
    SELECT *,
        DENSE_RANK() OVER (ORDER BY total_country_account_cnt DESC) AS rank_total_country_account_cnt,
        DENSE_RANK() OVER (ORDER BY total_country_sent_cnt DESC) AS rank_total_country_sent_cnt
    FROM aggregated_data
)

-- Вибір топ-10 країн
SELECT *
FROM ranked_data
WHERE rank_total_country_account_cnt <= 10 OR rank_total_country_sent_cnt <= 10;
