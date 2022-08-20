from pysqlparse.parser import get_table_names

long_query = """
        CREATE TABLE stage_db.table1 WITH (format = 'parquet', parquet_compression='snappy', external_location='s3://testy_mc_testface/', partitioned_by = 'year' ) AS 
        WITH table2 AS (
        SELECT 
            DISTINCT l_postal_code AS postal_code, 
            l_source_id, 
            l_id 
        FROM 
            business_data_h.l_history 
        WHERE 
            year = '2000'
        ), 
        customer_map AS (
        SELECT 
            advertiser_id, 
            a_id, 
            m_id, 
            replace(date, '-', '') AS snapshot_by_day, 
            substr(
            replace(date, '-', ''), 
            1, 
            6
            ) AS snapshot_by_month 
        FROM 
            (
            SELECT 
                advertiser_id, 
                date, 
                min(a_id) AS a_id, 
                min(
                split_part(a_id, '-', 2)
                ) AS m_id 
            FROM 
                business_data.customers 
            WHERE 
                substr(
                replace(date, '-', ''), 
                1, 
                6
                ) = date_format(
                cast('2020-01-01' AS timestamp)+ interval '-1' month, 
                '%Y%m'
                ) 
                and primary_flag = '1' 
            GROUP BY 
                advertiser_id, 
                date
            )
        ), 
        event_performance AS (
        SELECT 
            m_id, 
            postal_code, 
            l_id, 
            snapshot_by_month, 
            impression_count, 
            page_view, 
            goog_impression_count, 
            goog_page_view, 
            null AS l_type, 
            null AS l_status, 
            null AS days, 
            null AS price, 
            null AS active, 
            null AS profile_view_count, 
            null AS mini_profile_view_count, 
            null AS agent_impression_count 
        FROM 
            (
            SELECT 
                split_part(
                a.l_a_id, '-', 
                2
                ) AS m_id, 
                substr(a.event_date_mst_yyyymmdd, 1, 6) snapshot_by_month, 
                b.postal_code, 
                a.l_id, 
                sum(
                case WHEN metric_type = 'impression' 
                AND lower(experience) IN (
                    'desktop', 'phone', 'tablet'
                ) THEN event_count ELSE 0 end
                ) AS impression_count, 
                sum(
                case WHEN metric_type = 'page_view' 
                AND lower(experience) IN (
                    'desktop', 'phone', 'tablet'
                ) THEN event_count ELSE 0 end
                ) AS page_view, 
                sum(
                case WHEN metric_type = 'impression' 
                AND lower(experience) IN (
                    'android', 'ipad', 'iphone' 
                ) THEN event_count ELSE 0 end
                ) AS goog_impression_count, 
                sum(
                case WHEN metric_type = 'page_view' 
                AND lower(experience) IN (
                    'android', 'ipad', 'iphone' 
                    'rentals-iphone'
                ) THEN event_count ELSE 0 end
                ) AS goog_page_view 
            FROM 
                business_data_h.l_event_performance a 
            JOIN table2 b 
            ON a.l_id = b.l_id 
            AND substr(event_date_mst_yyyymmdd, 1, 6) = date_format(
                cast('2021-05-28' AS timestamp)+ interval '-1' month, 
                '%Y%m'
                ) 
            AND l_a_id LIKE 'A-%' 
            AND metric_type IN (
                'page_view', 'impression'
                ) 
            GROUP BY 
                split_part(
                a.l_a_id, '-', 
                2
                ), 
                b.postal_code, 
                a.l_id, 
                substr(a.event_date_mst_yyyymmdd, 1, 6)
            ) temp
        ), 
        mls_listing_metric_by_month AS (
        SELECT 
            m_id, 
            postal_code, 
            l_id, 
            substr(
            week_end_snapshot_by_day, 
            1, 6
            ) AS snapshot_by_month, 
            null AS impression_count, 
            null AS page_view, 
            null AS goog_impression_count, 
            null AS goog_page_view, 
            l_type, 
            l_status, 
            days, 
            price, 
            active, 
            null AS profile_view_count, 
            null AS mini_profile_view_count, 
            null AS agent_impression_count 
        FROM 
            (
            SELECT 
                upper(listing_source_id) AS m_id, 
                postal_code, 
                l_id, 
                l_type, 
                l_status, 
                days, 
                price, 
                active, 
                week_end_snapshot_by_day 
            FROM 
                business_data_l_metric.weekly_snapshot a 
            WHERE 
                substr(
                week_end_snapshot_by_day, 
                1, 6
                ) = date_format(
                cast('2021-05-28' AS timestamp)+ interval '-1' month, 
                '%Y%m'
                ) 

            )
        ), 
        profile_performance AS (
        SELECT 
            m_id, 
            postal_code, 
            l_id, 
            snapshot_by_month, 
            null AS impression_count, 
            null AS page_view, 
            null AS goog_impression_count, 
            null AS goog_page_view, 
            null AS l_type, 
            null AS l_status, 
            null AS days, 
            null AS price, 
            null AS active, 
            profile_view_count, 
            mini_profile_view_count, 
            agent_impression_count 
        FROM 
            (
            SELECT 
                c.m_id, 
                c.snapshot_by_month, 
                b.postal_code, 
                '' l_id, 
                sum(
                case when e.metric_type = 'profile_view' then e.event_count else 0 end
                ) AS profile_view_count, 
                sum(
                case when e.metric_type = 'mini_profile_view' then e.event_count else 0 end
                ) AS mini_profile_view_count, 
                sum(
                case when e.metric_type = 'far_agent_impression' then e.event_count else 0 end
                ) AS agent_impression_count 
            FROM 
                business_data_h.l_event_performance e, 
                customer_map c, 
                table2 b 
            WHERE 
                substr(event_date_mst_yyyymmdd, 1, 6) = date_format(
                cast('2021-05-28' AS timestamp)+ interval '-1' month, 
                '%Y%m'
                ) 
                AND c.snapshot_by_month = date_format(
                cast('2021-05-28' AS timestamp)+ interval '-1' month, 
                '%Y%m'
                ) 
                AND e.metric_type in (
                'far_agent_impression', 'profile_view', 
                'mini_profile_view'
                ) 
                AND e.l_id = b.l_id 
                AND e.event_advertiser_id_raw = c.advertiser_id 
                AND e.event_date_mst_yyyymmdd = c.snapshot_by_day 
            GROUP BY 
                c.m_id, 
                c.snapshot_by_month, 
                b.postal_code
            ) temp
        ), 
        final_dataset AS (
        SELECT 
            m_id, 
            postal_code, 
            l_id, 
            max(impression_count) AS impression_count, 
            max(page_view) AS page_view, 
            max(goog_impression_count) AS goog_impression_count, 
            max(goog_page_view) AS goog_page_view, 
            max(l_type) AS l_type, 
            max(l_status) AS l_status, 
            max(days) AS days, 
            max(price) AS price, 
            max(active) AS active, 
            max(profile_view_count) AS profile_view_count, 
            max(mini_profile_view_count) AS mini_profile_view_count, 
            max(agent_impression_count) AS agent_impression_count, 
            cast(current_timestamp AS timestamp) AS etl_created_date_gmt, 
            snapshot_by_month 
        FROM 
            (
            SELECT 
                m_id, 
                postal_code, 
                l_id, 
                impression_count, 
                page_view, 
                goog_impression_count, 
                goog_page_view, 
                l_type, 
                l_status, 
                days, 
                price, 
                active, 
                profile_view_count, 
                mini_profile_view_count, 
                agent_impression_count, 
                snapshot_by_month 
            FROM 
                event_performance 
            UNION ALL 
            SELECT 
                m_id, 
                postal_code, 
                l_id, 
                impression_count, 
                page_view, 
                goog_impression_count, 
                goog_page_view, 
                l_type, 
                l_status, 
                days, 
                price, 
                active, 
                profile_view_count, 
                mini_profile_view_count, 
                agent_impression_count, 
                snapshot_by_month 
            FROM 
                mls_listing_metric_by_month 
            UNION ALL 
            SELECT 
                m_id, 
                postal_code, 
                l_id, 
                impression_count, 
                page_view, 
                goog_impression_count, 
                goog_page_view, 
                l_type, 
                l_status, 
                days, 
                price, 
                active, 
                profile_view_count, 
                mini_profile_view_count, 
                agent_impression_count, 
                snapshot_by_month 
            FROM 
                profile_performance
            ) t 
        GROUP BY 
            m_id, 
            postal_code, 
            l_id, 
            snapshot_by_month
        ), 
        last_month AS (
        SELECT 
            month_end_date, 
            month_start_date, 
            cy_month AS snapshot_month 
        FROM 
            business_data.calendar_date c 
        WHERE 
            full_date = (
            SELECT 
                date_add('day',-1, month_start_date) AS last_month_end_date 
            FROM 
                business_data.calendar_date c 
            WHERE 
                full_date = cast('2020-01-01' AS timestamp)
            )
        ) 
        SELECT 
        final_dataset.m_id, 
        final_dataset.postal_code, 
        final_dataset.l_id, 
        final_dataset.impression_count, 
        final_dataset.page_view, 
        final_dataset.goog_impression_count, 
        final_dataset.goog_page_view, 
        final_dataset.l_type, 
        final_dataset.l_status, 
        final_dataset.days, 
        final_dataset.price, 
        final_dataset.active, 
        final_dataset.profile_view_count, 
        final_dataset.mini_profile_view_count, 
        final_dataset.agent_impression_count, 
        last_month.month_start_date, 
        last_month.month_end_date, 
        final_dataset.snapshot_by_month 
        FROM 
        final_dataset 
        JOIN last_month 
        ON final_dataset.snapshot_by_month = last_month.snapshot_month
    """


def test_matching_table_aliases_to_table_name():
    query = """
        SELECT *
        FROM requests.by_account m
        INNER JOIN customer_data.styles s ON m.metadata_version = s.id
        LEFT JOIN profiles.users u ON m.csm = u.id
    """
    result = get_table_names(query)
    assert result == {'requests.by_account', 'customer_data.styles', 'profiles.users'}


def test_analyze_nested_query():
    query = """SELECT *
        FROM
        (SELECT a.*,
                b.*
        FROM
            (SELECT DISTINCT anonymous_id,
                            user_id
            FROM customer_data.segment
            WHERE dt >= '1918-07-01') a
        LEFT JOIN
            (SELECT id,
                    email
            FROM customer_data.accounts) b ON a.user_id = b.id
        WHERE context IS NOT NULL )
    """
    result = get_table_names(query)
    assert result == {'customer_data.segment', 'customer_data.accounts'}


def test_ctes():
    query = """
            WITH a AS
            (SELECT DISTINCT anonymous_id,
                            user_id
            FROM customer_data.segment
            WHERE dt >= '1918-06-23'),
                b AS
            (SELECT id,
                    email
            FROM customer_data.accounts)
            SELECT a.*,
                b.*
            FROM a
            LEFT JOIN b ON a.user_id = b.id
            WHERE context IS NOT NULL
        """
    result = get_table_names(query)
    assert result == {'customer_data.segment', 'customer_data.accounts'}


def test_join_with_alias():
    query = """
            SELECT *
            FROM requests.by_account m
            INNER JOIN customer_data.styles s ON m.version = s.id
            LEFT JOIN profiles.users u ON m.csm = u.id
        """
    result = get_table_names(query)
    assert result == {
        'requests.by_account',
        'customer_data.styles',
        'profiles.users',
    }


def test_join_without_name():
    pass


def test_filter_out_comments():
    result = get_table_names(long_query)
    assert result == {
        'business_data_l_metric.weekly_snapshot',
        'business_data.customers',
        'business_data_h.l_event_performance',
        'business_data_h.l_history',
        'business_data.calendar_date',
    }


def test_filter_out_aliases():
    result = get_table_names(long_query)
    assert result == {
        'business_data_l_metric.weekly_snapshot',
        'business_data.customers',
        'business_data_h.l_event_performance',
        'business_data_h.l_history',
        'business_data.calendar_date',
    }


def test_ignores_cross_join():
    query = 'SELECT internal_id AS internal_account_id,\n    ' \
            'internal_source_id,\n    ao.internal_id AS ' \
            'internal_office_id,\n    date_format(cast(concat(year, \'-\', ' \
            'month, \'-\', day) as date), \'%Y-%m-%d\') AS snapshot_date\nFROM ' \
            '"db1"."account"\nCROSS JOIN UNNEST(offices) as ' \
            't(account_office)\nWHERE year = date_format(cast(\'2012-08-23\' as date), ' \
            '\'%Y\')\nAND month = date_format(cast(\'2012-08-23\' as date), \'%m\')\nAND' \
            ' day = date_format(cast(\'2012-08-23\' as date), \'%d\')'
    result = get_table_names(query)
    assert result == {'db1.account', }


def test_finds_tables_in_multiple_select():
    query = '''SELECT * FROM test_table as tbl1, test_schema.test_table2 tabl2 group by tbl1.name'''
    result = get_table_names(query)
    assert result == {'test_table', 'test_schema.test_table2'}

    query2 = '''SELECT * FROM test_table as tbl1, test_schema.test_table2 tabl2, test3.test_table3 group by tbl1.name'''
    result2 = get_table_names(query2)
    assert result2 == {'test_table', 'test_schema.test_table2', 'test3.test_table3'}
