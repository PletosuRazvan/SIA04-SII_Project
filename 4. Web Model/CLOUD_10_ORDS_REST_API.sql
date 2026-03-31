BEGIN
    ORDS.ENABLE_SCHEMA(
        p_enabled             => TRUE,
        p_schema              => 'FDBO',
        p_url_mapping_type    => 'BASE_PATH',
        p_url_mapping_pattern => 'fdbo',
        p_auto_rest_auth      => FALSE
    );
    COMMIT;
END;

BEGIN
    ORDS.DEFINE_MODULE(
        p_module_name    => 'olympics',
        p_base_path      => '/olympics/',
        p_items_per_page => 25,
        p_status         => 'PUBLISHED',
        p_comments       => 'Olympics Data REST API - 4 Data Sources'
    );
    COMMIT;
END;

BEGIN
    ORDS.DEFINE_TEMPLATE(
        p_module_name    => 'olympics',
        p_pattern        => 'sports',
        p_comments       => 'Lista sporturilor olimpice (DS1-Oracle)'
    );
    ORDS.DEFINE_HANDLER(
        p_module_name    => 'olympics',
        p_pattern        => 'sports',
        p_method         => 'GET',
        p_source_type    => 'json/collection',
        p_source         => 'SELECT sport_id, sport_name, data_source FROM FDBO.FDBO_SPORTS_V ORDER BY sport_name'
    );
    COMMIT;
END;

BEGIN
    ORDS.DEFINE_TEMPLATE(
        p_module_name    => 'olympics',
        p_pattern        => 'games',
        p_comments       => 'Lista edițiilor olimpice (DS1-Oracle)'
    );
    ORDS.DEFINE_HANDLER(
        p_module_name    => 'olympics',
        p_pattern        => 'games',
        p_method         => 'GET',
        p_source_type    => 'json/collection',
        p_source         => 'SELECT game_id, games_name, year, season, city, data_source FROM FDBO.FDBO_GAMES_V ORDER BY year'
    );
    COMMIT;
END;

BEGIN
    ORDS.DEFINE_TEMPLATE(
        p_module_name    => 'olympics',
        p_pattern        => 'countries',
        p_comments       => 'Lista țărilor (DS1-Oracle)'
    );
    ORDS.DEFINE_HANDLER(
        p_module_name    => 'olympics',
        p_pattern        => 'countries',
        p_method         => 'GET',
        p_source_type    => 'json/collection',
        p_source         => 'SELECT noc, region, notes, data_source FROM FDBO.FDBO_COUNTRIES_V ORDER BY region'
    );
    COMMIT;
END;

BEGIN
    ORDS.DEFINE_TEMPLATE(
        p_module_name    => 'olympics',
        p_pattern        => 'top-medalists',
        p_comments       => 'Top medaliști global (Window Functions)'
    );
    ORDS.DEFINE_HANDLER(
        p_module_name    => 'olympics',
        p_pattern        => 'top-medalists',
        p_method         => 'GET',
        p_source_type    => 'json/collection',
        p_source         => 'SELECT athlete_name, country_name, total_medals, gold, global_dense_rank FROM FDBO.WF_TOP_MEDALISTS_V WHERE global_dense_rank <= 50 ORDER BY global_dense_rank'
    );
    COMMIT;
END;

BEGIN
    ORDS.DEFINE_TEMPLATE(
        p_module_name    => 'olympics',
        p_pattern        => 'medal-share',
        p_comments       => 'Procentul de medalii per țară (Window Functions)'
    );
    ORDS.DEFINE_HANDLER(
        p_module_name    => 'olympics',
        p_pattern        => 'medal-share',
        p_method         => 'GET',
        p_source_type    => 'json/collection',
        p_source         => 'SELECT country_name, total_medals, gold, pct_of_all_medals, pct_of_all_gold, rank_total FROM FDBO.WF_MEDAL_SHARE_V ORDER BY rank_total FETCH FIRST 50 ROWS ONLY'
    );
    COMMIT;
END;

BEGIN
    ORDS.DEFINE_TEMPLATE(
        p_module_name    => 'olympics',
        p_pattern        => 'olap/country-medals',
        p_comments       => 'Medalii per țară cu subtotaluri (ROLLUP)'
    );
    ORDS.DEFINE_HANDLER(
        p_module_name    => 'olympics',
        p_pattern        => 'olap/country-medals',
        p_method         => 'GET',
        p_source_type    => 'json/collection',
        p_source         => 'SELECT country_name, sport_name, total_medals, gold, silver, bronze, is_country_total, is_sport_total FROM FDBO.OLAP_ROLLUP_COUNTRY_SPORT_V WHERE is_sport_total = 1 AND is_country_total = 0 ORDER BY total_medals DESC FETCH FIRST 30 ROWS ONLY'
    );
    COMMIT;
END;

BEGIN
    ORDS.DEFINE_TEMPLATE(
        p_module_name    => 'olympics',
        p_pattern        => 'data-sources',
        p_comments       => 'Statistici per sursă de date (GROUPING SETS)'
    );
    ORDS.DEFINE_HANDLER(
        p_module_name    => 'olympics',
        p_pattern        => 'data-sources',
        p_method         => 'GET',
        p_source_type    => 'json/collection',
        p_source         => 'SELECT source_name, category, record_count, grouping_id FROM FDBO.OLAP_GS_DATA_SOURCES_V ORDER BY grouping_id, source_name'
    );
    COMMIT;
END;

BEGIN
    ORDS.DEFINE_TEMPLATE(
        p_module_name    => 'olympics',
        p_pattern        => 'json/athlete-medals',
        p_comments       => 'Documente JSON medalii atleți (DS3-MongoDB)'
    );
    ORDS.DEFINE_HANDLER(
        p_module_name    => 'olympics',
        p_pattern        => 'json/athlete-medals',
        p_method         => 'GET',
        p_source_type    => 'json/collection',
        p_source         => 'SELECT athlete_name, total_medals, gold_medals, silver_medals, bronze_medals, sports_count, games_count, data_source FROM FDBO.FDBO_JSON_ATHLETE_MEDALS_V ORDER BY total_medals DESC FETCH FIRST 50 ROWS ONLY'
    );
    COMMIT;
END;

BEGIN
    ORDS.DEFINE_TEMPLATE(
        p_module_name    => 'olympics',
        p_pattern        => 'growth',
        p_comments       => 'Creșterea Jocurilor Olimpice (LAG/LEAD Window Functions)'
    );
    ORDS.DEFINE_HANDLER(
        p_module_name    => 'olympics',
        p_pattern        => 'growth',
        p_method         => 'GET',
        p_source_type    => 'json/collection',
        p_source         => 'SELECT year, season, city, total_athletes, prev_edition_athletes, growth, growth_pct FROM FDBO.WF_LAG_LEAD_EDITIONS_V ORDER BY year'
    );
    COMMIT;
END;

SELECT id, name, uri_prefix, status 
FROM user_ords_modules;

SELECT module_id, uri_template, method 
FROM user_ords_templates t
JOIN user_ords_modules m ON t.module_id = m.id;
