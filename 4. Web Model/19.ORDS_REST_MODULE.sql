BEGIN
    ORDS.delete_module(p_module_name => 'fdbo.olympics.api');
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN NULL;
END;

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

DECLARE
    PROCEDURE add_get_endpoint(
        p_module_name IN VARCHAR2,
        p_pattern     IN VARCHAR2,
        p_sql         IN CLOB
    ) IS
    BEGIN
        ORDS.DEFINE_TEMPLATE(
            p_module_name => p_module_name,
            p_pattern     => p_pattern,
            p_priority    => 0,
            p_etag_type   => 'NONE',
            p_etag_query  => NULL,
            p_comments    => NULL
        );

        ORDS.DEFINE_HANDLER(
            p_module_name    => p_module_name,
            p_pattern        => p_pattern,
            p_method         => 'GET',
            p_source_type    => 'json/collection',
            p_items_per_page => 25,
            p_mimes_allowed  => '',
            p_comments       => NULL,
            p_source         => p_sql
        );
    END;
BEGIN
    ORDS.DEFINE_MODULE(
        p_module_name    => 'fdbo.olympics.api',
        p_base_path      => '/olympics/',
        p_items_per_page => 25,
        p_status         => 'PUBLISHED',
        p_comments       => 'Olympic Games OLAP and analytical REST endpoints'
    );

    add_get_endpoint('fdbo.olympics.api', 'fact-results',
        q'[SELECT * FROM FACT_RESULTS_V ORDER BY result_id]');

    add_get_endpoint('fdbo.olympics.api', 'rollup/decade-year-game',
        q'[SELECT * FROM OLAP_ROLLUP_DECADE_YEAR_GAME_V]');

    add_get_endpoint('fdbo.olympics.api', 'rollup/sport-event',
        q'[SELECT * FROM OLAP_ROLLUP_SPORT_EVENT_V]');

    add_get_endpoint('fdbo.olympics.api', 'rollup/country-season',
        q'[SELECT * FROM OLAP_ROLLUP_COUNTRY_SEASON_V]');

    add_get_endpoint('fdbo.olympics.api', 'cube/country-season',
        q'[SELECT * FROM OLAP_CUBE_COUNTRY_SEASON_V]');

    add_get_endpoint('fdbo.olympics.api', 'cube/sport-medal-gender',
        q'[SELECT * FROM OLAP_CUBE_SPORT_MEDAL_GENDER_V]');

    add_get_endpoint('fdbo.olympics.api', 'cube/era-agegroup',
        q'[SELECT * FROM OLAP_CUBE_ERA_AGEGROUP_V]');

    add_get_endpoint('fdbo.olympics.api', 'gsets/year-country-sport',
        q'[SELECT * FROM OLAP_GSETS_YEAR_COUNTRY_SPORT_V]');

    add_get_endpoint('fdbo.olympics.api', 'gsets/season-country-medal',
        q'[SELECT * FROM OLAP_GSETS_SEASON_COUNTRY_MEDAL_V]');

    add_get_endpoint('fdbo.olympics.api', 'gsets/decade-gender-age',
        q'[SELECT * FROM OLAP_GSETS_DECADE_GENDER_AGE_V]');

    add_get_endpoint('fdbo.olympics.api', 'wf/country-running-medals',
        q'[SELECT * FROM WV_COUNTRY_RUNNING_MEDALS_V]');

    add_get_endpoint('fdbo.olympics.api', 'wf/country-rank-per-game',
        q'[SELECT * FROM WV_COUNTRY_RANK_PER_GAME_V]');

    add_get_endpoint('fdbo.olympics.api', 'wf/sport-avg-age-trend',
        q'[SELECT * FROM WV_SPORT_AVG_AGE_TREND_V]');

    add_get_endpoint('fdbo.olympics.api', 'wf/country-medal-diff-avg',
        q'[SELECT * FROM WV_COUNTRY_MEDAL_DIFF_AVG_V]');

    add_get_endpoint('fdbo.olympics.api', 'wf/country-first-last-medal',
        q'[SELECT * FROM WV_COUNTRY_FIRST_LAST_MEDAL_V]');

    add_get_endpoint('fdbo.olympics.api', 'wf/sport-medal-share',
        q'[SELECT * FROM WV_SPORT_MEDAL_SHARE_V]');

    add_get_endpoint('fdbo.olympics.api', 'wf/athlete-sport-rank',
        q'[SELECT * FROM WV_ATHLETE_SPORT_RANK_V]');

    add_get_endpoint('fdbo.olympics.api', 'wf/country-lag-lead',
        q'[SELECT * FROM WV_COUNTRY_LAG_LEAD_V]');

    COMMIT;
END;

SELECT name, uri_prefix
FROM user_ords_modules
ORDER BY name;
