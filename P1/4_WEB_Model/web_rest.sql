BEGIN
    ORDS.ENABLE_SCHEMA(
        p_enabled             => TRUE,
        p_schema              => 'PROIECT_OLAP',
        p_url_mapping_type    => 'BASE_PATH',
        p_base_path           => 'olympics_api',
        p_auto_rest_auth      => FALSE
    );
    COMMIT;
END;
/

BEGIN
    ORDS.DEFINE_MODULE(
        p_module_name    => 'medal_analytics',
        p_base_path      => 'medals/',
        p_items_per_page => 50
    );

    ORDS.DEFINE_TEMPLATE(
        p_module_name    => 'medal_analytics',
        p_pattern        => 'top_countries'
    );

    ORDS.DEFINE_HANDLER(
        p_module_name    => 'medal_analytics',
        p_pattern        => 'top_countries',
        p_method         => 'GET',
        p_source_type    => ORDS.source_type_collection_feed,
        p_source         => 'SELECT * FROM vw_cube_tara_sport WHERE Tara != ''TOATE TARILE'' AND Sport = ''TOATE SPORTURILE'' ORDER BY Nr_Medalii DESC',
        p_items_per_page => 10
    );
    COMMIT;
END;
/

BEGIN
    ORDS.DEFINE_MODULE(
        p_module_name    => 'athlete_services',
        p_base_path      => 'athletes/',
        p_items_per_page => 10
    );

    ORDS.DEFINE_TEMPLATE(
        p_module_name    => 'athlete_services',
        p_pattern        => ':id'
    );

    ORDS.DEFINE_HANDLER(
        p_module_name    => 'athlete_services',
        p_pattern        => ':id',
        p_method         => 'GET',
        p_source_type    => ORDS.source_type_collection_item,
        p_source         => 'SELECT * FROM vw_consolidare_olimpica WHERE Athlete_ID = :id'
    );
    COMMIT;
END;
/

BEGIN
    ORDS.DEFINE_MODULE(
        p_module_name    => 'trends_api',
        p_base_path      => 'trends/',
        p_items_per_page => 25
    );

    ORDS.DEFINE_TEMPLATE(
        p_module_name    => 'trends_api',
        p_pattern        => 'participation'
    );

    ORDS.DEFINE_HANDLER(
        p_module_name    => 'trends_api',
        p_pattern        => 'participation',
        p_method         => 'GET',
        p_source_type    => ORDS.source_type_collection_feed,
        p_source         => 'SELECT * FROM vw_analiza_evolutie_participare WHERE Atleti_Editia_Trecuta IS NOT NULL'
    );
    COMMIT;
END;
/