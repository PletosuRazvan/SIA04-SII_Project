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
  ORDS.ENABLE_OBJECT(TRUE, 'FDBO', 'OLAP_ROLLUP_DECADE_YEAR_GAME_V', 'VIEW', 'olap_rollup_decade_year_game_v');
  ORDS.ENABLE_OBJECT(TRUE, 'FDBO', 'OLAP_ROLLUP_SPORT_EVENT_V', 'VIEW', 'olap_rollup_sport_event_v');
  ORDS.ENABLE_OBJECT(TRUE, 'FDBO', 'OLAP_ROLLUP_COUNTRY_SEASON_V', 'VIEW', 'olap_rollup_country_season_v');
  ORDS.ENABLE_OBJECT(TRUE, 'FDBO', 'OLAP_CUBE_COUNTRY_SEASON_V', 'VIEW', 'olap_cube_country_season_v');
  ORDS.ENABLE_OBJECT(TRUE, 'FDBO', 'OLAP_CUBE_SPORT_MEDAL_GENDER_V', 'VIEW', 'olap_cube_sport_medal_gender_v');
  ORDS.ENABLE_OBJECT(TRUE, 'FDBO', 'OLAP_CUBE_ERA_AGEGROUP_V', 'VIEW', 'olap_cube_era_agegroup_v');
  ORDS.ENABLE_OBJECT(TRUE, 'FDBO', 'OLAP_GSETS_YEAR_COUNTRY_SPORT_V', 'VIEW', 'olap_gsets_year_country_sport_v');
  ORDS.ENABLE_OBJECT(TRUE, 'FDBO', 'OLAP_GSETS_SEASON_COUNTRY_MEDAL_V', 'VIEW', 'olap_gsets_season_country_medal_v');
  ORDS.ENABLE_OBJECT(TRUE, 'FDBO', 'OLAP_GSETS_DECADE_GENDER_AGE_V', 'VIEW', 'olap_gsets_decade_gender_age_v');
  COMMIT;
END;

BEGIN
  ORDS.ENABLE_OBJECT(TRUE, 'FDBO', 'WV_COUNTRY_RUNNING_MEDALS_V', 'VIEW', 'wv_country_running_medals_v');
  ORDS.ENABLE_OBJECT(TRUE, 'FDBO', 'WV_COUNTRY_RANK_PER_GAME_V', 'VIEW', 'wv_country_rank_per_game_v');
  ORDS.ENABLE_OBJECT(TRUE, 'FDBO', 'WV_SPORT_AVG_AGE_TREND_V', 'VIEW', 'wv_sport_avg_age_trend_v');
  ORDS.ENABLE_OBJECT(TRUE, 'FDBO', 'WV_COUNTRY_MEDAL_DIFF_AVG_V', 'VIEW', 'wv_country_medal_diff_avg_v');
  ORDS.ENABLE_OBJECT(TRUE, 'FDBO', 'WV_COUNTRY_FIRST_LAST_MEDAL_V', 'VIEW', 'wv_country_first_last_medal_v');
  ORDS.ENABLE_OBJECT(TRUE, 'FDBO', 'WV_SPORT_MEDAL_SHARE_V', 'VIEW', 'wv_sport_medal_share_v');
  ORDS.ENABLE_OBJECT(TRUE, 'FDBO', 'WV_ATHLETE_SPORT_RANK_V', 'VIEW', 'wv_athlete_sport_rank_v');
  ORDS.ENABLE_OBJECT(TRUE, 'FDBO', 'WV_COUNTRY_LAG_LEAD_V', 'VIEW', 'wv_country_lag_lead_v');
  COMMIT;
END;
