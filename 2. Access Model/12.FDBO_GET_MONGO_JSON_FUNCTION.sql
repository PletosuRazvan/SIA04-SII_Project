CREATE OR REPLACE FUNCTION get_mongo_json(p_url VARCHAR2)
RETURN CLOB
IS
  req   UTL_HTTP.req;
  resp  UTL_HTTP.resp;
  buf   VARCHAR2(32767);
  res   CLOB;
BEGIN
  DBMS_LOB.createtemporary(res, TRUE);

  req := UTL_HTTP.begin_request(p_url, 'GET', 'HTTP/1.1');
  UTL_HTTP.set_authentication(req, 'admin', 'secret', 'Basic');
  UTL_HTTP.set_header(req, 'Accept', 'application/json');

  resp := UTL_HTTP.get_response(req);

  BEGIN
    LOOP
      UTL_HTTP.read_text(resp, buf, 32767);
      DBMS_LOB.writeappend(res, LENGTH(buf), buf);
    END LOOP;
  EXCEPTION
    WHEN UTL_HTTP.end_of_body THEN
      NULL;
  END;

  UTL_HTTP.end_response(resp);

  RETURN res;
END;

SELECT LENGTH(get_mongo_json('http://localhost:8081/olympics_nosql/game_summary_docs')) AS json_length
FROM dual;
