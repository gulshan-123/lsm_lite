-- \echo Use "CREATE EXTENSION lsm_lite" to load this file. \quit

CREATE FUNCTION lsm_put(key integer, val integer) 
RETURNS boolean
AS 'MODULE_PATHNAME', 'lsm_put'
LANGUAGE C STRICT;

CREATE FUNCTION lsm_get(key integer) 
RETURNS integer
AS 'MODULE_PATHNAME', 'lsm_get'
LANGUAGE C STRICT;

CREATE FUNCTION lsm_delete(key integer) 
RETURNS boolean
AS 'MODULE_PATHNAME', 'lsm_delete'
LANGUAGE C STRICT;
