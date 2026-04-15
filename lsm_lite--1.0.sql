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

-- FDW Handler Declaration
CREATE FUNCTION lsm_fdw_handler() 
RETURNS fdw_handler
AS 'MODULE_PATHNAME', 'lsm_fdw_handler'
LANGUAGE C STRICT;

-- Create the Wrapper
CREATE FOREIGN DATA WRAPPER lsm_wrapper HANDLER lsm_fdw_handler;

-- Create a Default Server
CREATE SERVER lsm_server FOREIGN DATA WRAPPER lsm_wrapper;
