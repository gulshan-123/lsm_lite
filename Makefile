MODULE_big = lsm_lite
OBJS = lsm_lite.o lsm_skiplist.o lsm_sstable.o

EXTENSION = lsm_lite
DATA = lsm_lite--1.0.sql

PG_CONFIG = /home/gulshan/postgres-dev/install/bin/pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)