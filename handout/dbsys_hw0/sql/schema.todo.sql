-- This script creates each of the TPCH tables using the SQL 'create table' command.
drop table if exists part;
drop table if exists supplier;
drop table if exists partsupp;
drop table if exists customer;
drop table if exists orders;
drop table if exists lineitem;
drop table if exists nation;
drop table if exists region;

-- Notes:
--   1) Use all lowercase letters for table and column identifiers.
--   2) Use only INTEGER/REAL/TEXT datatypes. Use TEXT for dates.
--   3) Do not specify any integrity contraints (e.g., PRIMARY KEY, FOREIGN KEY).

-- Students should fill in the followins statements:

create table part (
    p_partkey       INT, 
    p_name          TEXT,
    p_mfgr          TEXT,
    p_brand         TEXT,
    p_type          TEXT,
    p_size          INT,
    p_container     TEXT,
    p_retailprice   REAL,
    p_comment       TEXT
);

create table supplier (
    s_suppkey       INT,     
    s_name          TEXT,
    s_address       TEXT,
    s_nationkey     INT,
    s_phone         TEXT,
    s_acctbal       REAL,
    s_comment       TEXT
);

create table partsupp (
    ps_partkey      INT,
    ps_suppkey      INT,
    ps_availqty     INT,
    ps_supplycost   REAL,
    ps_comment      TEXT
);

create table customer (
    c_custkey       INT,
    c_name          TEXT,
    c_address       TEXT,
    c_nationkey     INT,
    c_phone         TEXT,
    c_acctbal       REAL,
    c_mktsegment    TEXT,
    c_comment       TEXT
);

create table orders (
    o_orderkey      INT,         
    o_custkey       INT,
    o_orderstatus   TEXT,
    o_totalprice    REAL,
    o_orderdate     TEXT,
    o_orderpriority TEXT,
    o_clerk         TEXT,
    o_shippriority  INT,    
    o_comment       TEXT
);

create table lineitem (
    l_orderkey      INT,
    l_partkey       INT,
    l_suppkey       INT,
    l_linenumber    INT,
    l_quantity      REAL,
    l_extendedprice REAL,
    l_discount      REAL,
    l_tax           REAL,
    l_returnflag    TEXT,
    l_linestatus    TEXT,
    l_shipdate      TEXT,
    l_commitdate    TEXT,
    l_receiptdate   TEXT,
    l_shipinstrust  TEXT,
    l_shipmode      TEXT,
    l_comment       TEXT
);

create table nation (
    n_nationkey     INT,
    n_name          TEXT,
    n_regionkey     INT,
    n_comment       TEXT
);

create table region (
    r_regionkey     INT,
    r_name          TEXT,
    r_comment       TEXT
);
