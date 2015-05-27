CREATE TABLE lineitem (orderkey INT , partkey INT , suppkey INT , linenumber INT , quantity DECIMAL , extendedprice DECIMAL , discount DECIMAL , tax DECIMAL , returnflag CHAR (1) , linestatus CHAR (1) , shipdate DATE , commitdate DATE , receiptdate DATE , shipinstruct CHAR (25) , shipmode CHAR (10) , comment VARCHAR (44) )

CREATE TABLE orders (orderkey INT , custkey INT , orderstatus CHAR (1) , totalprice DECIMAL , orderdate DATE , orderpriority CHAR (15) , clerk CHAR (15) , shippriority INT , comment VARCHAR (79) )

CREATE TABLE part (partkey INT , name VARCHAR (55) , mfgr CHAR (25) , brand CHAR (10) , type VARCHAR (25) , size INT , container CHAR (10) , retailprice DECIMAL  , comment VARCHAR (23) )

CREATE TABLE customer (custkey INT , name VARCHAR (25) , address VARCHAR (40) , nationkey INT , phone CHAR (15) , acctbal DECIMAL , mktsegment CHAR (10) , comment VARCHAR (117) )

CREATE TABLE SUPPLIER (suppkey INT , name CHAR (25) , address VARCHAR (40) , nationkey INT , phone CHAR (15) , acctbal DECIMAL , comment VARCHAR (101) )

CREATE TABLE PARTSUPP (partkey INT , suppkey INT , availqty INT , supplycost DECIMAL , comment VARCHAR (199) )

CREATE TABLE NATION (nationkey INT , name CHAR (25) , regionkey INT , comment VARCHAR (152) )

CREATE TABLE REGION (regionkey INT , name CHAR (25) , comment VARCHAR (152) )

SELECT sum(extendedprice * discount) AS revenue FROM lineitem WHERE shipdate >=
 DATE('1994-01-01') AND shipdate < date('1995-01-01') AND discount > 0.05 AND discount < 0.07 AND quantity < 24