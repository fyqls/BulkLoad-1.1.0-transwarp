set hyperbase.reader=true;

SELECT *
  FROM HYPERBASE_CBM_INDIV_DEP_DTL
 WHERE TXN_TME > '20000930101010' AND TXN_TME < '20100930101010' AND CSR_ID = '0000000001';
