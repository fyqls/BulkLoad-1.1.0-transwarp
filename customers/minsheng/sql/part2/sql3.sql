SELECT TXN_TYP_CD, COUNT (*)
  FROM HYPERBASE_CBM_INDIV_DEP_DTL
 WHERE TXN_ORG_ID = 'ORGID805'
GROUP BY TXN_TYP_CD;