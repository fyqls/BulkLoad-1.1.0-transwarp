SELECT BASE.rk.CUSTNO,
       BASE.CUSTNAME,
       BASE.SEX,
       SUB.AGEPART,
       SUB.DEPOSITBALAVGL3MPART,
       CON.DEPOSITBALAVGL3M,
       CON.FASSETBAL,
       CMAN.rk.CEMPID,
       ORG.ORGID
  FROM                   HYPERBASE_FIN_CUST_INFO_BASE BASE
                      INNER JOIN
                         HYPERBASE_FIN_CUST_INFO_SUB SUB
                      ON BASE.rk.CUSTNO = SUB.rk.CUSTNO
                   INNER JOIN
                      HYPERBASE_FIN_CONFINPROFILE_BASE CON
                   ON CON.rk.CUSTNO = BASE.rk.CUSTNO
                INNER JOIN
                   HYPERBASE_FIN_CUST_CMANAGER CMAN
                ON CMAN.CUSTNO = BASE.rk.CUSTNO
       INNER JOIN
          HYPERBASE_FIN_CUST_ORG ORG
       ON ORG.rk.CUSTNO = BASE.rk.CUSTNO
 WHERE BASE.rk.CUSTNO = '0000000001';
