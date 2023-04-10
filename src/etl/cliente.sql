-- Databricks notebook source
WITH tb_join AS (

  SELECT DISTINCT
         t1.idPedido,
         t1.idCliente,
         t2.idVendedor,
         t3.descUF

  FROM silver.olist.pedido AS t1

  LEFT JOIN silver.olist.item_pedido AS t2
  ON t1.idPedido = t2.idPedido

  LEFT JOIN silver.olist.cliente AS t3
  ON t1.idCliente = t3.idCliente

  WHERE dtPedido < '2018-01-01'
  AND dtPedido >= add_months('2018-01-01', -6)
  AND idVendedor IS NOT NULL
  
 ),
 
tb_group AS (
 
  SELECT
    idVendedor,
    COUNT(DISTINCT descUF) AS qtdUFsPedidos,
    COUNT(DISTINCT CASE WHEN descUF = 'AC' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedido_AC, 
    COUNT(DISTINCT CASE WHEN descUF = 'AL' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedido_AL, 
    COUNT(DISTINCT CASE WHEN descUF = 'AM' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedido_AM, 
    COUNT(DISTINCT CASE WHEN descUF = 'AP' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedido_AP, 
    COUNT(DISTINCT CASE WHEN descUF = 'BA' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedido_BA, 
    COUNT(DISTINCT CASE WHEN descUF = 'CE' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedido_CE, 
    COUNT(DISTINCT CASE WHEN descUF = 'DF' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedido_DF, 
    COUNT(DISTINCT CASE WHEN descUF = 'ES' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedido_ES, 
    COUNT(DISTINCT CASE WHEN descUF = 'GO' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedido_GO, 
    COUNT(DISTINCT CASE WHEN descUF = 'MA' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedido_MA, 
    COUNT(DISTINCT CASE WHEN descUF = 'MG' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedido_MG, 
    COUNT(DISTINCT CASE WHEN descUF = 'MS' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedido_MS, 
    COUNT(DISTINCT CASE WHEN descUF = 'MT' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedido_MT, 
    COUNT(DISTINCT CASE WHEN descUF = 'PA' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedido_PA, 
    COUNT(DISTINCT CASE WHEN descUF = 'PB' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedido_PB, 
    COUNT(DISTINCT CASE WHEN descUF = 'PE' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedido_PE, 
    COUNT(DISTINCT CASE WHEN descUF = 'PI' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedido_PI, 
    COUNT(DISTINCT CASE WHEN descUF = 'PR' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedido_PR, 
    COUNT(DISTINCT CASE WHEN descUF = 'RJ' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedido_RJ, 
    COUNT(DISTINCT CASE WHEN descUF = 'RN' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedido_RN, 
    COUNT(DISTINCT CASE WHEN descUF = 'RO' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedido_RO, 
    COUNT(DISTINCT CASE WHEN descUF = 'RR' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedido_RR, 
    COUNT(DISTINCT CASE WHEN descUF = 'RS' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedido_RS, 
    COUNT(DISTINCT CASE WHEN descUF = 'SC' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedido_SC, 
    COUNT(DISTINCT CASE WHEN descUF = 'SE' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedido_SE, 
    COUNT(DISTINCT CASE WHEN descUF = 'SP' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedido_SP, 
    COUNT(DISTINCT CASE WHEN descUF = 'TO' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedido_TO 


  FROM tb_join

  GROUP BY idVendedor

)

SELECT '2018-01-01' AS dtReference, 
       *
       
FROM tb_group
        

-- COMMAND ----------


