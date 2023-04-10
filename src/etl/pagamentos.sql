-- Databricks notebook source
WITH tb_pedidos AS (

  SELECT DISTINCT
         t1.idPedido,
         t2.idVendedor

  FROM silver.olist.pedido AS t1

  LEFT JOIN silver.olist.item_pedido as t2
  ON t1.idPedido = t2.idPedido

  WHERE t1.dtPedido < '2018-01-01'
    AND t1.dtPedido >= add_months('2018-01-01', -6)
    AND t2.idVendedor IS NOT NULL

),

tb_join AS (

  SELECT t1.idVendedor,
         t2.*
         
  FROM tb_pedidos AS t1

  LEFT JOIN silver.olist.pagamento_pedido AS t2
  ON t1.idPedido = t2.idPedido
  
),

tb_group AS (

  SELECT idVendedor,
         descTipoPagamento,
         COUNT(DISTINCT idPedido) AS qtdPedidoMeioPagamento,
         SUM(vlPagamento) AS vlPedidoMeioPagamento

  FROM tb_join

  GROUP BY idVendedor, descTipoPagamento
  ORDER BY idVendedor, descTipoPagamento

),

tb_summary(

  SELECT idVendedor,

  SUM(CASE WHEN descTipoPagamento = 'boleto' THEN qtdPedidoMeioPagamento ELSE 0 END) AS qtd_boleto_pedido,
  SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN qtdPedidoMeioPagamento ELSE 0 END) AS qtd_credit_card_pedido,
  SUM(CASE WHEN descTipoPagamento = 'voucher' THEN qtdPedidoMeioPagamento ELSE 0 END) AS qtd_voucher_pedido,
  SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN qtdPedidoMeioPagamento ELSE 0 END) AS qtd_debit_card_pedido,

  SUM(CASE WHEN descTipoPagamento = 'boleto' THEN vlPedidoMeioPagamento ELSE 0 END) AS valor_boleto_pedido,
  SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN vlPedidoMeioPagamento ELSE 0 END) AS valor_credit_card_pedido,
  SUM(CASE WHEN descTipoPagamento = 'voucher' THEN vlPedidoMeioPagamento ELSE 0 END) AS valor_voucher_pedido,
  SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN vlPedidoMeioPagamento ELSE 0 END) AS valor_debit_card_pedido,

  SUM(CASE WHEN descTipoPagamento = 'boleto' THEN qtdPedidoMeioPagamento ELSE 0 END) / SUM(qtdPedidoMeioPagamento) AS pct_qtd_boleto_pedido,
  SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN qtdPedidoMeioPagamento ELSE 0 END) / SUM(qtdPedidoMeioPagamento) AS pct_qtd_credit_card_pedido,
  SUM(CASE WHEN descTipoPagamento = 'voucher' THEN qtdPedidoMeioPagamento ELSE 0 END) / SUM(qtdPedidoMeioPagamento) AS pct_qtd_voucher_pedido,
  SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN qtdPedidoMeioPagamento ELSE 0 END) / SUM(qtdPedidoMeioPagamento) AS pct_qtd_debit_card_pedido,

  SUM(CASE WHEN descTipoPagamento = 'boleto' THEN vlPedidoMeioPagamento ELSE 0 END) / SUM(vlPedidoMeioPagamento) AS pct_valor_boleto_pedido,
  SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN vlPedidoMeioPagamento ELSE 0 END) / SUM(vlPedidoMeioPagamento) AS pct_valor_credit_card_pedido,
  SUM(CASE WHEN descTipoPagamento = 'voucher' THEN vlPedidoMeioPagamento ELSE 0 END) / SUM(vlPedidoMeioPagamento) AS pct_valor_voucher_pedido,
  SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN vlPedidoMeioPagamento ELSE 0 END) / SUM(vlPedidoMeioPagamento) AS pct_valor_debit_card_pedido

  FROM tb_group

  GROUP BY idVendedor

),

tb_cartao AS (

  SELECT idVendedor,
         AVG(nrParcelas) AS avgQtdParcelas,
         PERCENTILE(nrParcelas, 0.5) AS medianQtdParcelas,
         MAX(nrParcelas) AS maxQtdParcelas,
         MIN(nrParcelas) AS minQtdParcelas

  FROM tb_join

  WHERE descTipoPagamento = 'credit_card'

  GROUP BY idVendedor
  
)

SELECT '2018-01-01' AS dtReference,
       t1.* ,
       t2.avgQtdParcelas,
       t2.medianQtdParcelas,
       t2.maxQtdParcelas,
       t2.minQtdParcelas

FROM tb_summary as t1
LEFT JOIN tb_cartao as t2

ON t1.idVendedor = t2.idVendedor

-- COMMAND ----------


