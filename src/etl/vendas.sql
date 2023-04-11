-- CREATE TABLE silver.analytics.fs_vendedor_venda

WITH tb_pedido_item AS (

  SELECT t2.*,
         t1.dtPedido
          
  FROM silver.olist.pedido AS t1

  LEFT JOIN silver.olist.item_pedido AS t2
  ON t1.idPedido = t2.idPedido

  WHERE t1.dtPedido < '{date}'
  AND t1.dtPedido >= add_months('{date}', -6)
  AND t2.idVendedor IS NOT NULL

),

tb_summary AS (

  SELECT idVendedor,
        -- Quantidade de vendas (pedidos)
        COUNT(DISTINCT idPedido) AS qtdPedidos,

        -- Quantidade de vendas (dias)
        COUNT(DISTINCT DATE(dtPedido)) AS qtdDiasPedidos,

        -- Quantidade de vendas (itens)
        COUNT(idProduto) AS qtdDItensPedidos,

        -- Dias sem vender (recência)
        DATEDIFF('{date}', MAX(dtPedido)) AS qtdRecencia,

        -- Ticket Médio
        SUM(vlPreco) / COUNT(DISTINCT idPedido) AS avgTicket,

        -- Valor médio por produto
        AVG(vlPreco) AS avgValorProduto,

        -- Maior valor de produto
        MAX(vlPreco) AS maxValorProduto,

        -- Menor valor de produto
        MIN(vlPreco) AS minValorProduto,

        -- Média de itens por pedido
        COUNT(idProduto) / COUNT(DISTINCT idPedido) AS avgProdutoPedido

  FROM tb_pedido_item

  GROUP BY idVendedor

),

tb_pedido_summary AS (

  SELECT idVendedor,
        idPedido,
        SUM(vlPreco) AS vlPreco

  FROM tb_pedido_item

  GROUP BY idVendedor, idPedido

),

tb_pedido_minmax AS (

  SELECT idVendedor,
        -- Maior valor de venda
        MAX(vlPreco) AS maxValorPedido,

        -- Menor valor de venda
        MIN(vlPreco) AS minValorPedido

  FROM tb_pedido_summary

  GROUP BY idVendedor

),

tb_life AS (

  SELECT t2.idVendedor,
        -- LTV
        SUM(vlPreco) AS ltv,
        -- Dias desde a 1ª venda
        MAX(DATEDIFF('{date}', t1.dtPedido)) AS qtdDiasBase
          
  FROM silver.olist.pedido AS t1

  LEFT JOIN silver.olist.item_pedido AS t2
  ON t1.idPedido = t2.idPedido

  WHERE t1.dtPedido < '{date}'
  AND t2.idVendedor IS NOT NULL

  GROUP BY t2.idVendedor

),
       
tb_dtpedido AS (

  SELECT DISTINCT 
        idVendedor,
        DATE(dtPedido) as dtPedido

  FROM tb_pedido_item

  ORDER BY 1, 2

),

tb_lag AS (

  SELECT *,
        LAG(dtPedido) OVER (PARTITION BY idVendedor ORDER BY dtPedido) AS lag1
  FROM tb_dtpedido

),

tb_intervalo AS (

  SELECT idVendedor,
        -- Intervalo médio entre vendas (dias)
        AVG(DATEDIFF(dtPedido, lag1)) AS avgIntervaloVendas
  FROM tb_lag

  GROUP BY idVendedor

)

SELECT '{date}' AS dtReference,
       t1.*,
       t2.minValorPedido,
       t2.maxValorPedido,
       t3.ltv,
       t3.qtdDiasBase,
       t4.avgIntervaloVendas

FROM tb_summary AS t1

LEFT JOIN tb_pedido_minmax AS t2
ON t1.idVendedor = t2.idVendedor

LEFT JOIN tb_life AS t3
ON t1.idVendedor = t3.idVendedor

LEFT JOIN tb_intervalo AS t4
ON t1.idVendedor = t4.idVendedor

