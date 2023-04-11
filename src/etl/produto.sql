-- Databricks notebook source
-- CREATE TABLE silver.analytics.fs_vendedor_produto

WITH tb_join AS (

  SELECT DISTINCT
         t2.idVendedor,
         t3.*

  FROM silver.olist.pedido AS t1

  LEFT JOIN silver.olist.item_pedido AS t2
  ON t1.idPedido = t2.idPedido

  LEFT JOIN silver.olist.produto AS t3
  ON t2.idProduto = t3.idProduto

  WHERE dtPedido < '2018-01-01'
  AND dtPedido >= add_months('2018-01-01', -6)
  AND t2.idVendedor IS NOT NULL
  
),

tb_summary AS (

  SELECT idVendedor,
        AVG(COALESCE(nrFotos, 0)) AS avgFotosProduto,
        AVG(vlComprimentoCm * vlAlturaCm * vlLarguraCm) AS avgVolumeProduto,
        PERCENTILE(vlComprimentoCm * vlAlturaCm * vlLarguraCm, 0.5) AS medianVolumeProduto,
        MIN(vlComprimentoCm * vlAlturaCm * vlLarguraCm) AS minVolumeProduto,
        MAX(vlComprimentoCm * vlAlturaCm * vlLarguraCm) AS maxVolumeProduto,

        COUNT(DISTINCT CASE WHEN descCategoria = 'cama_mesa_banho' THEN idProduto END) / COUNT(DISTINCT idProduto) AS pctCategoria_cama_mesa_banho,
        COUNT(DISTINCT CASE WHEN descCategoria = 'beleza_saude' THEN idProduto END) / COUNT(DISTINCT idProduto) AS pctCategoria_beleza_saude,
        COUNT(DISTINCT CASE WHEN descCategoria = 'esporte_lazer' THEN idProduto END) / COUNT(DISTINCT idProduto) AS pctCategoria_esporte_lazer,
        COUNT(DISTINCT CASE WHEN descCategoria = 'informatica_acessorios' THEN idProduto END) / COUNT(DISTINCT idProduto) AS pctCategoria_informatica_acessorios,
        COUNT(DISTINCT CASE WHEN descCategoria = 'moveis_decoracao' THEN idProduto END) / COUNT(DISTINCT idProduto) AS pctCategoria_moveis_decoracao,
        COUNT(DISTINCT CASE WHEN descCategoria = 'utilidades_domesticas' THEN idProduto END) / COUNT(DISTINCT idProduto) AS pctCategoria_utilidades_domesticas,
        COUNT(DISTINCT CASE WHEN descCategoria = 'relogios_presentes' THEN idProduto END) / COUNT(DISTINCT idProduto) AS pctCategoria_relogios_presentes,
        COUNT(DISTINCT CASE WHEN descCategoria = 'telefonia' THEN idProduto END) / COUNT(DISTINCT idProduto) AS pctCategoria_telefonia,
        COUNT(DISTINCT CASE WHEN descCategoria = 'automotivo' THEN idProduto END) / COUNT(DISTINCT idProduto) AS pctCategoria_automotivo,
        COUNT(DISTINCT CASE WHEN descCategoria = 'brinquedos' THEN idProduto END) / COUNT(DISTINCT idProduto) AS pctCategoria_brinquedos,
        COUNT(DISTINCT CASE WHEN descCategoria = 'cool_stuff' THEN idProduto END) / COUNT(DISTINCT idProduto) AS pctCategoria_cool_stuff,
        COUNT(DISTINCT CASE WHEN descCategoria = 'ferramentas_jardim' THEN idProduto END) / COUNT(DISTINCT idProduto) AS pctCategoria_ferramentas_jardim,
        COUNT(DISTINCT CASE WHEN descCategoria = 'perfumaria' THEN idProduto END) / COUNT(DISTINCT idProduto) AS pctCategoria_perfumaria,
        COUNT(DISTINCT CASE WHEN descCategoria = 'bebes' THEN idProduto END) / COUNT(DISTINCT idProduto) AS pctCategoria_bebes,
        COUNT(DISTINCT CASE WHEN descCategoria = 'eletronicos' THEN idProduto END) / COUNT(DISTINCT idProduto) AS pctCategoria_eletronicos


  FROM tb_join

  GROUP BY idVendedor

)

SELECT '2018-01-01' AS dtReference,
       *

FROM tb_summary


