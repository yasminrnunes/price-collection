# global.R
library(shiny)
library(bslib)
library(shinyWidgets)
library(shinyjs)
library(fontawesome)
library(dplyr)
library(bsicons)
library(ggplot2)
library(shiny)
library(knitr)
library(kableExtra)
library(DT)

# Full Data Frame
data_df  <- data.frame(
  Produto = c("Arroz","Arroz", "Feijão", "Macarrão", "Macarrão", "Leite", "Café", "Açúcar", "Farinha", "Óleo", "Sal", "Molho de Tomate"),
  Categoria = c("Grãos", "Grãos", "Grãos", "Massas", "Massas", "Laticínios", "Bebidas", "Doces", "Grãos", "Óleos", "Temperos", "Enlatados"),
  Supermarket = c("Tenda", "Carrefour","Tenda", "Carrefour", "Tenda", "Carrefour", "StMarche", "Tenda", "Carrefour", "StMarche", "Carrefour", "StMarche"),
  Price = c(2.5,3,4,5,4.5,6,7,5,4,10,9,8),
  stringsAsFactors = FALSE
)

# Data frame de produtos e categorias
produtos_categorias <- data.frame(
  Produto = c("Arroz", "Feijão", "Macarrão", "Leite", "Café", "Açúcar", "Farinha", "Óleo", "Sal", "Molho de Tomate"),
  Categoria = c("Grãos", "Grãos", "Massas", "Laticínios", "Bebidas", "Doces", "Grãos", "Óleos", "Temperos", "Enlatados"),
  stringsAsFactors = FALSE
)

# Data frame de supermercados e logos
market_df <- data.frame(
  market_name = c("Tenda", "Carrefour", "StMarche"),
  logo_file = c("logo_tenda.svg", "logo_carrefour.webp", "logo_stmarche.webp"),
  stringsAsFactors = FALSE
)

# Data frame do gráfico de lineas
data <- data.frame(
  qtd_supermercados = 1:4,
  Valor = c(25, 20, 18, 15)
)

# Data frame do resultado de seleçao dos supermercados
df <- data.frame(
  Supermercado = c("Tenda","Tenda","StMarche","Carrefour","Carrefour"),
  Produto      = c("Arroz","Feijão","Leite","Macarrão","Óleo"),
  `Valor.Unitário` = c(10, 8, 12, 15, 20),
  Quantidade = c(1,1,2,1,2),
  `Valor.Total` = c(10, 8, 24, 15, 40),
  stringsAsFactors = FALSE
)