# ui.R

ui <- page_navbar(
  #titlePanel(tagList(icon("shopping-cart"),"Carrinho Leve")),
  title = tagList("Carrinho Leve  ",icon("shopping-cart")),
  theme = bslib::bs_theme(primary = "orange"),
  navbar_options = navbar_options(
  bg = "orange"),
  
  # Painel de filtros
  nav_panel(
    title = "Lista de la Compra",
    
    # Seleção de supermercados
    accordion_panel(
      "Selecciona los supermercados donde quieras buscar:",
      fluidPage(
        checkboxGroupInput(
          inputId = "selected_markets",
          label = "",
          choices = sort(unique(data_df$Supermarket)),
          selected = sort(unique(data_df$Supermarket))
        )
      )
    ),
    
    # Lista de compras
    accordion_panel(
      "Crea tu lista de la compra",
      fluidRow(
        column(
          width = 4,
          pickerInput(
            inputId = "produtos_picker",
            label = "Elige los productos:",
            choices = sort(unique(data_df$Produto)),  # vem do global.R
            multiple = TRUE,
            options = list(`actions-box` = TRUE,
                           `select-all-text` = "Seleccionar todos",
                           `deselect-all-text` = "Limpiar",
                           `live-search` = TRUE)
          )
        ),
        column(
          width = 8,
          h4("Tu Lista de la Compra:"),
          uiOutput("inputs_quantidade")  # Inputs de quantidade gerados dinamicamente
        )
      )
    )
    
    
  ),
  
  # Painel de resultados
  nav_panel(
    title = "Donde Comprar",
   ### CARDS ###  
    p("Si compras todos los artículos solo en este supermercado"),
    uiOutput("market_value_boxes"),  # Value_boxes dinâmicos
   layout_columns( 
     card(
       card_header("Cómo cambia el precio al repartir la compra entre varios supermercados"),
       card_body(
     plotOutput("line_plot")
     )
     #style = "border: 1px solid #ccc; border-radius: 8px;"),
     ),
   navset_card_underline(
       title = "Resumen de la compra",
       nav_panel("Opción 1", uiOutput("tabela_kable")),
       nav_panel("Opción 2", "Probando 2")
   ),
   col_widths = c(4, 8))  #the sum must be 12
   )
)
