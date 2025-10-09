# server.R

server <- function(input, output, session) {
  
  # Reactive values para produtos selecionados
  lista_produtos <- reactiveVal(character())
  quantidades <- reactiveValues()
  
  # Atualizar lista de produtos selecionados
  observeEvent(input$produtos_picker, {
    lista_atual <- lista_produtos()
    novos <- setdiff(input$produtos_picker, lista_atual)
    removidos <- setdiff(lista_atual, input$produtos_picker)
    lista_atualizada <- setdiff(union(lista_atual, novos), removidos)
    lista_produtos(lista_atualizada)
  })
  
  # Atualizar opções de produtos baseado nos supermercados selecionados
  observe({
    if (length(input$supermercados) > 0) {
      # Filtrar produtos disponíveis nos supermercados selecionados
      produtos_disponiveis <- data_df %>%
        filter(Supermarket %in% input$supermercados) %>%
        pull(Produto) %>%
        unique() %>%
        sort()
      
      updateCheckboxGroupInput(
        session,
        "produtos",
        choices = produtos_disponiveis,
        selected = produtos_disponiveis
      )
    } else {
      # Se nenhum supermercado selecionado, limpar produtos
      updateCheckboxGroupInput(
        session,
        "produtos",
        choices = NULL,
        selected = NULL
      )
    }
  })
  
  # Dados filtrados
  dados_filtrados <- reactive({
    req(input$supermercados, input$produtos)
    
    data_df %>%
      filter(
        Supermarket %in% input$supermercados,
        Produto %in% input$produtos
      ) %>%
      arrange(Supermarket, Produto)
  })
  
  # Gerar inputs de quantidade dinamicamente
  output$inputs_quantidade <- renderUI({
    req(lista_produtos())
    
    produtos_selecionados <- produtos_categorias %>%
      filter(Produto %in% lista_produtos()) %>%
      arrange(Categoria, Produto)
    
    categorias <- unique(produtos_selecionados$Categoria)
    
    ui_lista <- lapply(categorias, function(cat) {
      produtos_da_categoria <- produtos_selecionados %>% filter(Categoria == cat)
      
      bloco_categoria <- list(div(class = "categoria-titulo", cat))
      
      produtos_ui <- lapply(produtos_da_categoria$Produto, function(produto) {
        valor_inicial <- isolate(quantidades[[produto]])
        if (is.null(valor_inicial)) valor_inicial <- 1
        
        fluidRow(
          column(width = 4, tags$strong(produto)),
          column(width = 3,
                 div(class = "small-input",
                     numericInput(
                       inputId = paste0("qtd_", produto),
                       label = NULL,
                       value = valor_inicial,
                       min = 1, step = 1
                     )
                 )
          ),
          column(width = 2,
                 actionButton(
                   inputId = paste0("remover_", produto),
                   label = NULL,
                   icon = icon("trash"),
                   class = "btn btn-danger btn-sm"
                 )
          )
        )
      })
      
      c(bloco_categoria, produtos_ui)
    })
    
    do.call(tagList, unlist(ui_lista, recursive = FALSE))
  })
  
  # Atualizar reactiveValues com quantidades
  observe({
    lapply(lista_produtos(), function(produto) {
      observeEvent(input[[paste0("qtd_", produto)]], {
        quantidades[[produto]] <- input[[paste0("qtd_", produto)]]
      }, ignoreInit = TRUE, ignoreNULL = TRUE)
    })
  })
  
  # Remover produto da lista
  observe({
    lapply(lista_produtos(), function(produto) {
      observeEvent(input[[paste0("remover_", produto)]], {
        nova_lista <- setdiff(lista_produtos(), produto)
        lista_produtos(nova_lista)
        updatePickerInput(session, "produtos_picker", selected = nova_lista)
      }, ignoreInit = TRUE, ignoreNULL = TRUE)
    })
  })
  
  # Gerar value_boxes dinâmicos para supermercados selecionados
  output$market_value_boxes <- renderUI({
    req(input$selected_markets)
    
    selected_df <- market_df[market_df$market_name %in% input$selected_markets, ]
    
    # Cria uma lista de value_boxes
    boxes_list <- lapply(1:nrow(selected_df), function(i) {
      value_box(
        title = NULL,#tags$span(selected_df$market_name[i], style = "font-size:16px; text-align:center;"),
        value = tags$span(sample(50:100, 1), style = "font-size:22px; font-weight:bold; text-align:center;"),  # exemplo de métrica dinâmica
        theme = "bg-gradient-yellow-orange",
        showcase = tags$img(
          src = selected_df$logo_file[i],
          class = "img-fluid",
          style = "max-height: 50px;"
        ),
        showcase_layout = "left center",
        full_screen = FALSE, fill = TRUE, height = "130px"
      )
    })
    
    # Passa a lista para layout_columns
    do.call(layout_columns, c(boxes_list, list(col_widths = rep(1, length(boxes_list)))))
  })

  output$line_plot <- renderPlot({
    ggplot(data, aes(x = qtd_supermercados, y = Valor,ymin=0)) +
      geom_line(color = "orange", size = 1.2) +     # Linha
      geom_point(color = "orange", size = 3) +       # Pontos
      labs(x = "Número de Supermercados", 
           y = "Importe total de la compra (R$)") +
      theme_minimal(base_size = 14)
  })
  
  output$tabela_kable <- renderUI({
    # Criar linha de total manualmente
    linha_total <- data.frame(
      Supermercado = "TOTAL",
      Produto = "",
      Valor.Unitário = "",
      Quantidade = "",
      Valor.Total = sum(df$Valor.Total, na.rm = TRUE),
      stringsAsFactors = FALSE
    )
    
    # Combinar dataframes
    df_com_total <- rbind(df, linha_total)
    
    # Criar tabela com melhor formatação
    tbl <- kable(df_com_total, 
                 align = c("l", "l", "r", "r", "r"), 
                 format = "html",
                 col.names = c("Supermercado", "Producto", "Precio Unitario (R$)", 
                               "Cantidad", "Subtotal (R$)")) %>%
      kable_styling(
        bootstrap_options = c("striped", "hover", "condensed", "bordered"),
        full_width = TRUE,
        position = "center",
        font_size = 14
      ) %>%
      column_spec(1, bold = TRUE) %>%
      column_spec(3:5) %>%
      collapse_rows(columns = 1, valign = "middle") %>%
      row_spec(0, bold = TRUE, color = "white", background = "orange") %>%
      row_spec(nrow(df_com_total), bold = TRUE, color = "white", background = "orange")
    
    HTML(tbl)
  })
  
}
