# AUGMECON-like approach in R using lpSolveAPI
# User defines both: list of products and list of supermarkets to consider

library(lpSolveAPI)
library(dplyr)
library(jsonlite)

optimization_model <- function(data_df, prod_selected, supermarkets_selected, quantities){

# Preparing the price matrix
supermarkets <- supermarkets_selected
products <- prod_selected
price_matrix <- matrix(NA, nrow = length(products), ncol = length(supermarkets),
                       dimnames = list(products, supermarkets))

for (p in products) {
  for (s in supermarkets) {
    subset_row <- subset(data_df, Produto == p & Supermarket == s)
    if (nrow(subset_row) > 0) {
      price_matrix[p, s] <- subset_row$Price
    }
  }
}

# Replacing the price of products not available by 1e6
BIG <- 1e6
na_rows <- apply(price_matrix, 1, function(x) all(is.na(x)))
if (any(na_rows)) {
  stop("Product(s) ",
       paste(products[which(na_rows)], collapse = ", "),
       " not available in the selected supermarkets.")
}
price_matrix[is.na(price_matrix)] <- BIG


# Function to check the product availability for each ε value (quantity of supermarkets to visit)
can_cover_all <- function(price_mat, epsilon, BIG = 1e6) {
  m <- ncol(price_mat)
  combs <- combn(m, epsilon, simplify = FALSE)
  
  for (comb in combs) {
    sub_mat <- price_mat[, comb, drop = FALSE]
    
    # For each product, check if at least one supermarket in this combo has a valid price
    products_available <- apply(sub_mat, 1, function(x) any(!is.na(x) & x < BIG))
    
    # For each supermarket in the combo, check if it sells at least one product
    supermarkets_useful <- apply(sub_mat, 2, function(x) any(!is.na(x) & x < BIG))
    
    # Valid combination: all products are covered AND all supermarkets are useful
    if (all(products_available) && all(supermarkets_useful)) {
      return(TRUE)
    }
  }
  
  return(FALSE)
}

### Preparing the results
## Buying all products at the same supermarket
# Multipying the price_matrix by the product quantity
qty_vector <- quantities[products]  
total_matrix <- price_matrix * qty_vector 

# Replacing the BIG values for NA
total_matrix[total_matrix >= BIG] <- NA

# Sum by supermarket
total_per_supermarket <- colSums(total_matrix,na.rm = FALSE)

# Results
cost_summary <- data.frame(
  Supermarket = names(total_per_supermarket),
  TotalCost = total_per_supermarket,
  row.names = NULL
)%>% 
  filter(!is.na(TotalCost))  # not considering supermarkets that has at least one product not available from the list

# Convert results to JSON
results_json <- toJSON(cost_summary, pretty = TRUE, dataframe = "rows", na = "null")

results <- list(
  total_per_supermarket = results_json,  
  optimization = list()                  
)



## Optimization analysis 
# Defining the interval of ε (the minimum value between the quantity of supermarkets to visit
# and the quantity of products in the basket
max_supermarkets <- min(length(supermarkets), length(products))
for (epsilon in 1:max_supermarkets) {
  
  #cat("\n=============================\n")
  #cat("For ",epsilon, "(ε) supermarket(s):\n")
  #cat("=============================\n")
  
  # Checking if the ε cover all products required
  if (!can_cover_all(price_matrix, epsilon)) {
    cat("There is no product combination available for ", epsilon,
        "supermarket(s).\n")
    next
  }
  
  # Creating the optimization model
  n <- nrow(price_matrix)
  m <- ncol(price_matrix)
  cols <- n * m + m
  lprec <- make.lp(0, cols)
  
  # Variables name
  colnames(lprec) <- c(paste0("x_", rep(products, each = m), "_", rep(supermarkets, times = n)),
                       paste0("y_", supermarkets))
  
  # Defining a binary type
  set.type(lprec, columns = 1:(cols), type = "binary")
  
  # Objective Function: minimize the total cost
  cost_vector <- c(as.vector(t(price_matrix * quantities)), rep(0, m))
  set.objfn(lprec, cost_vector)
  
  # Each product must be bought in one supermarket
  for (i in 1:n) {
    constr <- rep(0, cols)
    constr[((i - 1) * m + 1):(i * m)] <- 1
    add.constraint(lprec, constr, "=", 1)
  }
  
  # Restriction: A product can only be bought in a visited supermarket
  for (i in 1:n) {
    for (j in 1:m) {
      constr <- rep(0, cols)
      constr[(i - 1) * m + j] <- 1   # x_ij
      constr[n * m + j] <- -1        # -y_j
      add.constraint(lprec, constr, "<=", 0)
    }
  }
  
  # Restriction: If a supermarket is visited, at least one product must be bought there
  for (j in 1:m) {
    constr <- rep(0, cols)
    for (i in 1:n) {
      constr[(i - 1) * m + j] <- 1   # sum_i x_ij
    }
    constr[n * m + j] <- -1          # -y_j
    add.constraint(lprec, constr, ">=", 0)
  }
  
  # Limit the number of supermarkets visited
  constr <- rep(0, cols)
  constr[(n * m + 1):(cols)] <- 1
  add.constraint(lprec, constr, "=", epsilon)
  
  # Minimize the objective function
  lp.control(lprec, sense = "min")
  
  # Solving the optimization
  solve(lprec)
  
  # Result
  sol <- get.variables(lprec)
  x_vars <- matrix(sol[1:(n * m)], nrow = n, byrow = TRUE,
                   dimnames = list(products, supermarkets))
  y_vars <- sol[(n * m + 1):(cols)]
  
#  cat("Supermercados visitados:\n")
#  print(supermarkets[y_vars > 0.5])
  
#  cat("Produtos comprados em cada supermercado:\n")
#  print(x_vars)
  
#  cat("Custo total: ", get.objective(lprec), "\n")
  
  
  # Table with the results
  opt_table <- data.frame(
    Supermarket = character(),
    Product = character(),
    UnitPrice = numeric(),
    Quantity = numeric(),
    TotalCost = numeric(),
    stringsAsFactors = FALSE
  )
  
  for (i in 1:n) {
    for (j in 1:m) {
      if (x_vars[i, j] > 0.5) {
        opt_table <- rbind(opt_table, data.frame(
          Epsilon = epsilon,
          Supermarket = supermarkets[j],
          Product = products[i],
          UnitPrice = price_matrix[i, j],
          Quantity = quantities[products[i]],
          TotalCost = price_matrix[i, j] * quantities[products[i]],
          stringsAsFactors = FALSE,
          row.names = NULL
        ))
      }
    }
  }
  
  # Convert results to JSON
  opt_table_json <- toJSON(opt_table, pretty = TRUE, dataframe = "rows", na = "null")
  
  results$optimization[[as.character(epsilon)]] <- opt_table_json
  
  #print(opt_table)
  
}
  return(results)
}


##### Examples

# Input data 
data_df <- data.frame(
  Produto = c("Arroz","Arroz",
              "Feijão","Feijão","Feijão",
              "Macarrão", "Macarrão",
              "Leite","Leite",
              "Café","Café","Café",
              "Açúcar","Açúcar",
              "Farinha", "Farinha",
              "Óleo", "Óleo",
              "Sal","Sal","Sal",
              "Molho de Tomate"),
  Categoria = c("Grãos", "Grãos",
                "Grãos", "Grãos", "Grãos",
                "Massas", "Massas",
                "Laticínios","Laticínios",
                "Bebidas", "Bebidas", "Bebidas",
                "Doces", "Doces",
                "Grãos", "Grãos",
                "Óleos", "Óleos",
                "Temperos", "Temperos", "Temperos",
                "Enlatados"),
  Supermarket = c("Tenda", "Carrefour",
                  "Tenda", "Carrefour", "StMarche",
                  "Tenda", "Carrefour",
                  "StMarche", "Tenda",
                  "Carrefour", "StMarche", "Tenda",
                  "StMarche", "Carrefour",
                  "Tenda", "Carrefour",
                  "StMarche", "Carrefour",
                  "StMarche", "Carrefour", "Tenda",
                  "StMarche"),
  Price = c(2.5,3,
            4,5,4.5,
            7,5,
            6,4,
            4,10,9,
            8,7,
            6,5,
            5,6,
            10,8,9,
            10),
  stringsAsFactors = FALSE
)

### Example 1 - All products are available in all supermarkets
prod_selected1 <- c("Feijão", "Café", "Sal")
supermarkets_selected1 <- c("Tenda", "StMarche","Carrefour") 
quantities1 <- c(1, 1, 3)
names(quantities1) <- prod_selected1

optimization_model(data_df, prod_selected1, supermarkets_selected1, quantities1)

### Example 2 - Not all products are available in all supermarkets
prod_selected2 <- c("Feijão", "Café", "Molho de Tomate")
supermarkets_selected2 <- c("Tenda", "StMarche","Carrefour") 
quantities2 <- c(2, 1, 1)
names(quantities2) <- prod_selected2

optimization_model(data_df, prod_selected2, supermarkets_selected2, quantities2)

### Example 3 - Products are only available in two supermarkets
prod_selected3 <- c("Farinha", "Arroz", "Macarrão")
supermarkets_selected3 <- c("Tenda", "StMarche","Carrefour") 
quantities3 <- c(2, 4, 3)
names(quantities3) <- prod_selected3

optimization_model(data_df, prod_selected3, supermarkets_selected3, quantities3)

### Example 4 - Products not available in the selected supermarket
prod_selected4 <- c("Açúcar", "Molho de Tomate")
supermarkets_selected4 <- c("Tenda") 
quantities4 <- c(2, 4)
names(quantities4) <- prod_selected4

optimization_model(data_df, prod_selected4, supermarkets_selected4, quantities4)

### Example 5 - Less products than supermarkets
prod_selected5 <- c("Feijão", "Café")
supermarkets_selected5 <- c("Tenda", "StMarche","Carrefour") 
quantities5 <- c(2, 4)
names(quantities5) <- prod_selected5

optimization_model(data_df, prod_selected5, supermarkets_selected5, quantities5)