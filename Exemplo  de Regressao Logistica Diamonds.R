library(tidyverse)
library(tidymodels)
library(themis)

data("diamonds")



diamonds <- diamonds %>%
  mutate(
    id_chave = row_number(),
    cut = if_else(cut == "Ideal", "Ideal", "Outro"),
    cut = factor(cut)
  ) 


set.seed(123)
data_split <- initial_split(diamonds, strata = cut, prop = 0.8)

train_data <- training(data_split)
test_data <- testing(data_split)


recipe_diamonds <- recipe(cut ~ depth + price + table + x + id_chave, data = train_data) %>%
  update_role(id_chave, new_role = "id variable") %>%
  step_interact(~ starts_with("price"):starts_with("x")) %>% 
  step_normalize(all_predictors()) %>% 
  step_downsample(cut)

recipe_diamonds %>% 
  prep() %>% 
  juice()

set.seed(123)
folds <- vfold_cv(train_data, v = 5, strata = cut)


# Especificação do modelo
model_spec <- logistic_reg() %>%
  set_engine("glm")



# Workflow
workflow_diamonds <- workflow() %>%
  add_model(model_spec) %>%
  add_recipe(recipe_diamonds)

# 7. Ajustar o modelo usando fit_resamples

set.seed(123)
results <- workflow_diamonds %>%
  fit_resamples(
    resamples = folds,
    metrics = metric_set(roc_auc, accuracy),
    control = control_resamples(save_pred = TRUE)
  )


# Coletar predições das dobras de validação cruzada
cv_predictions <- collect_predictions(results)


cv_predictions

collect_metrics(results)


# 8. Ajustar o modelo final nos dados de treinamento completos
final_model <- workflow_diamonds %>%
  fit(data = train_data)

#Exibir os coeficientes do modelo ajustado
coeficientes <- final_model %>%
  extract_fit_parsnip() %>%
  tidy()

coeficientes


# Fazer predições no conjunto de teste
test_predictions <- predict(final_model, test_data, type = "prob") %>%
  bind_cols(test_data %>% select(cut, id_chave))


#Adicionar a classe predita
test_predictions <- test_predictions %>%
  mutate(.pred_class = if_else(.pred_Ideal >= 0.5, "Ideal", "Outro") %>% as.factor())


# Calcular as métricas
metrics <- test_predictions %>%
  roc_auc(truth = cut, .pred_Ideal) %>%
  bind_rows(
    test_predictions %>% accuracy(truth = cut, estimate = .pred_class)
  )

print(metrics)

# 11. Plotar a curva ROC no conjunto de teste


roc_data <- test_predictions %>%
  roc_curve(cut, .pred_Ideal)

autoplot(roc_data)

# 12. Salvar a tibble com as predições usando o id_chave

predicoes <- test_predictions %>%
  select(id_chave, cut, .pred_Ideal, .pred_class)

# Exemplo de visualização das predições
head(predicoes)

# 13. Demonstrar como realizar predições com o modelo treinado

# Novos dados de exemplo
novos_dados <- test_data %>%
  slice(1:5) %>%
  select(id_chave, depth, price, table, x)

# Predições nos novos dados
novas_predicoes <- predict(final_model, novos_dados, type = "prob") %>%
  bind_cols(novos_dados %>% select(id_chave))

print(novas_predicoes)


#### Pacote Scorecard -----
#install.packages("scorecard")
library(scorecard)

data("diamonds")

# Preparar os dados
diamonds <- diamonds %>%
  mutate(
    id_chave = row_number(),
    cut_bin = if_else(cut == "Ideal", 1, 0)
  ) %>% 
  mutate(cut_bin = as.factor(cut_bin))


set.seed(123)
data_split <- initial_split(diamonds, strata = cut_bin, prop = 0.8)

train_data <- training(data_split)
test_data <- testing(data_split)

# Realizar o binning das variáveis preditoras
bins <- woebin(train_data, y = "cut_bin", x = c("depth", "price", "table", "x"))

# Aplicar o binning aos dados de treinamento e teste
train_woe <- woebin_ply(train_data, bins) 
test_woe <- woebin_ply(test_data, bins)


# Obter os nomes das variáveis WOE
woe_vars <- names(train_woe)[str_detect(names(train_woe), "_woe$")]

# Criar o recipe
recipe_diamonds <- recipe(cut_bin ~ ., data = train_woe) %>%
  add_role(id_chave, new_role = "id variable") %>%
  step_select(all_outcomes(), id_chave, all_of(woe_vars)) %>%
  step_downsample(cut_bin) 


recipe_diamonds %>% 
  prep() %>% 
  juice() %>% 
  count(cut_bin)

# Especificação do modelo
model_spec <- logistic_reg() %>%
  set_engine("glm")

# Criar o workflow
workflow_diamonds <- workflow() %>%
  add_model(model_spec) %>%
  add_recipe(recipe_diamonds)

# Criar as dobras de validação cruzada
set.seed(123)
folds <- vfold_cv(train_woe, v = 5, strata = cut_bin)

unregister_dopar <- function() {
  env <- foreach:::.foreachGlobals
  rm(list=ls(name=env), pos=env)
}
unregister_dopar()

# Ajustar o modelo usando fit_resamples
set.seed(123)
results <- workflow_diamonds %>%
  fit_resamples(
    resamples = folds,
    metrics = metric_set(roc_auc, accuracy),
    control = control_resamples(save_pred = TRUE)
  )



# Verificar se o ajuste foi bem-sucedido
print(results)

# Ajustar o modelo final nos dados de treinamento completos
final_model <- workflow_diamonds %>%
  fit(data = train_woe)

# Exibir os coeficientes do modelo ajustado
coeficientes <- final_model %>%
  pull_workflow_fit() %>%
  tidy()

print(coeficientes)

# Fazer predições no conjunto de teste
test_predictions <- predict(final_model, test_woe, type = "prob") %>%
  bind_cols(
    predict(final_model, test_woe, type = "class") %>% rename(.pred_class = .pred_class)
  ) %>%
  bind_cols(test_woe %>% select(cut_bin, id_chave))

# Calcular as métricas
metrics <- test_predictions %>%
  roc_auc(truth = cut_bin, .pred_1) %>%
  bind_rows(
    test_predictions %>% accuracy(truth = cut_bin, estimate = .pred_class)
  )

print(metrics)

# Plotar a curva ROC
roc_data <- test_predictions %>%
  roc_curve(truth = cut_bin, .pred_1)

autoplot(roc_data)

# Salvar a tibble com as predições
predicoes <- test_predictions %>%
  select(id_chave, cut_bin, .pred_1, .pred_class)

head(predicoes)

# Aplicar o modelo a novos dados
novos_dados <- test_data %>%
  slice(1:5) %>%
  select(id_chave, depth, price, table, x)

novos_dados_woe <- woebin_ply(novos_dados, bins)

novas_predicoes <- predict(final_model, novos_dados_woe, type = "prob") %>%
  bind_cols(
    predict(final_model, novos_dados_woe, type = "class") %>% rename(.pred_class = .pred_class)
  ) %>%
  bind_cols(novos_dados_woe %>% select(id_chave))

print(novas_predicoes)

