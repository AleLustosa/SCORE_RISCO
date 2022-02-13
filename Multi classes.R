
# Uyilizado para corrigir problema ao baixar base do Big Query ----
options(scipen = 20)


library(writexl)

library(data.table)

library(tidymodels)

library(tune)


library(workflows)

library(dials)

library(themis)

library(tidytext)

library(skimr)

library(bigrquery) # R Interface to Google BigQuery API  
library(dplyr) # Grammar for data manipulation  


library(DBI) # Interface definition to connect to databases 


library(MatchIt)

#install.packages("timetk")
library(abjutils)
library(ggraph)
library(ggridges)
library(GGally)
library(vip)
library(DataExplorer)

library(tidytext)
library(tidylo)
library(tidyquant)
library(lubridate)
library(widyr)
library(timetk)

library(tidyverse)


# Conecta com o BigQuery
# Projeto diferente
projectid_prd <-'self-service-saude-prd'
datasetid<-'SBX_NUCLEO_INDICADORES'
bq_conn_prd <-  dbConnect(bigquery(),
                          project = projectid_prd,
                          dataset = datasetid,
                          use_legacy_sql = FALSE
                          
)

projectid <-'self-service-saude'
datasetid <-'SBX_NUCLEO_INDICADORES'
bq_conn   <-  dbConnect(bigquery(),
                        project = projectid,
                        dataset = datasetid,
                        use_legacy_sql = FALSE
                        
)


# CARREGA TABELA INTERNACOES BQ ----
fat_inter <- dplyr::tbl(bq_conn,
                        "TRU_SAUDE_CONTROLE_PRECO_SINISTRO_PRD.TB_FAT_INTERNACAO")



# Tabela de Sinistro Mensal por Carteirinha
tabela_cadastro <- dplyr::tbl(bq_conn_prd,
                              "REF_SAUDE_BENEFICIARIO.TB_CAD_BENEFICIARIO")


tabela_aberta  <-dplyr::tbl(bq_conn,
                            "TRU_SAUDE_CONTROLE_PRECO_SINISTRO_PRD.TB_FAT_ITEM_SERVICO_SINISTRO")


fat_nao_inter <- dplyr::tbl(bq_conn,
                            "TRU_SAUDE_CONTROLE_PRECO_SINISTRO_PRD.TB_FAT_NAO_INTERNACAO")


# Tabela de Sinistro Mensal por Carteirinha - BIGQUERY
tabela_apoio_sinistro <- dplyr::tbl(bq_conn,
                                    "SBX_NUCLEO_INDICADORES.TB_ESTUDO_TMP2")





base_pct_acumlado_bq <- tabela_aberta %>% 
  select(COD_ANALITICO_BENEFICIARIO, DAT_ATENDIMENTO_EFETIVO_SINISTRO, VLR_PAGO_SERVICO) %>% 
  filter(DAT_ATENDIMENTO_EFETIVO_SINISTRO >= "2020-01-01" ) %>% 
  mutate(ano = year(DAT_ATENDIMENTO_EFETIVO_SINISTRO),
         mes = month(DAT_ATENDIMENTO_EFETIVO_SINISTRO)) %>% 
  ungroup() %>% 
  filter(ano == 2021 & mes == 3) %>% 
  group_by(COD_ANALITICO_BENEFICIARIO) %>% 
  summarise(gasto = sum(VLR_PAGO_SERVICO, na.rm = TRUE)) %>% 
  ungroup() %>% 
  filter(gasto >0) %>% 
  arrange(desc(gasto)) %>% 
  #group_by(COD_ANALITICO_BENEFICIARIO) %>% 
  mutate(pct = gasto / sum(gasto)) %>% 
  ungroup() %>% 
  mutate(acumulado = cumsum(pct))



# Baixa base de marc0/20 pct ----
pct_acum_tbl <- base_pct_acumlado_bq %>% 
  collect()




# pct_acum_tbl %>% 
#   mutate(target = case_when(
#     acumulado <= 0.75 ~ 1,
#     acumulado <= 0.9 ~ 2,
#     TRUE ~ 3
#   )) %>% 
#   group_by(target) %>% 
#   summarise(gasto = mean(gasto),
#             qtd = n()) %>% 
#   ungroup() %>% 
#   mutate(qtd_benef = qtd / sum(qtd))



# Busca acumulado D - 1 ----



base_pct_acumlado_D1_bq <- tabela_aberta %>% 
  select(COD_ANALITICO_BENEFICIARIO, DAT_ATENDIMENTO_EFETIVO_SINISTRO, VLR_PAGO_SERVICO) %>% 
  filter(DAT_ATENDIMENTO_EFETIVO_SINISTRO >= "2020-01-01" ) %>% 
  mutate(ano = year(DAT_ATENDIMENTO_EFETIVO_SINISTRO),
         mes = month(DAT_ATENDIMENTO_EFETIVO_SINISTRO)) %>% 
  ungroup() %>% 
  filter(ano == 2021 & mes == 2) %>% 
  group_by(COD_ANALITICO_BENEFICIARIO) %>% 
  summarise(gasto = sum(VLR_PAGO_SERVICO, na.rm = TRUE)) %>% 
  ungroup() %>% 
  filter(gasto >0) %>% 
  arrange(desc(gasto)) %>% 
  #group_by(COD_ANALITICO_BENEFICIARIO) %>% 
  mutate(pct = gasto / sum(gasto)) %>% 
  ungroup() %>% 
  mutate(acumulado = cumsum(pct))



# Baixa base de marc0/20 pct ----
pct_acum_D1_tbl <- base_pct_acumlado_D1_bq  %>% 
  collect()


# Busca acumulado D - 6 ----


base_pct_acumlado_D6_bq <- tabela_aberta %>% 
  select(COD_ANALITICO_BENEFICIARIO, DAT_ATENDIMENTO_EFETIVO_SINISTRO, VLR_PAGO_SERVICO) %>% 
  filter(DAT_ATENDIMENTO_EFETIVO_SINISTRO >= "2020-01-01" ) %>% 
  mutate(ano = year(DAT_ATENDIMENTO_EFETIVO_SINISTRO),
         mes = month(DAT_ATENDIMENTO_EFETIVO_SINISTRO)) %>% 
  ungroup() %>% 
  mutate(ano = year(DAT_ATENDIMENTO_EFETIVO_SINISTRO),
         mes = month(DAT_ATENDIMENTO_EFETIVO_SINISTRO)) %>% 
  ungroup() %>% 
  mutate(tempo = case_when(
    (ano == 2021 & mes %in% c(1,2,3)) | (ano == 2020 & mes %in% c(10,11,12)) ~ "janela_semestre",
    TRUE ~  "nulo"
  )) %>% 
  filter(tempo == "janela_semestre") %>%  
  group_by(COD_ANALITICO_BENEFICIARIO) %>% 
  summarise(gasto = sum(VLR_PAGO_SERVICO, na.rm = TRUE)) %>% 
  ungroup() %>% 
  filter(gasto >0) %>% 
  arrange(desc(gasto)) %>% 
  #group_by(COD_ANALITICO_BENEFICIARIO) %>% 
  mutate(pct = gasto / sum(gasto)) %>% 
  ungroup() %>% 
  mutate(acumulado = cumsum(pct))


pct_acum_D6_tbl <- base_pct_acumlado_D6_bq  %>% 
  collect()



# Podenramento ----
#rename_with(.cols = 2:4, .fn = ~ str_c("d0_", .))
# df_ponderamento <- pct_acum_tbl %>% 
#   rename_with(.cols = 2:4, .fn = ~ str_c("d0_", .)) %>% 
#   left_join(pct_acum_D1_tbl ) %>% 
#   rename_with(.cols = 5:7, .fn = ~ str_c("d1_", .)) %>% 
#   
#   left_join(pct_acum_D6_tbl ) %>% 
#   rename_with(.cols = 8:10, .fn = ~ str_c("d6_", .))


df_ponderamento <- pct_acum_tbl %>% 
  rename_with(.cols = 2:4, .fn = ~ str_c("d0_", .)) %>% 
  full_join(pct_acum_D1_tbl ) %>% 
  rename_with(.cols = 5:7, .fn = ~ str_c("d1_", .)) %>% 
  
  full_join(pct_acum_D6_tbl ) %>% 
  rename_with(.cols = 8:10, .fn = ~ str_c("d6_", .))


df_ponderamento %>% 
  skim()
  


# df_pond_a <- df_ponderamento %>% 
#   mutate(d1_gasto = if_else(is.na(d1_gasto), 0, d1_gasto)) %>% 
#   arrange(desc(d1_gasto)) %>% 
#   mutate(d1_pct = d1_gasto / sum(d1_gasto)) %>% 
#   mutate(d1_acumulado = cumsum(d1_pct)) %>% 
#   
#   arrange(d0_acumulado, d1_acumulado, d6_acumulado)


df_pond_a <- df_ponderamento %>% 
  
  mutate(d0_gasto = if_else(is.na(d0_gasto), 0, d0_gasto)) %>% 
  arrange(desc(d0_gasto)) %>% 
  mutate(d0_pct = (d0_gasto + 0.00001) / sum(d0_gasto)) %>% 
  mutate(d0_acumulado = cumsum(d0_pct)) %>% 
  
  mutate(d1_gasto = if_else(is.na(d1_gasto), 0, d1_gasto)) %>% 
  arrange(desc(d1_gasto)) %>% 
  mutate(d1_pct = (d1_gasto + 0.00001) / sum(d1_gasto)) %>% 
  mutate(d1_acumulado = cumsum(d1_pct)) %>%
  
  mutate(d6_gasto = if_else(is.na(d6_gasto), 0, d6_gasto)) %>% 
  arrange(desc(d6_gasto)) %>% 
  mutate(d6_pct = (d6_gasto + 0.00001) / sum(d6_gasto)) %>% 
  mutate(d6_acumulado = cumsum(d6_pct)) %>%
  
  
  arrange(d0_acumulado, d1_acumulado, d6_acumulado)


df_pond_a


df_pond_b <- df_pond_a %>% 
  mutate(rank_int = (d0_acumulado * 0.6) + (d1_acumulado * 0.25) + (d6_acumulado * 0.15) ) %>% 
  
  arrange(rank_int) %>% 
  
  mutate(target = case_when(
    rank_int <= 0.75 ~ 1,
    rank_int <= 0.9 ~ 2,
    TRUE ~ 3
  )) 



#df_pond_b %>% write_csv("bases intermediarias/df_pond_b.csv")


# Carrega base ponderada ----
df_pond_b <- read_csv("bases intermediarias/df_pond_b.csv")

df_pond_b %>% 
  group_by(target) %>% 
  summarise(gasto = mean( d0_gasto),
            qtd = n()) %>% 
  ungroup() %>% 
  mutate(qtd_benef = qtd / sum(qtd))


df_pond_b %>% 
  filter(target == 1) %>% 
  arrange(d0_gasto)
  
  


# Cortes de bases ----

alvo_1 <- df_pond_b  %>% 
  filter(target == 1) %>% 
  select(COD_ANALITICO_BENEFICIARIO) %>% 
  mutate(tipo = "alto_risco")


DBI::dbWriteTable( bq_conn, "TB_RISCO_ALTO_TEMP", alvo_1, overwrite = TRUE)


alvo_2 <- df_pond_b %>% 
  filter(target == 2) %>% 
  select(COD_ANALITICO_BENEFICIARIO) %>% 
  mutate(tipo = "medio_risco")


DBI::dbWriteTable( bq_conn, "TB_RISCO_MEDIO_TEMP", alvo_2, overwrite = TRUE)




alvo_3 <- df_pond_b %>% 
  filter(target == 3) %>% 
  select(COD_ANALITICO_BENEFICIARIO) %>% 
  mutate(tipo = "baixo_risco")


DBI::dbWriteTable( bq_conn, "TB_RISCO_BAIXO_TEMP", alvo_3, overwrite = TRUE)


bq_alto_risco <- dplyr::tbl(bq_conn,
                      "SBX_NUCLEO_INDICADORES.TB_RISCO_ALTO_TEMP")


bq_medio_risco <- dplyr::tbl(bq_conn,
                            "SBX_NUCLEO_INDICADORES.TB_RISCO_MEDIO_TEMP")

bq_baixo_risco <- dplyr::tbl(bq_conn,
                             "SBX_NUCLEO_INDICADORES.TB_RISCO_BAIXO_TEMP")



# Baixa tabela de Cadastro ----
tabela_full_cadastro <- tabela_cadastro %>%
  filter(DAT_INICIO_VIGENCIA <= "2022-01-01") %>%
  filter(DAT_FIM_VIGENCIA>= "2020-06-01") %>%
  select(COD_ANALITICO_BENEFICIARIO, DAT_INICIO_VIGENCIA, DAT_FIM_VIGENCIA, QTD_IDADE, DAT_NASCIMENTO, SIG_UF_RESIDENCIA, DSC_PADRAO_PLANO, DSC_CARTEIRA) %>%
  collect()



#tabela_full_cadastro %>% write_csv("bases intermediarias/tabela_full_cadastro.csv")


tabela_full_cadastro <- read_csv("bases intermediarias/tabela_full_cadastro.csv")


# Montagem base alto risco -----


alto_risco_marco_21 <- tabela_aberta %>% 
  filter(DAT_ATENDIMENTO_EFETIVO_SINISTRO >= "2019-01-01") %>% 
  inner_join(bq_alto_risco, by = "COD_ANALITICO_BENEFICIARIO") %>% 
  mutate(ano = year(DAT_ATENDIMENTO_EFETIVO_SINISTRO),
         mes = month(DAT_ATENDIMENTO_EFETIVO_SINISTRO)) %>% 
  ungroup() %>% 
  filter(ano == 2021 & mes == 3) %>% 
  group_by(COD_ANALITICO_BENEFICIARIO, ano, mes, FLG_PS, FLG_INTERNACAO) %>% 
  summarise(gasto = sum(VLR_PAGO_SERVICO, na.rm = TRUE)) %>% 
  ungroup() %>% 
  collect()


alto_risco_fev_21 <- tabela_aberta %>% 
  filter(DAT_ATENDIMENTO_EFETIVO_SINISTRO >= "2019-01-01") %>% 
  inner_join(bq_alto_risco, by = "COD_ANALITICO_BENEFICIARIO") %>% 
  mutate(ano = year(DAT_ATENDIMENTO_EFETIVO_SINISTRO),
         mes = month(DAT_ATENDIMENTO_EFETIVO_SINISTRO)) %>% 
  ungroup() %>% 
  filter(ano == 2021 & mes == 2) %>% 
  group_by(COD_ANALITICO_BENEFICIARIO, ano, mes, FLG_PS, FLG_INTERNACAO) %>% 
  summarise(gasto = sum(VLR_PAGO_SERVICO, na.rm = TRUE)) %>% 
  ungroup() %>% 
  collect()



mes_0_alto_risco_sinistro <- alto_risco_marco_21 %>% 
  mutate(tempo = "d0") %>% 
  select(-ano, -mes) %>% 
  select(COD_ANALITICO_BENEFICIARIO, tempo, everything()) %>% 
  pivot_wider(names_from = tempo:FLG_INTERNACAO, values_from = gasto, values_fill = 0)


mes_1_alto_risco_sinistro <- alto_risco_fev_21 %>% 
  mutate(tempo = "d1") %>% 
  select(-ano, -mes) %>% 
  select(COD_ANALITICO_BENEFICIARIO, tempo, everything()) %>% 
  pivot_wider(names_from = tempo:FLG_INTERNACAO, values_from = gasto, values_fill = 0)


# Sumariza alvo por semestre ----

semestre_qtd_alto_risco <- fat_nao_inter %>% 
  inner_join(bq_alto_risco, by = "COD_ANALITICO_BENEFICIARIO") %>% 
  mutate(ano = year(DAT_ATENDIMENTO_EFETIVO_SINISTRO),
         mes = month(DAT_ATENDIMENTO_EFETIVO_SINISTRO)) %>% 
  ungroup() %>% 
  mutate(tempo = case_when(
    (ano == 2021 & mes %in% c(1,2,3)) | (ano == 2020 & mes %in% c(10,11,12)) ~ "janela_semestre",
    TRUE ~  "nulo"
  )) %>% 
  filter(tempo == "janela_semestre") %>% 
  mutate(onco = if_else(FLG_ONCOLOGIA == "S", 1,0)) %>% 
  mutate(dialise = if_else(FLG_TERAPIA_DIALISE == "S", 1,0)) %>%
  mutate(hemo = if_else(FLG_TERAPIA_HEMODIALISE == "S", 1,0)) %>%
  mutate(quimio = if_else(FLG_TERAPIA_QUIMIOTERAPIA == "S", 1,0)) %>%
  mutate(radio = if_else(FLG_TERAPIA_RADIOTERAPIA == "S", 1,0)) %>%
  mutate(adomici = if_else(FLG_ATENDIMENTO_DOMICILIAR == "S", 1,0)) %>%
  mutate(ps = if_else(FLG_PS == "S", 1,0)) %>%
  group_by(COD_ANALITICO_BENEFICIARIO) %>% 
  summarise(onco = sum(onco),
            dialise = sum(dialise),
            hemodialise = sum(hemo),
            quimio = sum(quimio),
            radio = sum(radio),
            atdom = sum(adomici),
            qtd_ps = sum(ps),
            qtd_consulta = sum(QTD_CONSULTA)) %>% 
  ungroup() %>% 
  collect()

semestre_qtd_alto_risco <- semestre_qtd_alto_risco %>% 
  rename_with(.cols = 2:9, .fn = ~ str_c("d6_", .))


# Valor gasto total semestre
total_semestre_alto_risco <- tabela_aberta %>% 
  inner_join(bq_alto_risco, by = "COD_ANALITICO_BENEFICIARIO") %>% 
  mutate(ano = year(DAT_ATENDIMENTO_EFETIVO_SINISTRO),
         mes = month(DAT_ATENDIMENTO_EFETIVO_SINISTRO)) %>% 
  ungroup() %>% 
  mutate(tempo = case_when(
    (ano == 2021 & mes %in% c(1,2,3)) | (ano == 2020 & mes %in% c(10,11,12)) ~ "janela_semestre",
    TRUE ~  "nulo"
  )) %>% 
  filter(tempo == "janela_semestre") %>% 
  group_by(COD_ANALITICO_BENEFICIARIO, FLG_PS, FLG_INTERNACAO) %>% 
  summarise(gasto = sum(VLR_PAGO_SERVICO)) %>% 
  ungroup() %>% 
  collect()



total_semestre_alto_risco <- total_semestre_alto_risco %>% 
  pivot_wider(names_from = FLG_PS:FLG_INTERNACAO, values_from = gasto, values_fill = 0) %>% 
  rename_with(.cols = 2:4, .fn = ~ str_c("d6_gasto", .))



#  
df_passo_c_alto_risco <- mes_0_alto_risco_sinistro %>%
  full_join(mes_1_alto_risco_sinistro) %>% 
  full_join(semestre_qtd_alto_risco) %>% 
  full_join(total_semestre_alto_risco ) %>% 
  
  mutate(across(starts_with("d"), .fns = ~ ifelse(is.na(.), 0, .)))



df_alto_risco_full <- df_passo_c_alto_risco %>% 
  left_join(tabela_full_cadastro %>% select(COD_ANALITICO_BENEFICIARIO, DAT_INICIO_VIGENCIA, DAT_FIM_VIGENCIA, QTD_IDADE, SIG_UF_RESIDENCIA, DSC_PADRAO_PLANO, DSC_CARTEIRA, DAT_NASCIMENTO) ) %>% 
  mutate(tempo_SAS = interval(start = ymd(DAT_INICIO_VIGENCIA),end = ymd(date("2021-03-01"))) / days(1)) %>% 
  add_column(target = "alto_risco")

# 61 benef sem datas 
df_alto_risco_full <- df_alto_risco_full %>% 
  mutate(QTD_IDADE = as.double(QTD_IDADE)) %>% 
  mutate(QTD_IDADE = if_else(is.na(QTD_IDADE), 47, QTD_IDADE)) %>% 
  mutate(tempo_SAS = if_else(is.na(tempo_SAS), 1200, tempo_SAS))



# df_alto_risco_full %>% 
#   skim()






# Montagem base medio risco -----


medio_risco_marco_21 <- tabela_aberta %>% 
  filter(DAT_ATENDIMENTO_EFETIVO_SINISTRO >= "2019-01-01") %>% 
  inner_join(bq_medio_risco, by = "COD_ANALITICO_BENEFICIARIO") %>% 
  mutate(ano = year(DAT_ATENDIMENTO_EFETIVO_SINISTRO),
         mes = month(DAT_ATENDIMENTO_EFETIVO_SINISTRO)) %>% 
  ungroup() %>% 
  filter(ano == 2021 & mes == 3) %>% 
  group_by(COD_ANALITICO_BENEFICIARIO, ano, mes, FLG_PS, FLG_INTERNACAO) %>% 
  summarise(gasto = sum(VLR_PAGO_SERVICO, na.rm = TRUE)) %>% 
  ungroup() %>% 
  collect()


medio_risco_fev_21 <- tabela_aberta %>% 
  filter(DAT_ATENDIMENTO_EFETIVO_SINISTRO >= "2019-01-01") %>% 
  inner_join(bq_medio_risco, by = "COD_ANALITICO_BENEFICIARIO") %>% 
  mutate(ano = year(DAT_ATENDIMENTO_EFETIVO_SINISTRO),
         mes = month(DAT_ATENDIMENTO_EFETIVO_SINISTRO)) %>% 
  ungroup() %>% 
  filter(ano == 2021 & mes == 2) %>% 
  group_by(COD_ANALITICO_BENEFICIARIO, ano, mes, FLG_PS, FLG_INTERNACAO) %>% 
  summarise(gasto = sum(VLR_PAGO_SERVICO, na.rm = TRUE)) %>% 
  ungroup() %>% 
  collect()


mes_0_medio_risco_sinistro <- medio_risco_marco_21 %>% 
  mutate(tempo = "d0") %>% 
  select(-ano, -mes) %>% 
  select(COD_ANALITICO_BENEFICIARIO, tempo, everything()) %>% 
  pivot_wider(names_from = tempo:FLG_INTERNACAO, values_from = gasto, values_fill = 0)


mes_1_medio_risco_sinistro <- medio_risco_fev_21 %>% 
  mutate(tempo = "d1") %>% 
  select(-ano, -mes) %>% 
  select(COD_ANALITICO_BENEFICIARIO, tempo, everything()) %>% 
  pivot_wider(names_from = tempo:FLG_INTERNACAO, values_from = gasto, values_fill = 0)





# Sumariza alvo por semestre ----

semestre_qtd_medio_risco <- fat_nao_inter %>% 
  inner_join(bq_medio_risco, by = "COD_ANALITICO_BENEFICIARIO") %>% 
  mutate(ano = year(DAT_ATENDIMENTO_EFETIVO_SINISTRO),
         mes = month(DAT_ATENDIMENTO_EFETIVO_SINISTRO)) %>% 
  ungroup() %>% 
  mutate(tempo = case_when(
    (ano == 2021 & mes %in% c(1,2,3)) | (ano == 2020 & mes %in% c(10,11,12)) ~ "janela_semestre",
    TRUE ~  "nulo"
  )) %>% 
  filter(tempo == "janela_semestre") %>% 
  mutate(onco = if_else(FLG_ONCOLOGIA == "S", 1,0)) %>% 
  mutate(dialise = if_else(FLG_TERAPIA_DIALISE == "S", 1,0)) %>%
  mutate(hemo = if_else(FLG_TERAPIA_HEMODIALISE == "S", 1,0)) %>%
  mutate(quimio = if_else(FLG_TERAPIA_QUIMIOTERAPIA == "S", 1,0)) %>%
  mutate(radio = if_else(FLG_TERAPIA_RADIOTERAPIA == "S", 1,0)) %>%
  mutate(adomici = if_else(FLG_ATENDIMENTO_DOMICILIAR == "S", 1,0)) %>%
  mutate(ps = if_else(FLG_PS == "S", 1,0)) %>%
  group_by(COD_ANALITICO_BENEFICIARIO) %>% 
  summarise(onco = sum(onco),
            dialise = sum(dialise),
            hemodialise = sum(hemo),
            quimio = sum(quimio),
            radio = sum(radio),
            atdom = sum(adomici),
            qtd_ps = sum(ps),
            qtd_consulta = sum(QTD_CONSULTA)) %>% 
  ungroup() %>% 
  collect()

semestre_qtd_medio_risco <- semestre_qtd_medio_risco %>% 
  rename_with(.cols = 2:9, .fn = ~ str_c("d6_", .))




# Valor gasto total semestre
total_semestre_medio_risco <- tabela_aberta %>% 
  inner_join(bq_medio_risco, by = "COD_ANALITICO_BENEFICIARIO") %>% 
  mutate(ano = year(DAT_ATENDIMENTO_EFETIVO_SINISTRO),
         mes = month(DAT_ATENDIMENTO_EFETIVO_SINISTRO)) %>% 
  ungroup() %>% 
  mutate(tempo = case_when(
    (ano == 2021 & mes %in% c(1,2,3)) | (ano == 2020 & mes %in% c(10,11,12)) ~ "janela_semestre",
    TRUE ~  "nulo"
  )) %>% 
  filter(tempo == "janela_semestre") %>% 
  group_by(COD_ANALITICO_BENEFICIARIO, FLG_PS, FLG_INTERNACAO) %>% 
  summarise(gasto = sum(VLR_PAGO_SERVICO)) %>% 
  ungroup() %>% 
  collect()



total_semestre_medio_risco <- total_semestre_medio_risco %>% 
  pivot_wider(names_from = FLG_PS:FLG_INTERNACAO, values_from = gasto, values_fill = 0) %>% 
  rename_with(.cols = 2:4, .fn = ~ str_c("d6_gasto", .))



df_passo_c_medio_risco <- mes_0_medio_risco_sinistro %>%
  full_join(mes_1_medio_risco_sinistro) %>% 
  full_join(semestre_qtd_medio_risco) %>% 
  full_join(total_semestre_medio_risco ) %>% 
  
  mutate(across(starts_with("d"), .fns = ~ ifelse(is.na(.), 0, .)))




df_medio_risco_full <- df_passo_c_medio_risco %>% 
  left_join(tabela_full_cadastro %>% select(COD_ANALITICO_BENEFICIARIO, DAT_INICIO_VIGENCIA, DAT_FIM_VIGENCIA, QTD_IDADE, SIG_UF_RESIDENCIA, DSC_PADRAO_PLANO, DSC_CARTEIRA, DAT_NASCIMENTO) ) %>% 
  mutate(tempo_SAS = interval(start = ymd(DAT_INICIO_VIGENCIA),end = ymd(date("2021-03-01"))) / days(1)) %>% 
  add_column(target = "medio_risco")



#df_medio_risco_full %>% skim()

df_medio_risco_full %>% 
  select(QTD_IDADE, tempo_SAS) %>% 
  summary()

# 61 benef sem datas 
df_medio_risco_full<- df_medio_risco_full %>% 
  mutate(QTD_IDADE = as.double(QTD_IDADE)) %>% 
  mutate(QTD_IDADE = if_else(is.na(QTD_IDADE), 44, QTD_IDADE)) %>% 
  mutate(tempo_SAS = if_else(is.na(tempo_SAS), 1100, tempo_SAS))



# Montagem base baixo risco -----



baixo_risco_marco_21 <- tabela_aberta %>% 
  filter(DAT_ATENDIMENTO_EFETIVO_SINISTRO >= "2019-01-01") %>% 
  inner_join(bq_baixo_risco, by = "COD_ANALITICO_BENEFICIARIO") %>% 
  mutate(ano = year(DAT_ATENDIMENTO_EFETIVO_SINISTRO),
         mes = month(DAT_ATENDIMENTO_EFETIVO_SINISTRO)) %>% 
  ungroup() %>% 
  filter(ano == 2021 & mes == 3) %>% 
  group_by(COD_ANALITICO_BENEFICIARIO, ano, mes, FLG_PS, FLG_INTERNACAO) %>% 
  summarise(gasto = sum(VLR_PAGO_SERVICO, na.rm = TRUE)) %>% 
  ungroup() %>% 
  collect()


baixo_risco_fev_21 <- tabela_aberta %>% 
  filter(DAT_ATENDIMENTO_EFETIVO_SINISTRO >= "2019-01-01") %>% 
  inner_join(bq_baixo_risco, by = "COD_ANALITICO_BENEFICIARIO") %>% 
  mutate(ano = year(DAT_ATENDIMENTO_EFETIVO_SINISTRO),
         mes = month(DAT_ATENDIMENTO_EFETIVO_SINISTRO)) %>% 
  ungroup() %>% 
  filter(ano == 2021 & mes == 2) %>% 
  group_by(COD_ANALITICO_BENEFICIARIO, ano, mes, FLG_PS, FLG_INTERNACAO) %>% 
  summarise(gasto = sum(VLR_PAGO_SERVICO, na.rm = TRUE)) %>% 
  ungroup() %>% 
  collect()


mes_0_baixo_risco_sinistro <- baixo_risco_marco_21 %>% 
  mutate(tempo = "d0") %>% 
  select(-ano, -mes) %>% 
  select(COD_ANALITICO_BENEFICIARIO, tempo, everything()) %>% 
  pivot_wider(names_from = tempo:FLG_INTERNACAO, values_from = gasto, values_fill = 0)


mes_1_baixo_risco_sinistro <- baixo_risco_fev_21 %>% 
  mutate(tempo = "d1") %>% 
  select(-ano, -mes) %>% 
  select(COD_ANALITICO_BENEFICIARIO, tempo, everything()) %>% 
  pivot_wider(names_from = tempo:FLG_INTERNACAO, values_from = gasto, values_fill = 0)





# Sumariza alvo por semestre ----

semestre_qtd_baixo_risco <- fat_nao_inter %>% 
  inner_join(bq_baixo_risco, by = "COD_ANALITICO_BENEFICIARIO") %>% 
  mutate(ano = year(DAT_ATENDIMENTO_EFETIVO_SINISTRO),
         mes = month(DAT_ATENDIMENTO_EFETIVO_SINISTRO)) %>% 
  ungroup() %>% 
  mutate(tempo = case_when(
    (ano == 2021 & mes %in% c(1,2,3)) | (ano == 2020 & mes %in% c(10,11,12)) ~ "janela_semestre",
    TRUE ~  "nulo"
  )) %>% 
  filter(tempo == "janela_semestre") %>% 
  mutate(onco = if_else(FLG_ONCOLOGIA == "S", 1,0)) %>% 
  mutate(dialise = if_else(FLG_TERAPIA_DIALISE == "S", 1,0)) %>%
  mutate(hemo = if_else(FLG_TERAPIA_HEMODIALISE == "S", 1,0)) %>%
  mutate(quimio = if_else(FLG_TERAPIA_QUIMIOTERAPIA == "S", 1,0)) %>%
  mutate(radio = if_else(FLG_TERAPIA_RADIOTERAPIA == "S", 1,0)) %>%
  mutate(adomici = if_else(FLG_ATENDIMENTO_DOMICILIAR == "S", 1,0)) %>%
  mutate(ps = if_else(FLG_PS == "S", 1,0)) %>%
  group_by(COD_ANALITICO_BENEFICIARIO) %>% 
  summarise(onco = sum(onco),
            dialise = sum(dialise),
            hemodialise = sum(hemo),
            quimio = sum(quimio),
            radio = sum(radio),
            atdom = sum(adomici),
            qtd_ps = sum(ps),
            qtd_consulta = sum(QTD_CONSULTA)) %>% 
  ungroup() %>% 
  collect()

semestre_qtd_baixo_risco <- semestre_qtd_baixo_risco %>% 
  rename_with(.cols = 2:9, .fn = ~ str_c("d6_", .))







# Valor gasto total semestre
total_semestre_baixo_risco <- tabela_aberta %>% 
  inner_join(bq_baixo_risco, by = "COD_ANALITICO_BENEFICIARIO") %>% 
  mutate(ano = year(DAT_ATENDIMENTO_EFETIVO_SINISTRO),
         mes = month(DAT_ATENDIMENTO_EFETIVO_SINISTRO)) %>% 
  ungroup() %>% 
  mutate(tempo = case_when(
    (ano == 2021 & mes %in% c(1,2,3)) | (ano == 2020 & mes %in% c(10,11,12)) ~ "janela_semestre",
    TRUE ~  "nulo"
  )) %>% 
  filter(tempo == "janela_semestre") %>% 
  group_by(COD_ANALITICO_BENEFICIARIO, FLG_PS, FLG_INTERNACAO) %>% 
  summarise(gasto = sum(VLR_PAGO_SERVICO)) %>% 
  ungroup() %>% 
  collect()



total_semestre_baixo_risco <- total_semestre_baixo_risco %>% 
  pivot_wider(names_from = FLG_PS:FLG_INTERNACAO, values_from = gasto, values_fill = 0) %>% 
  rename_with(.cols = 2:4, .fn = ~ str_c("d6_gasto", .))







# Dados benef medio risco
cad_baixo_risco <- tabela_full_cadastro %>% 
  inner_join(mes_0_baixo_risco_sinistro %>% select(COD_ANALITICO_BENEFICIARIO)) %>% 
  distinct()


cad_baixo_risco <- cad_baixo_risco%>% 
  mutate(tempo_SAS = interval(start = ymd(DAT_INICIO_VIGENCIA_PLANO),end = ymd(date("2021-03-01"))) / days(1)) 



df_baixo_risco_full <- mes_0_baixo_risco_sinistro %>% 
  left_join(mes_1_baixo_risco_sinistro) %>% 
  left_join(semestre_qtd_baixo_risco) %>% 
  left_join(total_semestre_baixo_risco ) %>% 
  left_join(cad_baixo_risco) %>% 
  
  filter(!is.na(tempo_SAS))


df_baixo_risco_full <- df_baixo_risco_full %>% 
  mutate(across(starts_with("d"), .fns = ~ ifelse(is.na(.), 0, .)))


df_baixo_risco_full <- df_baixo_risco_full %>% 
  add_column(target = "baixo_risco")



df_blended_full <- df_alto_risco_full %>% 
  bind_rows(df_medio_risco_full) %>% 
  bind_rows(df_baixo_risco_full)

#df_blended_full %>% write_csv("bases intermediarias/df_blended_full_mult_class.csv")

# ML ----
df_blended_full <- read_csv("bases intermediarias/df_blended_full_mult_class.csv")


df_blended_full <- df_blended_full %>% 
  mutate(target = factor(target)) %>% 
  mutate(DSC_PADRAO_PLANO = if_else(DSC_PADRAO_PLANO == "NÃO INFORMADO", "ESPECIAL", DSC_PADRAO_PLANO)) %>% 
  mutate(DSC_CARTEIRA = if_else(DSC_CARTEIRA == "PME+", "PME", DSC_CARTEIRA)) 


set.seed(123)
risco_split <- df_blended_full %>% initial_split(strata = target)

risco_train <- training(risco_split)
risco_test <- testing(risco_split)


set.seed(123)
risco_folds <- vfold_cv(risco_train, strata = target)


xgb_spec <- boost_tree(mode = "classification") %>% 
  set_engine("xgboost")

set.seed(123)
risco_rec <- recipe(target ~ . , data = risco_train) %>% 
  step_rm(DAT_INICIO_VIGENCIA_PLANO, DAT_FIM_VIGENCIA_PLANO, SIG_UF_RESIDENCIA) %>% 
  update_role(COD_ANALITICO_BENEFICIARIO , new_role = "id") %>% 
  step_normalize(all_numeric_predictors()) %>% 
  step_dummy(all_nominal_predictors()) %>% 
  step_downsample(target, under_ratio = 2)


df_utilizado_modelo <- risco_rec %>% prep() %>% juice() 



wf_basic <- workflow(risco_rec, xgb_spec)



# library(doFuture)
# library(parallel)
# 
# registerDoFuture()
# 
# #?plan
# 
# n_cores <- detectCores()
# 
# plan(
#   strategy = cluster,
#   workers  = parallel::makeCluster(n_cores)
# )





ctrl_preds <- control_resamples(save_pred = TRUE)
xgb_basic <- fit_resamples(wf_basic, risco_folds, control = ctrl_preds)



collect_predictions(xgb_basic) %>% 
  conf_mat(target, .pred_class) %>% 
  autoplot()


collect_predictions(xgb_basic) %>% 
  filter(.pred_class == "medio_risco") %>% 
  select(contains("pred")) %>% 
  summary()

# Predicoes ----

risco_fit <- fit(wf_basic, risco_train)


#risco_fit %>% write_rds("models/risco_fit_1.rds")


predicao <- predict(risco_fit, risco_test, type = "prob")

risco_test %>% glimpse()

predicao %>% 
  bind_cols(risco_test %>% select(COD_ANALITICO_BENEFICIARIO, target)) %>% 
  filter(.pred_medio_risco > 0.6) %>% 
  count(target)


geral_pred <- predict(risco_fit, df_blended_full, type = "prob")

geral_pred_classe <- predict(risco_fit, df_blended_full)


tabela_mar_21_score <- geral_pred_classe %>% 
  bind_cols(geral_pred) %>% 
  bind_cols(df_blended_full %>% select(COD_ANALITICO_BENEFICIARIO)) %>% 
  rename(pred_alto_risco = .pred_alto_risco,pred_baixo_risco = .pred_baixo_risco,
         pred_medio_risco = .pred_medio_risco, pred_class = .pred_class)
  

DBI::dbWriteTable( bq_conn, "TB_SCORE_MAR_21_com_classe", tabela_mar_21_score, overwrite = TRUE)



tab_score_classe_bq <- dplyr::tbl(bq_conn,
                                  "SBX_NUCLEO_INDICADORES.TB_SCORE_MAR_21_com_classe")

geral_pred_classe %>% 
  bind_cols(df_blended_full %>% select(target)) %>% 
  conf_mat(target, .pred_class)

tb_geral_previsoes_score <- geral_pred %>% 
  bind_cols(df_blended_full %>% select(COD_ANALITICO_BENEFICIARIO)) %>% 
  rename(pred_alto_risco = .pred_alto_risco,pred_baixo_risco = .pred_baixo_risco,
         pred_medio_risco = .pred_medio_risco)



#DBI::dbWriteTable( bq_conn, "TB_SCORE_MAR_21", tb_geral_previsoes_score, overwrite = TRUE)
  
# Verifica dados benef q nao constam em predicoes ----
tb_pred <- dplyr::tbl(bq_conn,
                        "SBX_NUCLEO_INDICADORES.TB_SCORE_MAR_21")


bb <- fat_nao_inter %>% 
  anti_join(tb_pred) %>% 
  mutate(ano = year(DAT_ATENDIMENTO_EFETIVO_SINISTRO),
         mes = month(DAT_ATENDIMENTO_EFETIVO_SINISTRO)) %>% 
  ungroup() %>% 
  filter(ano >= 2020 & ano < 2022) %>% 
  group_by(COD_ANALITICO_BENEFICIARIO, ano, mes) %>% 
  summarise(gasto = sum(VLR_PAGO_ATENDIMENTO_TOTAL)) %>% 
  arrange(ano, desc(mes)) %>% 
  filter(ano == 2021 & mes == 3)
  

fat_nao_inter %>% 
  anti_join(tb_pred) %>% 
  filter(DAT_ATENDIMENTO_EFETIVO_SINISTRO >= "2021-03-01" & DAT_ATENDIMENTO_EFETIVO_SINISTRO < "2021-04-01") %>% 
  arrange(DAT_ATENDIMENTO_EFETIVO_SINISTRO) %>% 
  select(COD_ANALITICO_BENEFICIARIO, DAT_ATENDIMENTO_EFETIVO_SINISTRO, VLR_PAGO_ATENDIMENTO_TOTAL)
  


fat_nao_inter %>% 
  filter(COD_ANALITICO_BENEFICIARIO == "8OAMV01108724001") %>% 
  filter(DAT_ATENDIMENTO_EFETIVO_SINISTRO >= "2021-01-01" & DAT_ATENDIMENTO_EFETIVO_SINISTRO < "2021-04-01") %>% 
  arrange(DAT_ATENDIMENTO_EFETIVO_SINISTRO) %>% 
  select(COD_ANALITICO_BENEFICIARIO, DAT_ATENDIMENTO_EFETIVO_SINISTRO, VLR_PAGO_ATENDIMENTO_TOTAL)

# "8L1KH46629805001" nao consta na tabela de cadastro
# 8OAMV01108724001

tabela_full_cadastro %>% 
  filter(COD_ANALITICO_BENEFICIARIO == "8OAMV01108724001") 


tabela_cadastro %>% 
  filter(COD_ANALITICO_BENEFICIARIO == "8OAMV01108724001") %>% 
  select(COD_ANALITICO_BENEFICIARIO, starts_with("DAT_")) %>% 
  glimpse()



df_blended_full %>% 
  ggplot(aes(QTD_IDADE, fill = target)) +
  geom_density(alpha = 0.6)

df_blended_full %>% 
  filter(QTD_IDADE > 80) %>% 
  filter(target == "baixo_risco") %>% 
  head() %>% 
  filter(COD_ANALITICO_BENEFICIARIO == "0900101439163001") %>% 
  glimpse()


tabela_mar_21_score %>% 
  filter(COD_ANALITICO_BENEFICIARIO == "0900101439163001")



# Join com idoso ----


medico_tela_raw_bq <- dplyr::tbl(bq_conn,
                                 "SBX_NUCLEO_INDICADORES.TB_PICC_174_ESTUDO_MEDICO_NA_TELA")


medico_tela_raw_bq %>% 
  filter(DAT_ATENDIMENTO >= "2021-02-01" & DAT_ATENDIMENTO < "2021-04-01") %>% 
  inner_join(tab_score_classe_bq) %>% 
  count(DSC_PUBLICO_ESTUDO,pred_class, sort = TRUE ) %>% 
  group_by(DSC_PUBLICO_ESTUDO) %>% 
  mutate(pct = n / sum(n))
