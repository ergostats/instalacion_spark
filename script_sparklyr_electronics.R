
# ------------------------------------------------------------------------- #
#                    Análisis de Big Data con Spark y R                     #
# ------------------------------------------------------------------------- #
# Centro de Investigación Estadística ERGOSTATS
# Agosto 2023

# Instalación y configuración  ---------------------------------------------

# Solo una vez:
# install.packages(c("tidyverse",
#                    "sparklyr",
#                    "dbplot"))

# Cargamos las librerias:

library(tidyverse)
library(sparklyr)
library(dbplot)

# Manos a la obra

spark_install_find()

spark_versions() %>%
  View()

# Descomentamos solo si no tenemos una versión de Spark:
# spark_install("3.3.2")

# Conexión:
conn <- spark_connect(master = "local",
                      version = "3.3.2")

# Lectura de los datos: ---------------------------------------------------


electronics <- spark_read_csv(sc = conn,
                              name = "electronics",
                              path = "data/ElectronicsProductsPricingData.csv")

# Preview de las 100 primeras lineas de la tabla

show(electronics)

# Obtengo los nombres de las variables:
tbl_vars(electronics)

# Limpieza de los datos ---------------------------------------------------

# Reduzco el número de columnas:


electronics <- electronics %>%
  select(prices_condition,
         prices_currency,
         prices_merchant,
         prices_shipping,
         prices_amountMax,
         prices_amountMin,
         categories,
         name,
         manufacturer,
         brand,
         weight)

# Cabecera para llevar a R:

observ <- electronics %>%
  head(60) %>%
  collect()

# Separo el peso en: valor y unidad

electronics <- electronics %>%
  ft_regex_tokenizer(input_col="weight",
                     output_col="weightSplit",
                     pattern=" ") %>%
  sdf_separate_column("weightSplit",
                      into=c("weight_value",
                             "weight_unit")) %>%
  select(-weightSplit)

observ <- electronics %>%
  head(60) %>% 
  collect()

# Podemos llevar objetos de R a Spark:

unidades <- electronics %>%
  count(weight_unit) %>%
  collect()

# Creemos en R una tabla para transformar las unidades a libras:

unidades <- unidades %>% 
  mutate(
    factor = case_when(
      weight_unit == "pounds" ~ 1,
      weight_unit == "lb" ~ 1,
      weight_unit == "ounces" ~ 1/16,
      weight_unit == "oz" ~ 1/16,
      weight_unit == "lbs" ~ 1,
      weight_unit == "lb." ~ 1,
      weight_unit == "g" ~ 1/453.59237,
      weight_unit == "kg" ~ 1/0.45359237)  
  )

# electronics <- electronics %>%
#   inner_join(unidades) # Esto no funciona ¿por qué?

# Lo subo a spark para que sea más fácil operar:

unidades_peso <- copy_to(dest = conn,
                         df = unidades,
                         name = "unidades_peso",
                         overwrite = T)

# Unimos la tabla de unidades y homologamos el peso:

electronics <- electronics %>%
  inner_join(unidades_peso) %>%
  mutate(
    weight_value = as.numeric(weight_value),
    weight_value_hom = weight_value*factor
  ) %>%
  select(-n,
         -factor,
         -weight_value,
         -weight_unit)

# Mismo tratamiento para el precio de entrega:
# Considerando que hay dólares y dólares canadienses:
precios_envio <- electronics %>%
  count(prices_shipping) %>% 
  collect()

# Tratamiento para los precios de envio:
precios_envio <- precios_envio %>% 
  mutate(prices_shipping_sep = prices_shipping) %>% 
  separate(prices_shipping_sep,
           sep = " ",
           into = c("unit_shipping",
                    "price_shipping")) 


precios_envio %>% 
  count(unit_shipping)
# Transformamos todo a dolar:

precios_envio <- precios_envio %>% 
  mutate(
    factor_shipping = case_when(
      unit_shipping == "USD" ~ 1,
      unit_shipping == "CAD" ~ 0.78,
      str_detect(prices_shipping,"[Ff]ree") ~ 0,
      TRUE ~ NA_real_),
    price_shipping_dolar = as.numeric(price_shipping),
    price_shipping_dolar = price_shipping_dolar*factor_shipping,
  ) %>% 
  select(prices_shipping,price_shipping_dolar)


precios_envio <- copy_to(dest = conn,
                         df = precios_envio,
                         name = "precios_envio",
                         overwrite = T)

# Unimos a la base:

electronics <- electronics %>% 
  inner_join(precios_envio) %>% 
  select(-prices_shipping)


# Hacemos la misma transformación a la unidad de precio:

electronics %>% 
  count(prices_currency)

# Homologamos los precios:

electronics <- electronics %>% 
  mutate(
    across(.cols = c(prices_amountMax,
                     prices_amountMin),
           ~case_when(
             prices_currency == "USD" ~ .x*1,
             prices_currency == "CAD" ~ .x*0.78,
             TRUE ~ NA_real_))
  ) %>% 
  select(-prices_currency)

# Verificamos la condición del precio:


electronics %>% 
  distinct(prices_condition)

# Extraemos las condiciones del precio:

electronics <- electronics %>% 
  mutate(
    prices_condition = str_to_lower(prices_condition),
    prices_condition_new =  regexp_extract(prices_condition, 
                                           "(new|used|refurbished)"),
    prices_condition_new = if_else(prices_condition_new == "",
                                   NA_character_,
                                   prices_condition_new)) %>% 
  select(-prices_condition)


# Verificamos:
electronics %>% 
  distinct(prices_condition_new)

electronics <- electronics %>% 
  mutate(manufacturer = if_else(is.na(manufacturer)," ",manufacturer))


# Tenemos nuestra base de datos limpia !!

observ2 <- electronics %>% 
  head(60) %>% 
  collect()


# Ingeniería de variables -------------------------------------------------


# Transformar las columnas de precios;

pipe_line <- ml_pipeline(conn)

pipe_line <- pipe_line %>% 
  ft_imputer(input_cols = c("price_shipping_dolar",
                            "prices_amountMax",
                            "prices_amountMin",
                            "weight_value_hom"),
             output_cols = c("price_shipping_dolar_t",
                             "prices_amountMax_t",
                             "prices_amountMin_t",
                             "weight_value_hom_t"),
             strategy = "median")  %>% 
  ft_vector_assembler(input_cols = "prices_amountMin_t",
                      output_col = "prices_amountMin_t_v")   %>% 
  ft_vector_assembler(input_cols = "prices_amountMax_t",
                      output_col = "prices_amountMax_t_v")  %>% 
  ft_vector_assembler(input_cols = "price_shipping_dolar_t",
                      output_col = "price_shipping_dolar_t_v") %>% 
  ft_vector_assembler(input_cols = "weight_value_hom_t",
                      output_col = "weight_value_hom_t_v") 
  

# Distirbuciones ----------------------------------------------------------


numericas <- electronics %>% 
  select(prices_amountMin,
         weight_value_hom) %>% 
  collect()


# numericas %>% 
#   ggplot() +
#   geom_histogram(aes(x = prices_amountMin))
# 
# numericas %>% 
#   ggplot() +
#   geom_histogram(aes(x = weight_value_hom))

# Vamos a establecer la estructura de factores adecuada:

pipe_line <- pipe_line %>% 
  ft_standard_scaler(input_col = "prices_amountMin_t_v",
                     output_col = "prices_amountMin_t_t",
                     with_mean = TRUE,
                     with_std = TRUE) %>% 
  ft_standard_scaler(input_col = "prices_amountMax_t_v",
                     output_col = "prices_amountMax_t_t",
                     with_mean = TRUE,
                     with_std = TRUE)  %>% 
  ft_standard_scaler(input_col = "price_shipping_dolar_t_v",
                     output_col = "price_shipping_dolar_t_t",
                     with_mean = TRUE,
                     with_std = TRUE) %>% 
  ft_standard_scaler(input_col = "weight_value_hom_t_v",
                     output_col = "weight_value_hom_t_t",
                     with_mean = TRUE,
                     with_std = TRUE)


# Tratamos a los textos:

pipe_line <- pipe_line %>% 
  ft_tokenizer(input_col = "prices_merchant",
               output_col = "prices_merchant_t") %>%
  ft_stop_words_remover(input_col = "prices_merchant_t",
                        output_col = "prices_merchant_t_t") %>%
  ft_count_vectorizer(input_col = "prices_merchant_t_t", 
                      output_col="prices_merchant_t_c",
                      min_df=1,
                      binary=TRUE) %>%
  ft_tokenizer(input_col = "name",
               output_col = "name_t") %>%
  ft_stop_words_remover(input_col = "name_t",
                        output_col = "name_t_t") %>%
  ft_count_vectorizer(input_col = "name_t_t", 
                      output_col="name_t_c",
                      min_df=1,
                      binary=TRUE) %>%
  ft_tokenizer(input_col = "manufacturer",
               output_col = "manufacturer_t") %>% 
  ft_stop_words_remover(input_col = "manufacturer_t",
                        output_col = "manufacturer_t_t") %>%
  ft_count_vectorizer(input_col = "manufacturer_t_t", 
                      output_col="manufacturer_t_c",
                      min_df=1,
                      binary=TRUE) %>% 
  ft_tokenizer(input_col = "brand",
               output_col = "brand_t") %>% 
  ft_stop_words_remover(input_col = "brand_t",
                        output_col = "brand_t_t") %>%
  ft_count_vectorizer(input_col = "brand_t_t", 
                      output_col="brand_t_c",
                      min_df=1,
                      binary=TRUE) %>% 
  ft_tokenizer(input_col = "categories",
               output_col = "categories_t") %>% 
  ft_stop_words_remover(input_col = "categories_t",
                        output_col = "categories_t_t") %>%
  ft_count_vectorizer(input_col = "categories_t_t", 
                      output_col="categories_t_c",
                      min_df=1,
                      binary=TRUE) 

pipe_line <- pipe_line %>% 
  ft_vector_assembler(input_cols = c("prices_amountMin_t_t",
                                     "prices_amountMax_t_t",
                                     "price_shipping_dolar_t_t",
                                     "weight_value_hom_t_t",
                                     "prices_merchant_t_c",
                                     "name_t_c",
                                     "manufacturer_t_c",
                                     "brand_t_c",
                                     "categories_t_c"),
                      output_col = "features")


# Añadimos el modelo 

pipe_line <- pipe_line %>% 
  ml_kmeans(features_col = "features",
            prediction_col = "cluster",
            k = 10)

# Creamos dos sets ------------------

partitions <- electronics %>%
  sdf_random_split(training = 0.7,
                   test = 0.3,
                   seed = 1111)

electronics_training <- partitions$training

electronics_test <- partitions$test


  

modelo <- ml_fit(x = pipe_line,
                 dataset = electronics_training)

# sparklyr::ml_save(x = "")

prediction <- ml_transform(x = modelo,
                           dataset = electronics_training)



# prediction %>%
#   count(cluster)

# Veamos que tal nos fue --------------------------------------------------

all_words <- prediction %>%
  mutate(categories = regexp_replace(categories, "[_\"\'():;,.!?\\-]", " ")) %>%
  ft_tokenizer(
    input_col = "categories",
    output_col = "word_list"
  ) %>%
  ft_stop_words_remover(
    input_col = "word_list",
    output_col = "wo_stop_words"
  ) %>%
  mutate(word = explode(wo_stop_words)) %>%
  filter(nchar(word) > 2) 



# Veamos la clasificación -------------------------------------------------

counte <- all_words %>%
  count(word,cluster) %>%
  collect()

counte %>%
  group_by(cluster) %>%
  top_n(5) %>%
  arrange(cluster,n) %>%
  View()


prediction <- ml_transform(x = modelo,
                           dataset = electronics_test)
