
# -------------------------------------------------------------------------#
#                        Pronostica tus ventas con R                       #
# -------------------------------------------------------------------------#

# Librerias: 

library(tidyverse)
library(lubridate)
library(tsibble)
library(openxlsx)
library(readxl)


# Lectura de datos --------------------------------------------------------

# Como viene del SAIKU:

raw_data <- read_csv(file = "data/Base NO procesada ciuu de Comercio.csv")

ciiu <- read_excel("data/CIIU SRI Luis sin punto.xls",skip = 1)

ciiu <- ciiu %>% 
  select(ciiu = CODIGO,
         descripcion = DESCRIPCIÓN...2) %>% 
  filter(str_count(ciiu) == 7)
  

# Preprocesamiento --------------------------------------------------------

# Limpiar los nombres de las variables:

nombres_limpios <- names(raw_data) %>% 
  str_to_lower() %>% 
  str_remove_all(pattern = "[:punct:]") %>% 
  str_replace_all(pattern = "[:space:]",
                  replacement = "_") 

# Creación de diccionario

diccionario <- tibble(etiquetas = names(raw_data),
                      variables = nombres_limpios,
                      fuente = "Saiku SRI")
         

print(diccionario)

# Renombrado:

# Cuando tienes una función:

raw_data %>% 
  rename_with(.fn = str_to_title)

# Cuando tienes un vector:

raw_data <- raw_data %>% 
  rename_with(.fn = ~ nombres_limpios)


# Creación de nuevas variables -----------------------------------------------

# Agregados a nivel de empresa:

raw_data <- raw_data %>% 
  mutate(ventas_12 = ventas_locales_12_411 + ventas_activos_fijos_12_412,
         ventas_0 = ventas_locales_0_413 + ventas_0_de_activos_fijos_414 + ventas_0_con_cred_trib_415 + ventas_0_de_af_con_cred_trib_416,
         ventas_totales = ventas_12 + ventas_0)

# Fechas:

raw_data <- raw_data %>% 
  mutate(
    fecha = str_c(anio_fiscal,mes_fiscal,"01",sep = "-"),
    fecha = ymd(fecha),
    trimestre = quarter(fecha),
    
    # Depende del sistema operativo, ver con ayuda de:  Sys.getlocale()
    # A la hora de hacer este ejercicio se tiene la siguiente configuración:
    # [1] "LC_COLLATE=Spanish_Ecuador.utf8;LC_CTYPE=Spanish_Ecuador.utf8;LC_MONETARY=Spanish_Ecuador.utf8;LC_NUMERIC=C;LC_TIME=Spanish_Ecuador.utf8"
    
    mes_lbl = month(fecha,
                    label = TRUE,
                    abbr = FALSE), 
    mes_lbl = str_to_sentence(mes_lbl)
    ) 
  

# Diccionario de nuevas variables: ----------------------------------------

new_var <- tibble(variables = c(
  "ventas_12",
  "ventas_0",
  "ventas_totales",
  "fecha",
  "trimestre",
  "mes_lbl"),
  etiquetas = c(
    "Ventas gravadas al 12%",
    "Ventas gravadas al 0%",
    "Ventas totales",
    "Fecha (YYYY/MM/DD)",
    "Trimestre",
    "Etiqueta de mes"
  ))

print(new_var)

# Exportación de tablas ---------------------------------------------------


# Procesada para power BI 

reporte_1 <- raw_data %>% 
  inner_join(y = unique(ciiu),
             by = c("actividad_economica" = "ciiu")) %>% 
  select(`ANIO FISCAL` = anio_fiscal,
         `MES FISCAL` = mes_fiscal,
         `Tipo de Contribuyente` = tipo_contribuyente,
         `CIIU 1` = familia,
         `Actividad` = descripcion,
         `VENTAS TOTALES` = ventas_totales,
         `Ventas 12%` = ventas_12,
         `Ventas 0%` = ventas_0,
         `Mes 1` = mes_lbl,
         `Trimestres` = trimestre,
         `Fecha` = fecha) 


# write.xlsx(x = reporte_1,
#            file = "data/procesada_power_bi.xlsx",
#            asTable = TRUE,
#            overwrite = T) 
# 

# Salida de datos para el taller: -----------------------------------------


tabla_taller <- raw_data %>% 
  inner_join(y = unique(ciiu),
             by = c("actividad_economica" = "ciiu")) %>% 
  select(anio_fiscal,
         mes_fiscal,
         tipo_contribuyente,
         actividad_economica,
         descripcion,
         ventas_0,
         ventas_12) 


tabla_taller %>% 
  write_csv(file = "data/ventas_comercio.txt")

tibble(
  variable = names(tabla_taller),
  descripcion = c("Año fiscal",
                  "Mes fiscal",
                  "Código CIIU a 6 digitos",
                  "Descripción de la actividad económica",
                  "Ventas gravadas con el 0% de IVA",
                  "Ventas gravadas con el 12% de IVA"))%>% 
  write_csv(file = "data/diccionario_ventas_comercio.txt")


# Para el taller:
# 
# raw_data %>% 
#   select(anio_fiscal,
#          mes_fiscal,
#          ventas_totales,
#          sactividad_economica)
#   write_csv(x = )


# Si yo les muestro esta gráfica: 

# Serie de tiempo de las ventas totales del Ecuador

# ¿Entre que rangos creen que cae la preducción 2023?
