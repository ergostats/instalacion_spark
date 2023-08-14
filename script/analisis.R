# ------------------------------------------------------------------------- #
#                         Proyección de ventas 2023                         #
# ------------------------------------------------------------------------- #

# Instructor: Alex Bajaña (alexvbr@ergostats.org)
# Organización: Fernando Arevalo (fdarevalo@lacamaradequito.com)
# Fecha del taller: Diciembre 15, 2022



# Libraries ---------------------------------------------------------------

library(tidyverse)
library(lubridate)
library(tsibble)
library(fable)
library(feasts)

# Lectura de los datos: ---------------------------------------------------

# Ventas:

base_ventas <- read_csv(file = "data/ventas_comercio.txt")

# Diccionario:

diccionario_df <- read_csv(_______)


# Transformación a objeto de tipo date: -----------------------------------

#### Ejemplos:

# Formato clásico:

ymd("2012-10-07")

# Formato de excel:

dmy("1/1/2021")

# Extraer partes de una fecha:

fechas <- _______(c("20120101","20150407","20180909"))

month(fechas,label = TRUE,abbr = FALSE)

# Operaciones con fechas:

quarter(fechas,type = "year.quarter")

#### Aplicación sobre nuestra tabla:

# Solución cuando tienes en ingles, solo para windows:

____________ # Verificar con Sys.getlocale()


# Transformamos a tsibble() -----------------------------------------------

base_ventas <- base_ventas %>% 
  mutate(fecha = str_c(anio_fiscal,mes_fiscal,"01",sep = "-"),
         fecha = _____(fecha))


# Establecemos la key (identificador) y el index (varaible de fecha) ------


base_ventas <- base_ventas %>% 
  as_tsibble(index = _______,
             key = ________)

# Proporción de ventas gravadas y desgravadas sobre el total de ventas

base_ventas <- base_ventas %>% 
  mutate(ventas_totales = ________) 

# Un tsibble conserva su forma de tibble!

# Primer resumen:

resumen_preliminar <- base_ventas %>% 
  index_by(anio_fiscal) %>% 
  group_by(tipo_contribuyente) %>% 
  summarise(ventas_promedio = mean(ventas_totales),
            ventas_totales = _____(ventas_totales), 
            ventas_medianas = median(______), 
            sd_ventas = ______(ventas_totales))

# Primer gráfico

autoplot(________,
         ________)

# Vamos a ir modificando nuestro gráfico ----------------------------------

trimestrales <- base_ventas %>% 
  mutate(trimestre = yearquarter(fecha,fiscal_start = 1)) %>% 
  index_by(_______) %>% 
  summarise(ventas_totales = sum(________))

# Generación de series trimestrales

autoplot(trimestrales,
         _______)


# Identificación de la tendencia estacional -------------------------------


# Tendencia trimestral:

gg_season(_______,________)

# Mensuales:

mensual_ventas_gravadas <- base_ventas %>% 
  mutate(meses = ___________(fecha)) %>% 
  index_by(meses)  %>% 
  summarise(ventas_totales = sum(ventas_totales))


# Verificar que tengamos todos los datos:

tsibble::has_gaps(_________)

tsibble::scan_gaps(_________)

tsibble::fill_gaps(_________)

# Veamos nuevamente la estacionalidad:

gg_season(data = ___________,
          ___________)


# Comparación interanual -------------------------------------------------------------

trimestrales <- base_ventas %>% 
  mutate(trimestre = __________(fecha,_______ = 1)) %>% 
  index_by(trimestre) %>% 
  _________(tipo_contribuyente) %>% 
  summarise(ventas_totales = sum(ventas_totales))

# Gráfico:

gg_subseries(_______,_________)



# Relación del presente con el pasado -----------------------------------


gg_lag(data = trimestrales,y = ________,geom = _______)

# Función de autocorrelación temporal:

mensual_ventas_gravadas %>% 
  ______() %>% 
  autoplot()


# Prediccion: -------------------------------------------------------------


# Otra forma de filtrar la tabla:

trimestrales <- trimestrales %>%
  filter_index("2011 Q1" ~ "2022 Q3") %>%
  select(ventas_totales)

# Modelo 1: Super simple, usamos la media

prediccion <- trimestrales %>% 
  model(______(ventas_totales)) %>% 
  _______(h = "6 months") 

# Gráfico del primer modelo:

prediccion %>% 
  autoplot(trimestrales) %>% +
  geom_line(aes(y = mean(ventas_totales)), colour = "#0072B2", linetype = "dashed") + 
  geom_line(_________)

# Modelo 2: Usamos la función seasonal SNAIVE

trimestrales %>% 
  model(________(ventas_totales ~ lag("6 months") + lag("year"))) %>% 
  forecast(h = "6 months") %>% 
  autoplot(_______) +
  geom_point(data = slice(trimestrales,
                          (n()-6):n()), 
             aes(y=ventas_totales), colour = "#0072B2")


# Un mejor candidato ------------------------------------------------------

# Filtramos para los datos que tenemos:

mensual_completo <- mensual_ventas_gravadas %>% 
  filter(meses <= ______(____ = 2022,_____ = 8)) 

# Modelo 3: Time series linear model

# Modelo con tencia lineal y estacional:

modelo_ts <- mensual_completo %>% 
  model(modelo_con_tendencia = TSLM(formula = ventas_totales ~ trend() + season()))

# Predicción:

modelo_ts %>% 
  forecast(h = "6 months")  %>%  
  autoplot(mensual_completo)

# Descomponemos la serie de tiempo:
# * Tendencia lineal
# * Tendencia estacional
# * Otros componentes

modelo_tsl <- mensual_completo %>% 
  ________(modelo_con_tendencia = ______(formula = ventas_totales ~ season(window = Inf)))

# Extraemos la tendencia

tendencia <- ______(modelo_tsl) %>% 
  select(meses,trend)

# Dibujamos la serie nuevamente:

modelo_ts %>% 
  forecast(h = "6 months")  %>%  
  autoplot(mensual_completo) +
  geom_line(________)
