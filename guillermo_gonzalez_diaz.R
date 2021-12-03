library(tidyverse)
library(data.table)
library(ggplot2)

###############################################################################

conn <- DBI::dbConnect(RSQLite::SQLite(),
                       "~/Desktop/etl/practica_asignatura/airbnb.sqlite")

DBI::dbListTables(conn)  

# BLOQUE I EXTRACCIÓN ########################################################
# EJERCICIO 1: LISTINGS ---------------------------------------------------

df_listings <- 
  as.data.frame(tbl(conn, sql("
                             SELECT list.price, list.number_of_reviews,
                             list.room_type, list.review_scores_rating, 
                             hood.neighbourhood_group
                             FROM Listings as list
                             INNER JOIN Hoods as hood
                             ON hood.neighbourhood = list.neighbourhood_cleansed")))

df_listings <- df_listings %>% 
  relocate(neighbourhood_group, .before = price) %>% 
  relocate(room_type, .before = price)

# EJERCICIO 2: EXTRACCIÓN REVIEWS -----------------------------------------
# Reviews a nivel de distrito y mes: 

df_reviews <- 
  as.data.frame(tbl(conn, sql("
                             SELECT COUNT(DISTINCT(rev.id)) AS num_reviews,
                             strftime('%Y-%m', date) as fecha, 
                             hood.neighbourhood_group as district
                             FROM Reviews as rev
                             INNER JOIN Listings as list
                             ON rev.listing_id = list.id
                             INNER JOIN Hoods as hood
                             ON hood.neighbourhood = list.neighbourhood_cleansed
                             WHERE date > '2010-12-31'
                             GROUP BY fecha , hood.neighbourhood_group")))  

df_reviews <- df_reviews %>% 
  relocate(num_reviews, .after = district) %>% 
  relocate(district, .before = fecha)

# BLOQUE 2: TRANSFORMACIÓN ###################################################
# Ejercicio 3: Transformación (listings). ---------------------------------
class(df_listings$price)

df_listings$price <- as.numeric(gsub("[\\$,]", "", df_listings$price))
class(df_listings$price)
sum(is.na(df_listings$price))  # Para comprobar que no ha hehco nada raro


# Ejercicio 4: (Opción A.)  -----------------------------------------------
# Sample tiene 3 argumentos: de donde coge los valores, el tamaño, y replacement:

df_listings$number_of_reviews[is.na(df_listings$number_of_reviews)] <- # afectamos esta columna
  
  sample(df_listings$number_of_reviews                # coge valores de aquí
         
         [!is.na(df_listings$number_of_reviews)],     # que NO sean Nas
         
         sum(is.na(df_listings$number_of_reviews)),   # del tamaño de los que son NAS
         
         replace=F)

sum(is.na(df_listings$number_of_reviews)) # comprobamos nas después


df_listings$review_scores_rating[is.na(df_listings$review_scores_rating)] <-
  
  sample(df_listings$review_scores_rating
         
         [!is.na(df_listings$review_scores_rating)],
         
         sum(is.na(df_listings$review_scores_rating)),
         
         replace=F)

sum(is.na(df_listings$review_scores_rating)) # compriobamos nas después


# 4B: (Opción B.)  --------------------------------------------------------

for (type in unique(df_listings$room_type)){ # tipos room
  for (col in c("number_of_reviews", "review_scores_rating")){ # esas cols
    na_bool <- is.na(df_listings[ ,col]) # boolian vector Nas
    if(all(na_bool == FALSE)){
      next
    }
    sample_values <- df_listings[df_listings$room_type == type & !na_bool, col] # resamole sin nas
    fill_values <- sample(sample_values, sum(na_bool), replace = TRUE) # valores usados
    df_listings[na_bool, col] <- fill_values # reemplaza los nas de col por fill_values
  }
}

sum(is.na(df_listings$number_of_reviews & df_listings$review_scores_rating))

# Ejercicio 5 -------------------------------------------------------------

# • Nota media ponderada (review_scores_rating ponderado con number_of_reviews). 
# • Precio mediano (price).
# • Número de alojamientos (id).

df_listings <- df_listings %>%
  group_by(neighbourhood_group, room_type) %>% 
  dplyr::summarise(median_price = median(price),
                   media_ponderada = weighted.mean(review_scores_rating,number_of_reviews))

# EJERCICIO 6 -------------------------------------------------------------

reviews_julio <- df_reviews %>%
  group_by(district, fecha) %>% 
  filter(fecha >= "2021-07") #%>% 
  #dplyr::summarise(num_reviews = n())

# Cuántos barrios hay:
district <- reviews_julio$district
# Cuántas reviews tenéian en julio
num_reviews <- reviews_julio$num_reviews
# Cuales son esas fechas, cambiado julio por agosto: 
fecha <- reviews_julio$fecha
fecha <- gsub("-07", "-08", fecha)
# Cuantas veces se repite 07, las repetimos con 08.
nrow(reviews_julio)
mes <- rep("08", 122)
# Creamos el dataframe para agosto:
prediccion_agosto <- data.frame(district, fecha, num_reviews)


# Ahora combinamos este dataframe con el original:
df_reviews <- rbind(prediccion_agosto, df_reviews)
df_reviews <- df_reviews %>% 
  group_by(fecha)



# EJERCICIO 7 -------------------------------------------------------------

# Chequeamos el mínimo y máximo de la columna fecha:
min(df_reviews$fecha)
max(df_reviews$fecha) # vamos de 2011-01 a 2021-07

# Creamos un vector de fechas que vaya desde 2011-01 : 2021-07
fechas_posibles <- seq(as.Date("2011-01-01"), as.Date("2021-07-01"), by="months")

# Corregimos el formato
fechas_posibles <- format(fechas_posibles, "%Y-%m") 
fechas_posibles <- char(fechas_posibles)

# Distritos posibles
distritos_posibles <- unique(df_reviews$district)

# Juntamos los dos en un dataframe con las combinaciones posibles:
comb_pos <- expand_grid(distritos_posibles, fechas_posibles)
colnames(comb_pos) <- c("district", "fecha")


# Lo juntamos con el df final.
df_reviews <- df_reviews %>% 
  bind_rows(comb_pos)

#df_reviews <- merge(df_reviews, comb_pos, all= TRUE)

# Chequeamos NAS. 
sum(is.na(df_reviews$num_reviews))

# Sustituimos NAS por 0s:
df_reviews[is.na(df_reviews)] = 0

# BLOQUE 3 CARGA ########################################################
# EJERCICIO 8 -------------------------------------------------------------

DBI::dbWriteTable(con = conn, name = 'df_listings', value= df_listings, overwrite=FALSE)
DBI::dbWriteTable(con = conn, name = 'df_reviews', value= df_reviews, overwrite=FALSE)

query_check <- tbl(conn, sql("
                             SELECT * 
                             FROM df_listings"))  
query_check %>% collect() 

query2_check <- tbl(conn, sql("
                             SELECT * 
                             FROM df_reviews")) 
query2_check %>% collect() 


