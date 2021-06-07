
library(purrr)
library(stringi)
library(dplyr)
library(writexl)
library(RMySQL)
library(stringdist)
library(stringr)


#load('data/patients_cs_polana_canico.RData')
setwd("/home/agnaldo/Git/iDART/data-clean")

# vec_us_names <- c("albasine","bagamoio","magoanine_a","catembe","cimento",
#                   "chamanculo","maxaquene","porto","xipamanine","zimpeto","cimento","1_maio","1_junho"
#                   ,"josemacamo")

vec_us_names <- c("altomae","canico")
for ( index in 1:length(vec_us_names)) {
  
#rm(list=setdiff(ls(), c("vec_us_names", "index") ))
      
source('genericFunctions.R')
## OpenMRS  - Configuracao de variaveis de conexao 
openmrs.user ='root'                          # ******** modificar
openmrs.password='password'                   # ******** modificar
openmrs.db.name= vec_us_names[index]                     # ******** modificar
openmrs.host='192.168.1.10'            # ******** modificar
openmrs.port=3306                             # ******** modificar

# Objecto de connexao com a bd openmrs
con_openmrs = dbConnect(MySQL(), user=openmrs.user, password=openmrs.password, dbname=openmrs.db.name, host=openmrs.host, port=openmrs.port)
if(openmrs.db.name=='polana_canico' ){
  load(paste0("data/patients_cs_","canico",".RData"))
}else if(openmrs.db.name=='1_maio' ){
  load(paste0("data/patients_cs_","maio",".RData"))
} else if(openmrs.db.name=='1_junho' ){
  load(paste0("data/patients_cs_","junho",".RData"))
}else if(openmrs.db.name=='josemacamo_cs' ){
  load(paste0("data/patients_cs_","josemacamo",".RData"))
}  else if(openmrs.db.name=='magoanine_a' ){
  load(paste0("data/patients_cs_","magoaninea",".RData"))
} else {
  load(paste0("data/patients_cs_",openmrs.db.name,".RData"))
}


## Pacientes
## Buscar todos pacientes OpenMRS & iDART
openmrs_patients <- getAllPatientsOpenMRS(con_openmrs)
openmrs_patients$data_estado <- as.Date(openmrs_patients$data_estado)
openmrs_patients$data_ult_consulta <- as.Date(openmrs_patients$data_ult_consulta)
openmrs_patients$data_prox_marcado <- as.Date(openmrs_patients$data_prox_marcado)

pacientes_dados_consistentes <- filter(patients, openmrs_status=='Ok' & estado_permanencia %in% c('ACTIVO NO PROGRAMA', 'TRANSFERIDO DE' )
                                       & duplicado_openmrs=='No' & uuidopenmrs==uuid_api_openmrs)

patients <- patients %>% filter(! id %in% pacientes_dados_consistentes$id)

## Pacientes Nao activos no OpenMRS( pelo API)
pacientes_nao_activos <- filter(patients, openmrs_status=='Ok' &  estado_permanencia =='ABANDONO/TRANSFERIDO PARA/NAO REGISTADO')

# Pacientes com estado permanencia TARV ='ABANDONO/TRANSFERIDO PARA/NAO REGISTADO' mas com consultas recentes
# estado_permanencia_incosistente <- left_join(pacientes_nao_activos, openmrs_patients, by = c('patientid'='identifier')) %>%
#   filter(  ( !is.na(data_ult_consulta) &  !is.na(data_prox_marcado) & data_ult_consulta > data_estado & data_prox_marcado > '2020-09-21' ) 
#            | (is.na(data_estado) & !is.na(data_ult_consulta)) | (!is.na(ult_levant_idart) & is.na(data_estado))) %>% 
#   select(patientid, firstnames, lastname, full_name_openmrs,  ult_levant_idart,estado_permanencia, 
#    uuid_api_openmrs,uuidopenmrs, estado_tarv, data_estado,data_ult_consulta,data_prox_marcado, duplicado_openmrs)

estado_permanencia_incosistente <- left_join(pacientes_nao_activos, openmrs_patients, by = c('patientid'='identifier')) %>%
  filter(  ( !is.na(data_ult_consulta) &  !is.na(data_prox_marcado) & data_ult_consulta > data_estado & data_prox_marcado > '2020-09-21' ) 
         ) %>% 
  select(patientid, firstnames, lastname, full_name_openmrs,  ult_levant_idart,estado_permanencia, 
         uuid_api_openmrs,uuidopenmrs, estado_tarv, data_estado,data_ult_consulta,data_prox_marcado, duplicado_openmrs)

if(nrow(estado_permanencia_incosistente)> 0) {
  write_xlsx(x =estado_permanencia_incosistente , path = paste0('share/cs_',openmrs.db.name,'_estado_permanencia_incosistente_corrigir_manualmente.xlsx') )
}

patients <- patients %>% filter(! patientid %in% estado_permanencia_incosistente$patientid)

# Pacientes nao encontrados no OpenMRS por NID (API request)
pacientes_nao_encontrados <- filter(patients, openmrs_status=='Not found')
pacientes_nao_encontrados <- pacientes_nao_encontrados %>% select(id, patientid,dateofbirth, firstnames,lastname,startreason,totaldispensas,ult_levant_idart,uuidopenmrs,uuid_api_openmrs,
                          openmrs_status,nid_openmrs,openmrs_full_name)

# Pacientes nao encontrados no OpenMRS por NID (API request) mas existem no OpenMRS com o mesmo uuidopenmrs
# verificar a semelhanca de NIDs OpenMRS/iDART
# TRUE/FALSE na coluna OBS
pacientes_nids_difer_uuid_iguais <- filter( pacientes_nao_encontrados, nchar(nid_openmrs)>0 )

if(nrow(pacientes_nids_difer_uuid_iguais)> 0){
  pacientes_nids_difer_uuid_iguais$obs <- map2_lgl(pacientes_nids_difer_uuid_iguais$patientid,pacientes_nids_difer_uuid_iguais$nid_openmrs,.f =checkNidSimilarity )
  for (v in 1:nrow(pacientes_nids_difer_uuid_iguais) ) {
    if(!pacientes_nids_difer_uuid_iguais$obs[v]){
      x <- pacientes_nids_difer_uuid_iguais$firstnames[v]
      y <- pacientes_nids_difer_uuid_iguais$openmrs_full_name[v] 
      pacientes_nids_difer_uuid_iguais$obs[v] <-checkNameSimilarity(x,y)
    }
    
  }
  
  pacientes_nids_difer_uuid_iguais  <- filter(pacientes_nids_difer_uuid_iguais,trimws(patientid) != trimws(nid_openmrs))
  # Gerar sql script para Actualizar o NID no iDART
  for( i in 1:nrow(pacientes_nids_difer_uuid_iguais)){
    
    if(pacientes_nids_difer_uuid_iguais$obs[i]){
      
      # pacientes da farmac geral um sql para actualizar
      if( pacientes_nids_difer_uuid_iguais$startreason[i] %in% c('Referido para outra Farmacia','Voltou da Referencia')){
        
        id <- pacientes_nids_difer_uuid_iguais$id[i]
        patientid <- pacientes_nids_difer_uuid_iguais$nid_openmrs[i]
        uuidopenmrs <- pacientes_nids_difer_uuid_iguais$uuid_api_openmrs[i]
        old_patientid <- pacientes_nids_difer_uuid_iguais$patientid[i]
        
        sql_update_pi <- paste0(" update patientidentifier set  value ='",
                                patientid,
                                "' where patient_id =",id, " ;")
        sql_update_patient<- paste0(" update patient set patientid  ='",
                                    patientid,
                                    "' , uuidopenmrs = '",
                                    uuidopenmrs,
                                    "'  where id = ",id, " ;")
        
        sql_update_packagedrung<- paste0(" update packagedruginfotmp set patientid  ='",
                                         patientid,
                                         "' where patientid ='",old_patientid, "' ;")
        sql_update_sync_temp_patients <- paste0(" update sync_temp_patients set patientid  ='",
                                                patientid,
                                                "' , uuidopenmrs = '",
                                                uuidopenmrs,
                                                "' where patientid ='",old_patientid, "' ;")
        sql_update_sync_temp_dispense <- paste0(" update sync_temp_dispense set patientid  ='",
                                                patientid,
                                                "' , uuidopenmrs = '",
                                                uuidopenmrs,
                                                "' where patientid ='",old_patientid, "' ;")
        message(sql_update_pi)
        message(sql_update_patient)
        message(sql_update_packagedrung)
        message(sql_update_sync_temp_patients)
        write("-- ------------------------------------------------------------------------------------------------",file=paste0('share/cs_',openmrs.db.name,"_sql_update_farmac_nid.sql"),append=TRUE)
        write(sql_update_pi,file=paste0('share/cs_',openmrs.db.name,"_sql_update_farmac_nid.sql"),append=TRUE)
        write(sql_update_patient,file=paste0('share/cs_',openmrs.db.name,"_sql_update_farmac_nid.sql"),append=TRUE)
        write(sql_update_packagedrung,file=paste0('share/cs_',openmrs.db.name,"_sql_update_farmac_nid.sql"),append=TRUE)
        write(sql_update_sync_temp_patients,file=paste0('share/cs_',openmrs.db.name,"_sql_update_farmac_nid.sql"),append=TRUE)
        write(sql_update_sync_temp_dispense,file=paste0('share/cs_',openmrs.db.name,"_sql_update_farmac_nid.sql"),append=TRUE)
        write("-- ------------------------------------------------------------------------------------------------",file=paste0('share/cs_',openmrs.db.name,"_sql_update_farmac_nid.sql"),append=TRUE)
        
      } else {
        
        id <- pacientes_nids_difer_uuid_iguais$id[i]
        patientid <- pacientes_nids_difer_uuid_iguais$nid_openmrs[i]
        uuidopenmrs <- pacientes_nids_difer_uuid_iguais$uuid_api_openmrs[i]
        old_patientid <- pacientes_nids_difer_uuid_iguais$patientid[i]
        
        sql_update_pi <- paste0(" update patientidentifier set  value ='",
                                patientid,
                                "' where patient_id =",id, " ;")
        sql_update_patient<- paste0(" update patient set patientid  ='",
                                    patientid,
                                    "' , uuidopenmrs = '",
                                    uuidopenmrs,
                                    "'  where id = ",id, " ;")
        sql_update_packagedrung<- paste0(" update packagedruginfotmp set patientid  ='",
                                         patientid,
                                         "' where patientid ='",old_patientid, "' ;")
        sql_update_sync<- paste0(" update sync_openmrs_dispense set nid  ='",
                                         patientid,
                                         "' where nid ='",old_patientid, "' ;")
        
        
        
        message(sql_update_pi)
        message(sql_update_patient)
        message(sql_update_packagedrung)
        
        write("-- ------------------------------------------------------------------------------------------------",file=paste0('share/cs_',openmrs.db.name,"_sql_fix_wrong_nids_querys.sql"),append=TRUE)
        write(sql_update_pi,file=paste0('share/cs_',openmrs.db.name,"_sql_fix_wrong_nids_querys.sql"),append=TRUE)
        write(sql_update_patient,file=paste0('share/cs_',openmrs.db.name,"_sql_fix_wrong_nids_querys.sql"),append=TRUE)
        write(sql_update_packagedrung,file=paste0('share/cs_',openmrs.db.name,"_sql_fix_wrong_nids_querys.sql"),append=TRUE)
        write(sql_update_sync,file=paste0('share/cs_',openmrs.db.name,"_sql_fix_wrong_nids_querys.sql"),append=TRUE)
        #write(sql_update_sync_temp_patients,file=paste0('share/cs_',openmrs.db.name,"_sql_fix_wrong_nids_querys.sql"),append=TRUE)
        
        write("-- ------------------------------------------------------------------------------------------------",file=paste0('share/cs_',openmrs.db.name,"_sql_fix_wrong_nids_querys.sql"),append=TRUE)
      }
      
      
      
    }
    
  }
  
  
  
  #write_xlsx(x =pacientes_nids_difer_uuid_iguais , path = paste0('share/cs_',openmrs.db.name,'_incosistencias_nids_openmrs_idart_uuid_iguais.xlsx') )
  if(length(which(pacientes_nids_difer_uuid_iguais$obs==FALSE))>0){
    write_xlsx(x =pacientes_nids_difer_uuid_iguais[pacientes_nids_difer_uuid_iguais$obs==FALSE,] , path = paste0('share/cs_',openmrs.db.name,'_incosistencias_nids_openmrs_idart_corrigir_manualmente.xlsx') )
  }
  
  
  # pacientes suspeitos que necessitam de verificacao manual no openmrs/iDART
  # (aplicar algoritmo de similaridade de nomes stringdist)
  
  pacientes_nao_encontrados <- subset(pacientes_nao_encontrados, !( id %in% pacientes_nids_difer_uuid_iguais$id) , )
  
  
}

# redundante
#pacientes_nao_encontrados <- filter(pacientes_nao_encontrados, nchar(nid_openmrs)==0)
pacientes_suspeitos <- filter(pacientes_nao_encontrados, ( is.na(uuidopenmrs) & grepl(pattern = 'ferido',x =startreason, ignore.case = TRUE) ) |
                               (!is.na(uuidopenmrs)  &  nid_openmrs=="") |  totaldispensas > 5)

pacientes_suspeitos <- pacientes_suspeitos[grepl(pattern = "/",x = pacientes_suspeitos$patientid)==TRUE,]

por_investigar = pacientes_suspeitos
por_investigar = por_investigar[,c(1:8,13)]
if(dim(por_investigar)[1] > 0) {
  
  por_investigar$full_name <- gsub( pattern = 'NA',replacement = '',x = paste0(por_investigar$firstnames, ' ', por_investigar$lastname) )
  por_investigar$full_name <-  sapply(por_investigar$full_name  ,  removeNumbersFromName)
  por_investigar$string_dist <- ""
  por_investigar$openmrs_uuid <- ""
  por_investigar$identifier <- ""
  por_investigar$given_name <- ""
  por_investigar$middle_name <- ""
  por_investigar$family_name <- ""
  por_investigar$openmrs_full_name <- ""
  por_investigar$estado_tarv <- ""
  
  
  for (i in 1:dim(por_investigar)[1]) {
    nome <- por_investigar$full_name[i]
    
    openmrs_patients$string_dist <-   mapply(getStringDistance, openmrs_patients$full_name_openmrs,  nome)
    
    index = which(as.numeric(openmrs_patients$string_dist)  == as.numeric(min(openmrs_patients$string_dist))  )
    
    if (length(index) > 1) {
      index <- index[1]
    }
    
    por_investigar$uuid[i]           <-          openmrs_patients$uuid[index]
    por_investigar$identifier[i]     <-    openmrs_patients$identifier[index]
    por_investigar$given_name[i]     <-    openmrs_patients$given_name[index]
    por_investigar$middle_name[i]    <-    openmrs_patients$middle_name[index]
    por_investigar$family_name[i]    <-    openmrs_patients$family_name[index]
    por_investigar$openmrs_full_name[i]  <- openmrs_patients$full_name_openmrs[index]
    por_investigar$estado_tarv[i]  <-    openmrs_patients$estado_tarv[index]
    por_investigar$string_dist[i] <-    openmrs_patients$string_dist[index]
    por_investigar$openmrs_uuid[i] <-    openmrs_patients$uuid[index]
    
  }
  
}
temp = por_investigar[, c(1:7,11:18)]
temp$nr_seq <- sapply(temp$patientid,FUN = getNumSeqNid)
temp$decision <- mapply(grepNrSeqNID,temp$nr_seq,temp$identifier)
temp_match <- filter(temp, decision==TRUE ,string_dist <=0.1486, estado_tarv != 'OBITO')


# verificar na bd do idart se os nids equivalentes nao existem no iDART (duplicado)
if (nrow(temp_match)>0) {
  temp_match$duplicated <- ""
  for (v in 1:nrow(temp_match)) {
    identifier <- temp_match$identifier[v]
    if(identifier %in% patients$patientid){
      temp_match$duplicated[v] <- TRUE
      index <- which(identifier==patients$patientid)
    } else {
      temp_match$duplicated[v] <- FALSE
    }
  }
  df_pacientes_gerrar_sql <- subset(temp_match, duplicated=="FALSE" & openmrs_uuid == uuid , )
  if (nrow(df_pacientes_gerrar_sql)>0){
    
    for(i in 1:nrow(df_pacientes_gerrar_sql)){
      
      
      # pacientes da farmac geral um sql para actualizar
      if( df_pacientes_gerrar_sql$startreason[i] %in% c('Referido para outra Farmacia','Voltou da Referencia')){
        
        id <- df_pacientes_gerrar_sql$id[i]
        patientid <- df_pacientes_gerrar_sql$identifier[i]
        uuidopenmrs <- df_pacientes_gerrar_sql$openmrs_uuid[i]
        old_patientid <- df_pacientes_gerrar_sql$patientid[i]
        
        sql_update_pi <- paste0(" update patientidentifier set  value ='",
                                patientid,
                                "' where patient_id = ",id, " ;")
        
        sql_update_patient<- paste0(" update patient set patientid  ='",
                                    patientid,
                                    "' , uuidopenmrs = '",
                                    uuidopenmrs,
                                    "'  where id = ",id, " ;")
        sql_update_packagedrung<- paste0(" update packagedruginfotmp set patientid  ='",
                                         patientid,
                                         "' where patientid ='",old_patientid, "' ;")
        sql_update_sync_temp_patients <- paste0(" update sync_temp_patients set patientid  ='",
                                                patientid,
                                                "' , uuidopenmrs = '",
                                                uuidopenmrs,
                                                "' where patientid ='",old_patientid, "' ;")
        sql_update_sync_temp_dispense <- paste0(" update sync_temp_dispense set patientid  ='",
                                                patientid,
                                                "' , uuidopenmrs = '",
                                                uuidopenmrs,
                                                "' where patientid ='",old_patientid, "' ;")

        
        
        message(sql_update_pi)
        message(sql_update_patient)
        message(sql_update_packagedrung)
        message(sql_update_sync_temp_patients)
        write("-- ------------------------------- Update apartir da aplicacao de algoritmo de proximidade de nomes ----------------------------------------------",file=paste0('share/cs_',openmrs.db.name,"_sql_update_farmac_nid.sql"),append=TRUE)
        write(sql_update_pi,file=paste0('share/cs_',openmrs.db.name,"_sql_update_farmac_nid.sql"),append=TRUE)
        write(sql_update_patient,file=paste0('share/cs_',openmrs.db.name,"_sql_update_farmac_nid.sql"),append=TRUE)
        write(sql_update_packagedrung,file=paste0('share/cs_',openmrs.db.name,"_sql_update_farmac_nid.sql"),append=TRUE)
        write(sql_update_sync_temp_patients,file=paste0('share/cs_',openmrs.db.name,"_sql_update_farmac_nid.sql"),append=TRUE)
        write(sql_update_sync_temp_dispense,file=paste0('share/cs_',openmrs.db.name,"_sql_update_farmac_nid.sql"),append=TRUE)
  
        
        write("-- ------------------------------------------------------------------------------------------------",file=paste0('share/cs_',openmrs.db.name,"_sql_update_farmac_nid.sql"),append=TRUE)
        
      } else {
        id <- df_pacientes_gerrar_sql$id[i]
        patientid <- df_pacientes_gerrar_sql$identifier[i]
        uuidopenmrs <- df_pacientes_gerrar_sql$openmrs_uuid[i]
        old_patientid <- df_pacientes_gerrar_sql$patientid[i]
        
        sql_update_pi <- paste0(" update patientidentifier set  value ='",
                                patientid,
                                "' where patient_id = ",id, " ;")
        
        sql_update_patient<- paste0(" update patient set patientid  ='",
                                    patientid,
                                    "' , uuidopenmrs = '",
                                    uuidopenmrs,
                                    "'  where id = ",id, " ;")
        sql_update_packagedrung<- paste0(" update packagedruginfotmp set patientid  ='",
                                         patientid,
                                         "' where patientid ='",old_patientid, "' ;")
        sql_update_sync<- paste0(" update sync_openmrs_dispense set nid  ='",
                                 patientid,
                                 "' where nid ='",old_patientid, "' ;")
        message(sql_update_pi)
        message(sql_update_patient)
        message(sql_update_packagedrung)
        #message(sql_update_sync_temp_patients)
        write("-- ------------------------------- Update apartir da aplicacao de algoritmo de proximidade de nomes ----------------------------------------------",file=paste0('share/cs_',openmrs.db.name,"_sql_update_farmac_nid.sql"),append=TRUE)
        write(sql_update_pi,file=paste0('share/cs_',openmrs.db.name,"_sql_fix_wrong_nids_querys.sql"),append=TRUE)
        write(sql_update_patient,file=paste0('share/cs_',openmrs.db.name,"_sql_fix_wrong_nids_querys.sql"),append=TRUE)
        write(sql_update_packagedrung,file=paste0('share/cs_',openmrs.db.name,"_sql_fix_wrong_nids_querys.sql"),append=TRUE)
        write(sql_update_sync,file=paste0('share/cs_',openmrs.db.name,"_sql_fix_wrong_nids_querys.sql"),append=TRUE)
        
        write("-- ------------------------------------------------------------------------------------------------",file=paste0('share/cs_',openmrs.db.name,"_sql_fix_wrong_nids_querys.sql"),append=TRUE)
      }
      
      
      
    }
    
    
    temp_match <- subset( temp_match, ! (id %in% df_pacientes_gerrar_sql$id), )
    
    # gerar script para corrigir 
    if(nrow(temp_match)> 1) {
      write_xlsx(x =temp_match , path = paste0('share/cs_',openmrs.db.name,'_pacientes_duplicados_suspeitos_analisar_manualmente.xlsx') )
    }
  }


  
}

#Pacientes duplicados/problemas desconhecidos
patients <- patients %>% filter(! patientid %in% temp_match$patientid)

pacientes_problemas_desconhecidos <- filter(patients, openmrs_status %in% c('Not Ok','Triplicado','Duplicated','Multiplos registos') & !is.na(ult_levant_idart))
# pacientes_problemas_desconhecidos <- left_join(pacientes_problemas_desconhecidos, openmrs_patients, by = c('patientid'='identifier')) %>%
#   select(patientid, firstnames, lastname, full_name_openmrs,  ult_levant_idart,totaldispensas,estado_permanencia, 
#          uuid_api_openmrs,uuid.y, estado_tarv, data_estado,data_ult_consulta,data_prox_marcado,obs,duplicado_openmrs)
pacientes_problemas_desconhecidos <- arrange(pacientes_problemas_desconhecidos, desc(ult_levant_idart))


pacientes_duplicados   <- pacientes_problemas_desconhecidos %>% filter(openmrs_status=='Duplicated')
pacientes_triplicados  <- pacientes_problemas_desconhecidos %>% filter(openmrs_status=='Triplicado' | openmrs_status=='Multiplos registos' )
pacientes_uuid_diferente <- pacientes_problemas_desconhecidos %>% filter(obs=='uuid_diferente')

#other_patients <- pacientes_problemas_desconhecidos %>% filter(openmrs_status=='Not Ok')

write_xlsx(x = arrange(pacientes_duplicados, desc(ult_levant_idart)), 
           path = paste0('share/cs_',openmrs.db.name,'_pacientes_duplicados_corrigir_manualmente.xlsx'))
write_xlsx(x = arrange(pacientes_triplicados, desc(ult_levant_idart)), 
           path = paste0('share/cs_',openmrs.db.name,'_pacientes_triplicados_corrigir_manualmente.xlsx'))
write_xlsx(x = arrange(pacientes_uuid_diferente, desc(ult_levant_idart)), 
           path = paste0('share/cs_',openmrs.db.name,'_pacientes_problemas_uuid_ou_nid_corrigir_manualmente.xlsx'))

# Gera estatisticas
unidade_sanitaria <-  openmrs.db.name
total_saidas_com_consultas_recentes <- c(nrow(estado_permanencia_incosistente))
total_duplicados_openmrs <- c(nrow(pacientes_duplicados))
total_triplicados_openmrs <- c(nrow(pacientes_triplicados))
total_nids_diferentes_uuid_iguais  <- c(nrow(pacientes_nids_difer_uuid_iguais))
total_nids_diferentes_nomes_nids_semelhantes  <- c(nrow(df_pacientes_gerrar_sql))
total_problemas_desconhecidos <- c(nrow(temp_match))
total_uuid_diferente   <- c(nrow(pacientes_uuid_diferente))


# empty temporary df

# Final df 
if (file.exists("df_estatistica.RData")){
  load(file = "df_estatistica.RData")
  df_estatistica_temp <-  data.frame(unidade_sanitaria,total_saidas_com_consultas_recentes,total_duplicados_openmrs,total_triplicados_openmrs,total_nids_diferentes_uuid_iguais,total_nids_diferentes_nomes_nids_semelhantes,
                                     total_problemas_desconhecidos,total_uuid_diferente)
  df_estatistica <- plyr::rbind.fill(df_estatistica, df_estatistica_temp)
  save(df_estatistica,file = "df_estatistica.RData")
} else { 
  df_estatistica <-  data.frame(unidade_sanitaria,total_saidas_com_consultas_recentes,total_duplicados_openmrs,total_triplicados_openmrs,total_nids_diferentes_uuid_iguais,total_nids_diferentes_nomes_nids_semelhantes,
                                     total_problemas_desconhecidos,total_uuid_diferente)
  save(df_estatistica,file = "df_estatistica.RData")
  }
  
  

dbDisconnect(conn = con_openmrs)

}







