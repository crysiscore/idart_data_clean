
library(purrr)
library(stringi)
library(dplyr)
library(writexl)
library(RMySQL)
library(stringdist)
library(stringr)
source('genericFunctions.R')


## OpenMRS  - Configuracao de variaveis de conexao 
openmrs.user ='esaude'                         # ******** modificar
openmrs.password='password'                      # ******** modificar
openmrs.db.name='openmrs'                      # ******** modificar
openmrs.host='192.168.2.160'                    # ******** modificar
openmrs.port=5457                              # ******** modificar

# Objecto de connexao com a bd openmrs
con_openmrs = dbConnect(MySQL(), user=openmrs.user, password=openmrs.password, dbname=openmrs.db.name, host=openmrs.host, port=openmrs.port)


## Pacientes
## Buscar todos pacientes OpenMRS & iDART
openmrs_patients <- getAllPatientsOpenMRS(con_openmrs)


pacientes_dados_consistentes <- filter(patients, openmrs_status=='Ok' & estado_permanencia=='ACTIVO NO PROGRAMA' 
                                       & duplicado_openmrs=='No' & uuidopenmrs==uuid_api_openmrs)

patients <- patients %>% filter(! id %in% pacientes_dados_consistentes$id)

## Pacientes Nao activos no OpenMRS( pelo API)
pacientes_nao_activos <- filter(patients, openmrs_status=='Ok' &  estado_permanencia =='ABANDONO/TRANSFERIDO PARA/NAO REGISTADO')

## Pacientes com estado permanencia TARV ='ABANDONO/TRANSFERIDO PARA/NAO REGISTADO' mas com consultas recentes
estado_permanencia_incosistente <- left_join(pacientes_nao_activos, openmrs_patients, by = c('patientid'='identifier')) %>%
  filter(  ( !is.na(data_ult_consulta) &  !is.na(data_prox_marcado) & data_ult_consulta > data_estado & data_prox_marcado > '2020-09-21' ) 
           | (is.na(data_estado) & !is.na(data_ult_consulta)) | (!is.na(ult_levant_idart) & is.na(data_estado))) %>% 
  select(patientid, firstnames, lastname, full_name_openmrs,  ult_levant_idart,estado_permanencia, 
   uuid_api_openmrs,uuidopenmrs, estado_tarv, data_estado,data_ult_consulta,data_prox_marcado)

write_xlsx(x =estado_permanencia_incosistente , path = paste0('share/cs_',openmrs.db.name,'_estado_permanencia_incosistente_corrigir_manualmente.xlsx') )

patients <- patients %>% filter(! patientid %in% estado_permanencia_incosistente$patientid)

# Pacientes nao encontrados no OpenMRS por NID (API request)
pacientes_nao_encontrados <- filter(patients, openmrs_status=='Not found')
pacientes_nao_encontrados <- pacientes_nao_encontrados %>% select(id, patientid,dateofbirth, firstnames,lastname,startreason,totaldispensas,ult_levant_idart,uuidopenmrs,uuid_api_openmrs,
                          openmrs_status,nid_openmrs,openmrs_full_name)

# Pacientes nao encontrados no OpenMRS por NID (API request) mas existem com o mesmo uuidopenmrs
pacientes_nids_difer_uuid_iguais <- filter (pacientes_nao_encontrados, nchar(nid_openmrs)>0)
# verificar a semelhanca de NIDs OpenMRS/iDART
# TRUE/FALSE na coluna OBS
pacientes_nids_difer_uuid_iguais$obs <- map2_lgl(pacientes_nids_difer_uuid_iguais$patientid,pacientes_nids_difer_uuid_iguais$nid_openmrs,.f =checkNidSimilarity )

# verifica a semalhanca de nomes para pacientes cujo NID nao e semelhante
# TRUE/FALSE na coluna OBS
for (v in 1:nrow(pacientes_nids_difer_uuid_iguais)) {
  if(!pacientes_nids_difer_uuid_iguais$obs[v]){
    x <- pacientes_nids_difer_uuid_iguais$firstnames[v]
    y <- pacientes_nids_difer_uuid_iguais$openmrs_full_name[v] 
    pacientes_nids_difer_uuid_iguais$obs[v] <-checkNameSimilarity(x,y)
  }
  
}

pacientes_nids_difer_uuid_iguais  <- filter(pacientes_nids_difer_uuid_iguais,patientid != nid_openmrs)
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
                              "' where value = '",old_patientid, "' ;")
      sql_update_patient<- paste0(" update patient set patientid  ='",
                                  patientid,
                                  "' where patientid = '",old_patientid, "' ;")
      sql_update_packagedrung<- paste0(" update packagedruginfotmp set patientid  ='",
                                       patientid,
                                       "' where patientid ='",old_patientid, "' ;")
      
      message(sql_update_pi)
      message(sql_update_patient)
      message(sql_update_packagedrung)
      write("-- ------------------------------------------------------------------------------------------------",file=paste0('share/cs_',openmrs.db.name,"_sql_update_farmac_nid.sql"),append=TRUE)
      write(sql_update_pi,file=paste0('share/cs_',openmrs.db.name,"_sql_update_farmac_nid.sql"),append=TRUE)
      write(sql_update_patient,file=paste0('share/cs_',openmrs.db.name,"_sql_update_farmac_nid.sql"),append=TRUE)
      write(sql_update_packagedrung,file=paste0('share/cs_',openmrs.db.name,"_sql_update_farmac_nid.sql"),append=TRUE)
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
                                  "' where id =",id, " ;")
      sql_update_packagedrung<- paste0(" update packagedruginfotmp set patientid  ='",
                                       patientid,
                                       "' where patientid ='",old_patientid, "' ;")
      
      message(sql_update_pi)
      message(sql_update_patient)
      message(sql_update_packagedrung)
      write("-- ------------------------------------------------------------------------------------------------",file=paste0('share/cs_',openmrs.db.name,"_sql_fix_wrong_nids_querys.sql"),append=TRUE)
      write(sql_update_pi,file=paste0('share/cs_',openmrs.db.name,"_sql_fix_wrong_nids_querys.sql"),append=TRUE)
      write(sql_update_patient,file=paste0('share/cs_',openmrs.db.name,"_sql_fix_wrong_nids_querys.sql"),append=TRUE)
      write(sql_update_packagedrung,file=paste0('share/cs_',openmrs.db.name,"_sql_fix_wrong_nids_querys.sql"),append=TRUE)
      write("-- ------------------------------------------------------------------------------------------------",file=paste0('share/cs_',openmrs.db.name,"_sql_fix_wrong_nids_querys.sql"),append=TRUE)
    }
    
   
    
  }
  
}



write_xlsx(x =pacientes_nids_difer_uuid_iguais , path = paste0('share/cs_',openmrs.db.name,'_incosistencias_nids_openmrs_idart_uuid_iguais.xlsx') )
if(length(which(pacientes_nids_difer_uuid_iguais$obs==FALSE))>0){
  write_xlsx(x =pacientes_nids_difer_uuid_iguais[pacientes_nids_difer_uuid_iguais$obs==FALSE,] , path = paste0('share/cs_',openmrs.db.name,'_incosistencias_nids_openmrs_idart_uuid_iguais_nomes_dif_corrigir_manualmente.xlsx') )
}


# Pacientes nao encontrados no OpenMRS por NID (API request) mas existem com o mesmo uuidopenmrs
pacientes_nao_encontrados <- filter(pacientes_nao_encontrados, nchar(nid_openmrs)==0)

# pacientes suspeitos que necessitam de verificacao manual no openmrs/iDART
# (aplicar algoritmo de similaridade de nomes stringdist)
pacientes_suspeitos <- filter(pacientes_nao_encontrados, ( is.na(uuidopenmrs) & grepl(pattern = 'ferido',x =startreason, ignore.case = TRUE) ) |
                               (!is.na(uuidopenmrs)  &  nid_openmrs=="" |  totaldispensas > 5))

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
temp = por_investigar[, c(1:5,6:7,11:18)]
temp$nr_seq <- sapply(temp$patientid,FUN = getNumSeqNid)
temp$decision <- mapply(grepNrSeqNID,temp$nr_seq,temp$identifier)
temp_match <- filter(temp, decision==TRUE ,string_dist <=0.1486, estado_tarv != 'OBITO')
temp_match$duplicated <- ""

# verificar na bd do idart se os nids equivalentes nao existem no iDART (duplicado)
if (nrow(temp_match)>0) {
  
  for (v in 1:nrow(temp_match)) {
    identifier <- temp_match$identifier[v]
    if(identifier %in% patients$patientid){
      temp_match$duplicated[v] <- TRUE
      index <- which(identifier==patients$patientid)
      
    } else {
      temp_match$duplicated[v] <- FALSE
    }
    
  }
  
  write_xlsx(x =pacientes_suspeitos , path = paste0('share/cs_',openmrs.db.name,'_pacientes_suspeitos_analisar_manualmente.xlsx') )
  
  
  
}

#Pacientes duplicados/problemas desconhecidos
pacientes_problemas_desconhecidos <- filter(patients, openmrs_status %in% c('Not Ok','Triplicado','Duplicated','Multiplos registos') & !is.na(ult_levant_idart))
# pacientes_problemas_desconhecidos <- left_join(pacientes_problemas_desconhecidos, openmrs_patients, by = c('patientid'='identifier')) %>%
#   select(patientid, firstnames, lastname, full_name_openmrs,  ult_levant_idart,totaldispensas,estado_permanencia, 
#          uuid_api_openmrs,uuid.y, estado_tarv, data_estado,data_ult_consulta,data_prox_marcado,obs,duplicado_openmrs)
pacientes_problemas_desconhecidos <- arrange(pacientes_problemas_desconhecidos, desc(ult_levant_idart))

write_xlsx(x = arrange(pacientes_problemas_desconhecidos, desc(ult_levant_idart)), 
           path = paste0('share/cs_',openmrs.db.name,'_pacientes_duplicados_triplicados_corrigir_manualmente.xlsx'))


