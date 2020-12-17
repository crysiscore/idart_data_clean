# pacientes com mesmo nid mas uuid e diferente idartuuid != personuuid
library(properties)
library(httr)
library(tibble)
library(writexl)

# Verifica e actualiza pacientes que tem dados no iDART diferentes do OpenMRS
# ******** Configure para o dir onde deixou os ficheiros necessarios para executar o programa ****

#wd <- '~/Git/iDART/data-clean/'
wd <- '/home/ccsadmin/R/projects/iDart/data_clean_altomae'

# Limpar o envinronment
rm(list=setdiff(ls(), c("wd","tipo_nid")))

if (dir.exists(wd)){
  
  setwd(wd)  
  source('paramConfiguration.R')
  source('openmrs_rest_api_functions.R')
  source('genericFunctions.R')
  
} else {
  
  message( paste0('O Directorio ', wd, ' nao existe, por favor configure corectamente o dir'))
  

}

us_name = 'cs_altomae'
patients$uuid_api_openmrs <- ""
patients$obs <- ""
patients$openmrs_status <- ""
patients$openmrs_full_name <- ""
patients$openmrs_full_name_pat_2 <- ""
patients$openmrs_full_name_pat_3 <- ""
patients$duplicado_openmrs<- ""
patients$stringdist_pat1 <- "" 
patients$stringdist_pat2 <- ""
patients$stringdist_pat3 <- ""
patients$nid_openmrs <- ""
patients$estado_permanencia <- ""
jdbc_properties <- readJdbcProperties(file = 'jdbc.properties')
  
for (k in 1:nrow(patients)) {
    
    patient <- composePatientToCheck(k,patients)
    # se a api nao retornar nada este nid nao existe
    df_openmrs_pat <- apiGetPatientByNid( jdbc_properties, patientid = patient)
    
    if(is.null(df_openmrs_pat) || names(df_openmrs_pat)=="error"){
      patients$openmrs_status[k] <- 'Not found'
      patients$obs[k]  <- 'Reason Unknown'
    }
    
    else if(length(df_openmrs_pat$results)==0){
      
      message(paste0(" NID :", patient[2], " nao existe no openmrs"))
      patients$obs[k]  <- paste0("NID :", patient[2], " nao existe no openmrs")
      patients$openmrs_status[k] <- 'Not found'
      message(" trying search by uuid...")
      status <- checkPatientUuidExistsOpenMRS( jdbc_properties,  patient)
      if(status){
        
        df_openmrs_pat <- apiGetPatientByUuid(jdbc_properties,patient)
        if(length(df_openmrs_pat)> 0){
          
          if(names(df_openmrs_pat)!= "error"){
            message(paste0("found uuid", df_openmrs_pat$uuid))
            patients$duplicado_openmrs[k] <- 'No'
            patients$uuid_api_openmrs[k]  <- df_openmrs_pat$uuid
            patients$obs[k] <- paste0("NID :", patient[2], " Nao existe no openmrs, mas o uuid existe")
            patients$openmrs_full_name[k]  <- df_openmrs_pat$person$display
            index_f_name_start <- stri_locate_last(str = df_openmrs_pat$display, regex = "-" )
            pat_nid <- substr(df_openmrs_pat$display, 0, index_f_name_start - 2)
            patients$nid_openmrs[k] <- pat_nid
          }
          
        }

       } else {message("not found... skip to next patient")} 
        
      }
    else  if(length(df_openmrs_pat$results)==1){
      
      patients$openmrs_status[k] <- 'Ok'
      patients$duplicado_openmrs[k] <- 'No'
      patients$uuid_api_openmrs[k] <- df_openmrs_pat$results[[1]]$uuid
      if(is.na(patients$uuidopenmrs[k])){
        patients$obs[k] <- 'uuid_diferente'
        patients$openmrs_status[k] <- 'Not Ok'
       }
      else {
        if( patients$uuid_api_openmrs[k] != patients$uuidopenmrs[k]){
          patients$obs[k] <- 'uuid_diferente'
          patients$openmrs_status[k] <- 'Not Ok'
          index_f_name_start <- stri_locate_last(str =df_openmrs_pat$results[[1]]$display, regex = "-" )
          pat_name <- substr(df_openmrs_pat$results[[1]]$display, index_f_name_start+2, nchar(df_openmrs_pat$results[[1]]$display))
          patients$openmrs_full_name [k]  <- pat_name
          
        
        }  else {
          patient_estado <- apiCheckEstadoPermanencia(jdbc_properties,df_openmrs_pat$results[[1]]$uuid)
          members <- length(patient_estado$members)
          if(members < 1){
            patients$estado_permanencia[k] <- "ABANDONO/TRANSFERIDO PARA/NAO REGISTADO"
          } else {
            patients$estado_permanencia[k] <- "ACTIVO NO PROGRAMA"
          }
          
          
        }
      
      } 

      
      
    } 
    else  if(length(df_openmrs_pat$results)==2){
      
      
      patients$duplicado_openmrs[k] <- 'Yes'
      patients$openmrs_status[k] <- 'Duplicated'
      index_f_name_start <- stri_locate_last(str =df_openmrs_pat$results[[1]]$display, regex = "-" )
      index_s_name_start <- stri_locate_last(str =df_openmrs_pat$results[[2]]$display, regex = "-" )
      pat_name_1 <- substr(df_openmrs_pat$results[[1]]$display, index_f_name_start+2, nchar(df_openmrs_pat$results[[1]]$display))
      pat_name_2 <- substr(df_openmrs_pat$results[[2]]$display, index_s_name_start+2, nchar(df_openmrs_pat$results[[2]]$display))
      
      patients$stringdist_pat1[k] <- getStringDistance(string1 = patient[5],string2 = pat_name_1)
      patients$stringdist_pat2[k] <- getStringDistance(string1 = patient[5], string2 = pat_name_2)
      patients$openmrs_full_name[k] <- pat_name_1
      patients$openmrs_full_name_pat_2[k] <- pat_name_2
      
      vec <- c(patients$stringdist_pat1[k],patients$stringdist_pat2[k])
      
      index <- which( vec == min(vec) )
      
      if(index==2){
      
        patients$uuid_api_openmrs[k]  <- df_openmrs_pat$results[[2]]$uuid
        patients$obs[k] <- paste0(pat_name_2, " -  nome mais semelhante (string dis algorithm)")
        
      }else{
        
        patients$uuid_api_openmrs[k]  <- df_openmrs_pat$results[[1]]$uuid
        patients$obs[k] <- paste0(pat_name_1, " -  nome mais semelhante (Levenshtein algorithm)")
        
      }
        
    }
    else if(length(df_openmrs_pat$results)==3){
      
      
      patients$duplicado_openmrs[k] <- 'Yes'
      patients$openmrs_status[k] <- 'Triplicado'
      
      index_f_name_start <- stri_locate_last(str =df_openmrs_pat$results[[1]]$display, regex = "-" )
      index_s_name_start <- stri_locate_last(str =df_openmrs_pat$results[[2]]$display, regex = "-" )
      index_t_name_start <- stri_locate_last(str =df_openmrs_pat$results[[3]]$display, regex = "-" )
      
      pat_name_1 <- substr(df_openmrs_pat$results[[1]]$display, index_f_name_start+2, nchar(df_openmrs_pat$results[[1]]$display))
      pat_name_2 <- substr(df_openmrs_pat$results[[2]]$display, index_s_name_start+2, nchar(df_openmrs_pat$results[[2]]$display))
      pat_name_3 <- substr(df_openmrs_pat$results[[3]]$display, index_t_name_start+2, nchar(df_openmrs_pat$results[[3]]$display))
      
      patients$stringdist_pat1[k] <- getStringDistance(string1 = patient[5],string2 = pat_name_1)
      patients$stringdist_pat2[k] <- getStringDistance(string1 = patient[5], string2 = pat_name_2)
      patients$stringdist_pat3[k] <- getStringDistance(string1 = patient[5], string2 = pat_name_3)
      
      vec_str_dist <-c(patients$stringdist_pat1[k],patients$stringdist_pat2[k],patients$stringdist_pat3[k]) 
      
      index = which(vec_str_dist == min(vec_str_dist))
      if(length(index)==1){
        
        if(index==1){
          patients$uuid_api_openmrs[k]  <- df_openmrs_pat$results[[1]]$uuid
          patients$obs[k] <- paste0(pat_name_1, " -  nome mais semelhante (string dis algorithm)")
        } else if(index==2){
          patients$uuid_api_openmrs[k]  <- df_openmrs_pat$results[[2]]$uuid
          patients$obs[k] <- paste0(pat_name_2, " -  nome mais semelhante (string dis algorithm)")
        }else if(index==3){
          patients$uuid_api_openmrs[k]  <- df_openmrs_pat$results[[3]]$uuid
          patients$obs[k] <- paste0(pat_name_3, " -  nome mais semelhante (string dis algorithm)")
        }
        
        
      } else {  # pacientes duplicados nid nome no openmrs # resolver manualemnte
        
        patients$obs[k] <- paste0("Paciente ",pat_name_1,' - ',pat_name_1,' - ', pat_name_1, " Triplicads nid nome no openmrs, corrigir manualmente ")
      }
      
      
    }
    else{
      message(paste0(" Paciente 4x NID  no openmrs"))
      patients$openmrs_status[k] <- 'Multiplos registos'
      patients$duplicado_openmrs[k] <- 'Yes'
      patients$obs[k] <- paste0("Paciente com 4x  nid  no openmrs, corrigir manualmente NID:", patient[2])
    }
  }
  
save(patients, file = paste0('data/patients_',us_name,'.RData'))

