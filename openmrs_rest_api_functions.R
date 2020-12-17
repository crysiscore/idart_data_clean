

#' checkPatientUuidExistsOpenMRS ->  verifica se existe um paciente no openmrs com um det. uuidopenmrs
#' @param jdbc.properties  [urlBase,urlBaseReportingRest,location,hibernateOpenMRSConnectionUrl,hibernateOpenMRSPassword,hibernateOpenMRSUsername]
#' @return  TRUE/FALSE 
#' @examples 
#' status  <- checkPatientUuidExistsOpenMRS(jdbc.properties, c("0102010001/2006/04892","julia" ,"jaoquina", "cb90174b-81e9-43e4-9b4d-dc09d2966efa"))
#' 

checkPatientUuidExistsOpenMRS <- function(jdbc.properties, patient) {
  # url da API
  base.url.rest <- as.character(jdbc.properties$urlBaseReportingRest)
  base.url <-  as.character(jdbc.properties$urlBase)
  status <- TRUE
  url.check.patient <- paste0(base.url,'person/',patient[4])
  
  r <- content(GET(url.check.patient, authenticate('admin', 'eSaude123')), as = "parsed")
  
  if("error" %in% names(r)){
    if(r$error$message =="Object with given uuid doesn't exist" ){
    }
    status<-FALSE
    return(FALSE)
    
  } else{
    
    return(status)
    
  }
  
  return(status)
}


#' ReadJdbcProperties ->  carrega os paramentros de conexao no ficheiro jdbc.properties
#' @param file  patg to file
#' @return  vec [urlBase,urlBaseReportingRest,location,hibernateOpenMRSConnectionUrl,hibernateOpenMRSPassword,hibernateOpenMRSUsername]
#' @examples 
#' user_admin  <- ReadJdbcProperties(file)
#' 

readJdbcProperties <- function(file='jdbc.properties') {
  
  
  vec <- as.data.frame(read.properties(file = file ))
  vec
}



#' apiGetPatientByNid ->  verifica se existe um paciente no openmrs com um det. nid
#' @param jdbc.properties  [urlBase,urlBaseReportingRest,location,hibernateOpenMRSConnectionUrl,hibernateOpenMRSPassword,hibernateOpenMRSUsername]
#' @return  TRUE/FALSE 
#' @examples  /openmrs/ws/rest/v1/patient?q=
#' status  <- checkPatientUuidExistsOpenMRS(jdbc.properties, c("0102010001/2006/04892","julia" ,"jaoquina", "cb90174b-81e9-43e4-9b4d-dc09d2966efa"))
#' 

apiGetPatientByNid <- function(jdbc.properties, patientid ) {
  # url da API
  base.url.rest <- as.character(jdbc.properties$urlBaseReportingRest)
  base.url <-  as.character(jdbc.properties$urlBase)
  url.check.patient <- paste0(base.url,'patient?q=' ,patientid[2])
  openmrsuuid <- ""
  
  r <- content(GET(url.check.patient, authenticate('admin', 'eSaude123')), as = "parsed")
  
  return(r)
}

#' apiGetPatientByUuid ->  verifica se existe um paciente no openmrs com um det. nid
#' @param jdbc.properties  [urlBase,urlBaseReportingRest,location,hibernateOpenMRSConnectionUrl,hibernateOpenMRSPassword,hibernateOpenMRSUsername]
#' @return  TRUE/FALSE 
#' @examples  /openmrs/ws/rest/v1/patient?q=
#' status  <- checkPatientUuidExistsOpenMRS(jdbc.properties, c("0102010001/2006/04892","julia" ,"jaoquina", "cb90174b-81e9-43e4-9b4d-dc09d2966efa"))
#' 

apiGetPatientByUuid <- function(jdbc.properties, patient ) {
  # url da API
  base.url.rest <- as.character(jdbc.properties$urlBaseReportingRest)
  base.url <-  as.character(jdbc.properties$urlBase)
  url.check.patient <- paste0(base.url,'patient/' ,patient[4])
  
  r <- content(GET(url.check.patient, authenticate('admin', 'eSaude123')), as = "parsed")
  
  return(r)
}

#' apiGetPatientByName ->  verifica se existe um paciente no openmrs com um det. nome
#' @param jdbc.properties  [urlBase,urlBaseReportingRest,location,hibernateOpenMRSConnectionUrl,hibernateOpenMRSPassword,hibernateOpenMRSUsername]
#' @return  TRUE/FALSE 
#' @examples  /openmrs/ws/rest/v1/patient?q=
#' status  <- checkPatientUuidExistsOpenMRS(jdbc.properties, c("0102010001/2006/04892","julia" ,"jaoquina", "cb90174b-81e9-43e4-9b4d-dc09d2966efa"))
#' 
apiGetPatientByName <- function(jdbc.properties, patient.full.name ) {
  # url da API
  base.url.rest <- as.character(jdbc.properties$urlBaseReportingRest)
  base.url <-  as.character(jdbc.properties$urlBase)
  url.check.patient <- paste0(base.url,'patient?q=' ,gsub(pattern = ' ', replacement = '%20' ,x =patient.full.name,ignore.case = TRUE ))
  
  r <- content(GET(url.check.patient, authenticate('admin', 'eSaude123')), as = "parsed")
  
  return(r)
}


#' composePatientToCheck --> Compoe um vector com dados do paciente =
#' 
#' @param df tabela de duplicados para extrair os dados do Pat
#' @param index row do paciente em causa 
#' @return vector[id,uuid,patientid,openmrs_patient_id,full.name,index] 
#' @examples pat <-  composePatientToCheck(k, df.different_uuid)
#' 
composePatientToCheck <- function(index,df){
  
  id = df$id[index]
  patientid = df$patientid[index]
  uuid_idart = df$uuididart[index]
  uuid_openmrs =df$uuidopenmrs[index]
  full_name =  df$idart_full_name[index]
  
  Encoding(full_name) <- "latin1"
  
  full_name  <- iconv(full_name, "latin1", "UTF-8",sub='')
  full_name  <- gsub(pattern = '  ' ,replacement = ' ', x = full_name)
  patientid <- gsub(pattern = ' ', replacement = '', x = patientid)
  patientid <- gsub(pattern = '\t', replacement = '', x = patientid)
  patient <- c(id,patientid,uuid_idart,uuid_openmrs,full_name)
  return(patient)
}





apiCheckEstadoPermanencia <- function(jdbc.properties, patient.uuid ) {
  # url da API
  url.base.reporting.rest<- as.character(jdbc.properties$urlBaseReportingRest)
  #base.url <-  as.character(jdbc.properties$urlBase)
  url.check.patient <- paste0(url.base.reporting.rest,'?personUuid=' ,patient.uuid)
  
  r <- content(GET(url.check.patient, authenticate('admin', 'eSaude123')), as = "parsed")
  
  return(r)
}

