# Packages que contem algumas funcoes a serem usadas. Deve-se  garantir que tem todos os packages instalados.
# Para instalar deve: ligar o pc a net e  na consola digitar a instrucao -  ex: install.packages("plyr") depois install.packages("stringi ") assim sucessivamente
require(RMySQL)
require(plyr)    
require(stringi)
require(RPostgreSQL)
require(stringr)
require(tidyr)
require(stringdist)
require(dplyr)  
require(writexl)
####################################### Configuracao de Parametros  ##########################################################################
###############################################################################################################################################

postgres.user ='postgres'                      # ******** modificar
postgres.password='iD@rt2020'                   # ******** modificar
postgres.db.name='altomae'                  # ******** modificar
postgres.host='192.168.1.163'                     # ******** modificar
postgres.port=5432                             # ******** modificar


## OpenMRS  - Configuracao de variaveis de conexao 
openmrs.user ='esaude'                         # ******** modificar
openmrs.password='esaude'                      # ******** modificar
openmrs.db.name='altomae'                      # ******** modificar
openmrs.host='192.168.1.10'                    # ******** modificar
openmrs.port=3306                              # ******** modificar

####################################### Final da config  de Parametros  ########################################################################
################################################################################################################################################

source('genericFunctions.R')             ## Carregar funcoes

  # Objecto de connexao com a bd openmrs postgreSQL
  con_postgres <-  dbConnect(PostgreSQL(),user = postgres.user,password = postgres.password, dbname = postgres.db.name,host = postgres.host)
  idartAllPatients <- getAllPatientsIdart(con_postgres)
  #idartAllPatients$patientidSemLetras <- sapply(idartAllPatients$patientid, removeLettersFromNid)    
  
  
  
  por_investigar <-  idartAllPatients[which(  !idartAllPatients$startreason %in%  c('Transito','Em Transito','Paciente em Transito',' Inicio na maternidade')),]
  dfTemp <-  por_investigar[which(grepl(pattern = "TR", ignore.case = TRUE,x = por_investigar$patientid ) == TRUE ),]
  dfTemp_2 <- por_investigar[which( grepl( pattern = "VIS",ignore.case = TRUE,x = por_investigar$patientid ) == TRUE  ),]
  dfTemp_3 <- por_investigar[which(grepl(  pattern = "VIA", ignore.case = TRUE,x = por_investigar$patientid ) == TRUE ),]
  dfTemp_4<-  por_investigar[which(grepl(pattern = "T", ignore.case = TRUE,x = por_investigar$patientid ) == TRUE ),]
  dfTemp_5<-  por_investigar[which(grepl(pattern = "CRAM", ignore.case = TRUE,x = por_investigar$patientid ) == TRUE ),]
  dfTemp_6<-  por_investigar[which(grepl(pattern = "socor", ignore.case = TRUE,x = por_investigar$patientid ) == TRUE ),]
  dfTemp_7<-  por_investigar[which(grepl(pattern = "SOCOR", ignore.case = TRUE,x = por_investigar$patientid ) == TRUE ),]
  por_investigar <-  por_investigar[which(!por_investigar$patientid %in% dfTemp$patientid),]
  por_investigar <-  por_investigar[which(!por_investigar$patientid %in% dfTemp_2$patientid),]
  por_investigar <-  por_investigar[which(!por_investigar$patientid %in% dfTemp_3$patientid),]
  por_investigar <-  por_investigar[which(!por_investigar$patientid %in% dfTemp_4$patientid),]
  por_investigar <-  por_investigar[which(!por_investigar$patientid %in% dfTemp_5$patientid),]
  por_investigar <-  por_investigar[which(!por_investigar$patientid %in% dfTemp_6$patientid),]
  por_investigar <-  por_investigar[which(!por_investigar$patientid %in% dfTemp_7$patientid),]
  rm(dfTemp_2, dfTemp,dfTemp_3,dfTemp_4,dfTemp_5,dfTemp_6,dfTemp_7)
  
  
  #Remover pacientes sem dispensa, provavelmente sejam transitos/abandonos/obitos/transferidos/para   ########
  por_investigar$totaldispensas[which(is.na(por_investigar$totaldispensas))] <- 0 
  #por_investigar <- por_investigar[which(por_investigar$totaldispensas>1 ),]
  patients <- por_investigar
  rm(por_investigar)
  
  
  
  # Objecto de connexao com a bd openmrs
  con_openmrs = dbConnect(MySQL(), user=openmrs.user, password=openmrs.password, dbname=openmrs.db.name, host=openmrs.host, port=openmrs.port)
  
  ## Pacientes
  ## Buscar todos pacientes OpenMRS & iDART
  openmrs_patients <- getAllPatientsOpenMRS(con_openmrs)
  
  