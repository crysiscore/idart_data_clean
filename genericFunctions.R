  
  #' Busca todos pacientes do OpenMRS
  #' 
  #' @param con.postgres  obejcto de conexao com BD OpenMRS
  #' @return tabela/dataframe/df com todos paciente do OpenMRS 
  #' @examples patients_idart <- getAllPatientsIdart(con_openmrs)
  getAllPatientsOpenMRS <- function(con.openmrs) {
    rs  <-
      dbSendQuery(
        con.openmrs,
        paste0(
          "
          SELECT   
          pat.patient_id, 
          pid.identifier , 
          pe.uuid,
           pn.given_name  given_name ,
           pn.middle_name middle_name,
           pn.family_name  family_name,
          concat(lower(pn.given_name),if(lower(pn.middle_name) is not null,concat(' ', lower(pn.middle_name) ) ,''),concat(' ',lower(pn.family_name)) ) as full_name_openmrs ,
          pe.birthdate,
          estado.estado as estado_tarv ,
          max(estado.start_date) data_estado,
          date(visita.encounter_datetime) as data_ult_consulta,
          date(visita.value_datetime) as data_prox_marcado
          FROM  patient pat INNER JOIN  patient_identifier pid ON pat.patient_id =pid.patient_id and pat.voided=0 
          INNER JOIN person pe ON pat.patient_id=pe.person_id and pe.voided=0 
          INNER JOIN person_name pn ON pe.person_id=pn.person_id and    pn.voided=0
          LEFT JOIN
        		(
        			SELECT 	pg.patient_id,ps.start_date encounter_datetime,location_id,ps.start_date,ps.end_date,
        					CASE ps.state
                                WHEN 6 THEN 'ACTIVO NO PROGRAMA'
        						WHEN 7 THEN 'TRANSFERIDO PARA'
        						WHEN 8 THEN 'SUSPENSO'
        						WHEN 9 THEN 'ABANDONO'
        						WHEN 10 THEN 'OBITO'
                                WHEN 29 THEN 'TRANSFERIDO DE'
        					ELSE 'OUTRO' END AS estado
        			FROM 	patient p
        					INNER JOIN patient_program pg ON p.patient_id=pg.patient_id
        					INNER JOIN patient_state ps ON pg.patient_program_id=ps.patient_program_id
        			WHERE 	pg.voided=0 AND ps.voided=0 AND p.voided=0 AND
        					pg.program_id=2 AND ps.state IN (6,7,8,9,10,29) AND ps.end_date IS NULL
        
        
        		) estado ON estado.patient_id=pe.person_id
         LEFT Join
             (
        		Select ult_levantamento.patient_id,ult_levantamento.encounter_datetime,o.value_datetime
        		from
        
        			(	select 	p.patient_id,max(encounter_datetime) as encounter_datetime
        				from 	encounter e
        						inner join patient p on p.patient_id=e.patient_id
        				where 	e.voided=0 and p.voided=0 and e.encounter_type in (6,9)
        				group by p.patient_id
        			) ult_levantamento
        			left join encounter e on e.patient_id=ult_levantamento.patient_id
        			left join obs o on o.encounter_id=e.encounter_id
        			where o.concept_id=1410 and o.voided=0 and e.encounter_datetime=ult_levantamento.encounter_datetime and
        			e.encounter_type in (6,9)
        		) visita  on visita.patient_id=pn.person_id
        
            group by pat.patient_id order by     pat.patient_id
        
                "
        )
      )
    
    data <- fetch(rs, n = -1)
    RMySQL::dbClearResult(rs)
    return(data)
    
  }
  
  
  #' Busca todos pacientes do iDART
  #' 
  #' @param con.postgres  obejcto de conexao com BD iDART
  #' @return tabela/dataframe/df com total de  lev por paciente
  #' @examples total_dispensas <- getTotalDeDispensas(con_idart)
  getAllPatientsIdart <- function(con.postgres) {
    
    
    patients  <-
      dbGetQuery(
        con.postgres,
        paste0(
          "select pat.id, pat.patientid,dateofbirth::TIMESTAMP::DATE as dateofbirth,lower(pat.firstnames) as firstnames , 
          pat.sex, lower(pat.lastname) as lastname ,pat.uuid,pat.uuidopenmrs, ep.startreason,
          dispensas.total as totalDispensas , ult_lev.ult_levant_idart
          from patient pat left join
          (
           select patient, max(startdate), startreason
             from episode
              group by patient, startreason
  
          )  ep on ep.patient = pat.id
  
          left join (
              select patientid, count(*) as total
              from packagedruginfotmp
              group by patientid
         ) dispensas on dispensas.patientid = pat.patientid
  
               left join (
              select patientid, max(dispensedate) as ult_levant_idart
              from packagedruginfotmp
              group by patientid
         ) ult_lev on ult_lev.patientid = pat.patientid;
  
  
   "
        )
      )
    
    patients  # same as return(patients)
    
  }
  
  
  #' Busca todos pacientes referidos para FARMAC
  #' 
  #' @param con.postgres  obejcto de conexao com BD iDART
  #' @return tabela/dataframe/df com pacientes ref as FARMAC
  #' @examples pat_farmac <- getAllPatientsFarmac(con_idart)
  getAllPatientsFarmac <- function(con.postgres) {
    
    tryCatch({
      
      patients  <-
        dbGetQuery(
          con.postgres,
          paste0(
            "SELECT  clinic, clinicname, 
         mainclinic, mainclinicname, firstnames,  lastname, 
          patientid,  uuid
    FROM sync_temp_patients; "
          )
        )
      
   return(patients)
      
    },error = function(cond) {
      
      message(cond)
      #imprimir a mgs a consola
      # Choose a return value in case of error
      return(0)
    },warning = function(cond) {
      message("Here's the original warning message:")
      message(cond)
      # Choose a return value in case of warning
      return(patients)
    },finally = {
      # NOTE:
      # Here goes everything that should be executed at the end,
      # regardless of success or error.
      # If you want more than one expression to be executed, then you
      # need to wrap them in curly brackets ({...}); otherwise you could
      # just have written 'finally=<expression>'
      
    })
    
    
  return(patients)
    
  }
  
  
  
  
  #' Busca todos pacientes do OpenMRS incluindo os que foram apagados/ (nao usar esta funcao) -> apenas para investigar
  #' 
  #' @param con.postgres  obejcto de conexao com BD OpenMRS
  #' @return tabela/dataframe/df com todos paciente do OpenMRS 
  #' @examples patients_idart <- getAllPatientsIdart(con_openmrs)
  getPatientsInvestigar <- function(con.openmrs, uuid) {
    rs  <-
      dbSendQuery(
        con.openmrs,
        paste0(
          "
       SELECT   
          pat.patient_id, 
          pid.identifier , 
          pe.uuid,
           pn.given_name  given_name ,
           pn.middle_name middle_name,
           pn.family_name  family_name,
          concat(lower(pn.given_name),if(lower(pn.middle_name) is not null,concat(' ', lower(pn.middle_name) ) ,''),concat(' ',lower(pn.family_name)) ) as full_name_openmrs ,
          pe.birthdate,
          estado.estado as estado_tarv ,
          max(estado.start_date) data_estado,
          date(visita.encounter_datetime) as data_ult_levant,
          date(visita.value_datetime) as data_prox_marcado
          FROM  patient pat INNER JOIN  patient_identifier pid ON pat.patient_id =pid.patient_id 
          INNER JOIN person pe ON pat.patient_id=pe.person_id 
          INNER JOIN person_name pn ON pe.person_id=pn.person_id 
          LEFT JOIN
        		(
        			SELECT 	pg.patient_id,ps.start_date encounter_datetime,location_id,ps.start_date,ps.end_date,
        					CASE ps.state
                                WHEN 6 THEN 'ACTIVO NO PROGRAMA'
        						WHEN 7 THEN 'TRANSFERIDO PARA'
        						WHEN 8 THEN 'SUSPENSO'
        						WHEN 9 THEN 'ABANDONO'
        						WHEN 10 THEN 'OBITO'
                                WHEN 29 THEN 'TRANSFERIDO DE'
        					ELSE 'OUTRO' END AS estado
        			FROM 	patient p
        					INNER JOIN patient_program pg ON p.patient_id=pg.patient_id
        					INNER JOIN patient_state ps ON pg.patient_program_id=ps.patient_program_id
        			WHERE 	pg.voided=0 AND ps.voided=0 AND p.voided=0 AND
        					pg.program_id=2 AND ps.state IN (6,7,8,9,10,29) AND ps.end_date IS NULL
        
        
        		) estado ON estado.patient_id=pe.person_id
         LEFT Join
             (
        		Select ult_levantamento.patient_id,ult_levantamento.encounter_datetime,o.value_datetime
        		from
        
        			(	select 	p.patient_id,max(encounter_datetime) as encounter_datetime
        				from 	encounter e
        						inner join patient p on p.patient_id=e.patient_id
        				where 	e.voided=0 and p.voided=0 and e.encounter_type=18
        				group by p.patient_id
        			) ult_levantamento
        			inner join encounter e on e.patient_id=ult_levantamento.patient_id
        			inner join obs o on o.encounter_id=e.encounter_id
        			where o.concept_id=5096 and o.voided=0 and e.encounter_datetime=ult_levantamento.encounter_datetime and
        			e.encounter_type =18
        		) visita  on visita.patient_id=pn.person_id
        
            group by pat.patient_id order by     pat.patient_id
          "
        )
      )
    
    data <- fetch(rs, n = -1)
    RMySQL::dbClearResult(rs)
    return(data)
    
  }
  
  #' Busca o total de levantamentos de um Paciente no iDART  
  #' TODO -  funcao referencia uma variavel global-- deve se optimizar de modo que seja um parametro
  #' 
  #' @param patient.id id do paciente na tabela  Patient
  #' @param uuid.openmrs uuid do  OpenMRS
  #' @return 0/1  (0) - error  (1) - sucess   
  #' @examples updateUUID(67,  uuid)
  getTotalDeDispensasPorPaciente  <- function(patient.id) {
    levPaciente  <-
      dispensasPorPaciente[which(dispensasPorPaciente$id == patient.id), ]
    
    levPaciente$totaldispensas
  }
  
  #' Escreve  os logs das accoes executadas nas DBs iDART/OpenMRS numa tabela logsExecucao
  #' 
  #' @param patient.info informacao do paciente[id,uuid,patientid,openmrs_patient_id,full.name,index]   
  #' @param action descricao das accoes executadas sobre o paciente
  #' @return append uma row na tabela logs
  #' @examples
  #' logAction(patientToUpdate, ' Paciente com NID X Alrerado Para NID Y')
  logAction <- function (patient.info,action){
    
    
    logsExecucao <<-  add_row(logsExecucao, id=as.numeric(patient.info[1]),uuid=patient.info[2],patientid=patient.info[3],full_name=patient.info[5],accao=action)
    # logsExecucao <<- rbind.fill(logsExecucao, temp)  gera registos multipos no log
    
  }
  
  
  #' Compoe um vector com dados do paciente que se vai actualizar/ Correcao de duplicados
  #' 
  #' @param df tabela de duplicados para extrair os dados do Pat
  #' @param index row do paciente em causa 
  #' @return vector[id,uuid,patientid,openmrs_patient_id,full.name,index] 
  #' @examples composePatientToUpdate(67, nids_dups)
  composePatientToUpdate <- function(index,df){
    
    id = df$id[index]
    uuid = df$uuid[index]
    patientid = df$patientid[index]
    openmrs_patient_id =df$patient_id[index]
    full.name =  df$full_name[index]
    
    Encoding(full.name) <- "latin1"
    full.name <- iconv(full.name, "latin1", "UTF-8",sub='')
    
    patientid <- gsub(pattern = ' ', replacement = '', x = patientid) 
    patientid <- gsub(pattern = '\t', replacement = '', x = patientid)
    patient <- c(id,uuid,patientid,openmrs_patient_id,full.name,index)
    patient
  }
  
  
  
  
  #' Compoe um vector com dados do paciente que se vai actualizar/ Formatacao de NIDS
  #' 
  #' @param df tabela de duplicados para extrair os dados do Pat
  #' @param index row do paciente em causa 
  #' @return vector[id,uuid,patientid,openmrs_patient_id,full.name,index,given_name+midle_name,family_name] 
  #' @examples composePatientToUpdate(67, nids_dups)
  composePatientToUpdateNomeNid <- function(index,df){
    
    id = df$id[index]
    uuid = df$uuid[index]
    if(is.na(uuid)){
      if("openmrs_uuid" %in% colnames(df))
      {
        uuid=df$openmrs_uuid[index];
      }
      
      }
    patientid = df$patientid[index]
    openmrs_patient_id =df$patient_id[index]
    full.name =  df$full_name[index]
    middle_name =df$middle_name[index]
    if(is.na(middle_name))
    { given_name = df$given_name[index]}
    else{
      
      given_name = gsub(pattern = 'NA', replacement = '',x = paste0(df$given_name[index],' ',df$middle_name[index]))
    }
   
    family_name = df$family_name[index]
    openmrs_patient_id =df$patient_id[index]
    firstnames <- df$firstnames[index]
    lastname <-  df$lastname[index]
    
  # enconding problems
    Encoding(given_name) <- "latin1"
    given_name <- iconv(given_name, "latin1", "UTF-8",sub='')
    
    Encoding(family_name) <- "latin1"
    family_name <- iconv(family_name, "latin1", "UTF-8",sub='')
    
    Encoding(firstnames) <- "latin1"
    firstnames <- iconv(firstnames, "latin1", "UTF-8",sub='')
    
    Encoding(lastname) <- "latin1"
    lastname <- iconv(lastname, "latin1", "UTF-8",sub='')
    patientid <- gsub(pattern = ' ', replacement = '', x = patientid)
    patientid <- gsub(pattern = '\t', replacement = '', x = patientid)
    patient <- c(id,uuid,patientid,openmrs_patient_id,full.name,index,given_name,family_name,firstnames,lastname)
    patient
  }
  #' Compoe um vector com dados do paciente para se utilizar na tabela dos logs
  #' 
  #' @param df tabela onde se vai extrair os dados 
  #' pode ser tabela do openmrs/idart/ ou uma composta
  #' @param index row do paciente em causa 
  #' @return vector[id,uuid,patientid,openmrs_patient_id,full.name,index] 
  #' @examples composePatientToLog(67, nids_dups)
  composePatientToLog <- function(df,index){
    
    id = df$id[index]
    uuid = df$uuid[index]
    patientid = df$patientid[index]
    patientid <- gsub(pattern = ' ', replacement = '', x = patientid) #heheh
    full_name <- str_replace_na(paste0(df$firstnames[index], ' ',df$lastname[index]),replacement=' ')
    
    Encoding(full_name) <- "latin1"
    full_name <- iconv(full_name, "latin1", "UTF-8",sub='')
    
    patientid <- gsub(pattern = '\t', replacement = '', x = patientid)
    patient <- c(id,uuid,patientid,full_name)
    patient
  }
  
  #' Actualiza os  uuid e openmrsuuid na tabela Patient
  #' 
  #' @param patient.id id do paciente na tabela  Patient
  #' @param uuid.openmrs uuid do  OpenMRS
  #' @return 0/1  (0) - error  (1) - sucess   
  #' @examples updateUUID(67,  uuid)
  updateUUID <- function (postgres.con, patient.id, uuid) {
    dbExecute(
      postgres.con,
      paste0(
        "update  public.patient set uuid='",
        uuid,
        "' , openmrsuuid='",
        uuid ,
        "'  where id = ",
        patient.id,
        " ;"
      )
    )
    
  }
  
  
  #' Actualiza os  dados do uuidopenmrs    na tabela Patient
  #' 
  #' @param patient.id id do paciente na tabela  Patient
  #' @param uuid.openmrs uuid do  OpenMRS
  #' @return 0/1  (0) - error  (1) - sucess   
  #' @examples updateUuuispenMRS(67,  uuid)
  updateUuidIdart <- function (patient.id, uuid.openmrs) {
    dbExecute(
      con_postgres,
      paste0(
        "update  public.patient set uuidopenmrs='",
        uuid.openmrs,
        "'  where id = ",
        patient.id,
        " ;"
      )
    )
    
  }
  
  
  #' Inicia o processo de actualizacao de um nid na base de dados iDART e OpenMRS 
  #' 
  #' @param new.nid.act NID do paciente por actualizar
  #' @param patient_to_update  informacao do paciente[id,uuid,patientid,openmrs_patient_id,full.name,index]
  #' @param df.idart.patients datafrane  com informacao de todos pacientes no iDART (info de identificacao e estado do paciente)
  #' @param con.openmrs objecto de conexao com a bd mysql
  #' @param con.idart objecto de  conexao com bd postgresql
  #' @return Unknown
  #' @examples beginUpdateProcess(new.nid.act,patient_to_update,df.idart.patients , con.openmrs,con.idart)
  beginUpdateProcess <- function(new.nid.act,patient_to_update,df.idart.patients , df.openmrs.patients,con.openmrs,con.idart){
    
    if(new.nid.act != 0) {
      
      if( checkIfExistsNidIdart(new.nid.act,df.idart.patients) ){
        
        new.nid.act <- getNewNid(new.nid.act)
        status_act_idart <- actualizaNidiDART(con.idart = con.idart,patient.to.update =patient_to_update,new.nid = new.nid.act )
        
        if(status_act_idart==1){
          actualizaNidOpenMRS(con.openmrs = con.openmrs,patient.to.update = patient_to_update,new.nid = new.nid.act)
          
        }
        
      }
      else if (checkIfExistsNidOpenMRS(new.nid.act,df.openmrs.patients) ){
      
        new.nid.act <- getNewNid(new.nid.act)
        status_act_idart <- actualizaNidiDART(con.idart = con.idart,patient.to.update =patient_to_update,new.nid = new.nid.act )
        
        if(status_act_idart==1){
          actualizaNidOpenMRS(con.openmrs = con.openmrs,patient.to.update = patient_to_update,new.nid = new.nid.act)
          
        }
        
      }
      else {
        
        status_act_idart <- actualizaNidiDART(con.idart = con_postgres,patient.to.update =patient_to_update,new.nid = new.nid.act )
        
        if(status_act_idart==1){
          actualizaNidOpenMRS(con.openmrs = con.openmrs,patient.to.update = patient_to_update,new.nid = new.nid.act)
          
        }
  
      }
    } 
    else{
      
      logAction(patient_to_update, 'Nao foi possivel obter novo NID para  o paciente. Verificar o Formato do NID!')
    }
    
  }
  
  
  
  #' Busca o nome da US do openmrs
  #' 
  #' @param openmrs.con objecto de conexao com mysql    
  #' @return us_default_locatoion
  #' @examples
  #' default_loc = getOpenmrsDefaultLocation(con_openmrs)
  getOpenmrsDefaultLocation <- function (openmrs.con){
    resut_set <- dbSendQuery(openmrs.con, "select property_value from global_property where property= 'default_location'")
    data <- fetch(resut_set,n=1)
    openmrs_default_location <- data$property_value[1]
    RMySQL::dbClearResult(resut_set)
    rm(data,resut_set)
    #detach("package:RMySQL", unload = TRUE)
    return (openmrs_default_location)
    
  }
  
  
  
  #' Busca o cod da US do openmrs
  #' 
  #' @param df.openmrs df com dados dos pacientes do openrms   
  #' @return us.code codigo da US
  #' @examples
  #' us_code = getOpenmrsUsCode(openmrsAllpPatients)
  getOpenmrsUsCode<- function ( df.openmrs){
    
    nids_correctos <- df.openmrs$patientidSemLetras[which(nchar(df.openmrs$patientidSemLetras)==21)]
    us_codes <- substr(nids_correctos, 0, stri_locate_first(nids_correctos, regex = "/")[[1]] -1 )
    us_code <- sort(table(us_codes),decreasing=TRUE)[1][1]
    us_code <- names(us_code)
    return(us_code)
    
  }
  
  
  
  #' Busca a maior frequencia da posicao da barra / nos nids mal formatados
  #' Ano/Seq  (934/11) ou Seq/Ano (11/934)
  #' 
  #' @param vec cevotr com todos os dados dos pacientes do openrms   
  #' @return pos posicao da barra do nid
  #' #' @examples
  #' pos = getMaxPosBarraVec(c('12/45','12/1342','12/34412','12345/23','12346/12','12345/15','12345/14')) retorna 6
  getMaxPosBarraVec<- function (vec ){
    
    
    if(length(vec)!=0){
      temp <- as.list( stri_locate_first(vec, regex = "/") )
      b = table(unlist(temp))
      max_pos= names(subset(b, b==max(b),c(,1)))
      max_pos=as.integer(max_pos)
      if(length(max_pos)==2){
        
        return(min(max_pos)[1])
      } 
      max_pos
      
    }else{ return(0)}
    
    
  }
  
  
  #' Busca o tipo de sequencia de nids mal registados 
  #' Ano/Seq  (934/11) ou Seq/Ano (11/934)
  #' 
  #' @param df df com dados dos pacientes do openrms   
  #' @param nr_char tamanho do nid mal formatado  
  #' @return pos posicao da barra do nid
  #' #' @examples
  #' seq = getTipoSeqNidUs(openmrsAllPatients)
  getTipoSeqNidUs <- function (df) {
    var_test_1 = 6
    var_test_2 = 7   # Nids com 7  caracters
    
    vec_nids_6 <-
      df$patientidSemLetras[which(nchar(df$patientidSemLetras) == var_test_1)] # Nids com 6 caracters
    vec_nids_7 <-
      df$patientidSemLetras[which(nchar(df$patientidSemLetras) == var_test_2)]
    
    pos_nid_6 <- getMaxPosBarraVec(vec_nids_6)
    pos_nid_7 <- getMaxPosBarraVec(vec_nids_7)
    
    if (pos_nid_7 != 0) {
      if (pos_nid_7 == 5) {
        return('Seq/Ano')
      } else if (pos_nid_7 == 3) {
        return('Ano/Seq')
      }
    } else {
      if (pos_nid_6 != 0) {
        if (pos_nid_6 == 4) {
          return('Seq/Ano')
        } else if (pos_nid_6 == 3) {
          return('Ano/Seq')
        } else {
          return('Ano/Seq_')
        } # default nao conseguimos encontrar nids com 6 ou 7 caracteres
        
      } else {
        return('Ano/Seq_')
      }  # default nao conseguimos encontrar nids com 6 ou 7 caracteres
      
      
    }
    
  }
  
  
  
  #' Calcula o peso da diferenca de dois strings atraves do algoritmo de leveingstein / stringdist no R
  #' Funcao baseada no algoritmo de simetria de nomes
  #' @param string1 do primeiro nome  
  #' @param string2 o segudo nome
  #' @return numeric distance
  #' #' @examples
  #' dist  = getStringDistance('agnaldo','agnaldo s')
  getStringDistance <- function (string1,string2) {
   
    dist <- stringdist(string1,string2,method = 'jw')
    return(dist)
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
  
  #ComposePatientToCheck --> Compoe um vector com dados do paciente =
    #' 
    #' @param df tabela de duplicados para extrair os dados do Pat
    #' @param index row do paciente em causa 
    #' @return vector[id,uuid,patientid,openmrs_patient_id,full.name,index] 
    #' @examples pat <-  composePatientToCheck(k, df.different_uuid)
    #' 
    composePatientToCheck <- function(index,df){
      
      id = df$id[index]
      patientid = df$patientid[index]
      uuid_idart = df$uuid[index]
      uuid_openmrs =df$uuidopenmrs[index]
      full_name =  gsub(pattern = " " , replacement = " ", x = paste0(df$firstnames[index], " ", df$lastname[index]))
      
      Encoding(full_name) <- "latin1"
       
      full_name  <- iconv(full_name, "latin1", "UTF-8",sub='')
      full_name  <- gsub(pattern = '  ' ,replacement = ' ', x = full_name)
      patientid <- gsub(pattern = ' ', replacement = '', x = patientid)
      patientid <- gsub(pattern = '\t', replacement = '', x = patientid)
      patient <- c(id,patientid,uuid_idart,uuid_openmrs,full_name)
      return(patient)
    }
    
    
    
    
    
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
          message("Object with given uuid doesn't exist" )
          return(FALSE)
        } else {
          
          message("Object with given uuid doesn't exist" )
          return(FALSE)
        }
      }
      return(status)
    }