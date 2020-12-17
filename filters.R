library(dplyr)


pacientes_dados_consistentes <- filter(patients, openmrs_status=='Ok' & estado_permanencia=='ACTIVO NO PROGRAMA' 
                                       & duplicado_openmrs=='No' & uuidopenmrs==uuid_api_openmrs)

pacientes_not_found <- filter(patients, openmrs_status=='Not found')

pacientes_nao_activos <- filter(patients, openmrs_status=='Ok' &  estado_permanencia =='ABANDONO/TRANSFERIDO PARA/NAO REGISTADO')

temp <- left_join(pacientes_nao_activos, openmrs_patients, by = c('patientid'='identifier')) %>% select(
  patientid, firstnames, lastname, full_name_openmrs,  ult_levant_idart,estado_permanencia, 
   uuid_api_openmrs,uuid.y, estado_tarv, data_estado,data_ult_consulta,data_prox_marcado
)







