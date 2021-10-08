#' Run QA/QC for Connect for Cancer Prevention
#'
#' Calls the QA/QC API on Google cloud platform.  Currently the code requires a service
#' account key for prod/stage environment.  In the future, code will run with the user's
#' credentials obtained via OAUTH2.
#'
#' Note: this takes a few minutes to run.
#'
#' @param service_account_key_path  The service account key file.
#'
#' @return a list holding the output of the QAQC code
#' @export
#'
cloud_run_qaqc<-function(service_account_key_path){
  stopifnot(file.exists(service_account_key_path))

  sevice_account_details = get_details(service_account_key_path)
  token_service = sevice_account_details$token
  env = sevice_account_details$env
  url = sevice_account_details$url

  message("Attempting to access GCP...")
  res_1 <-httr::GET(url[env], config = httr::add_headers(Authorization = sprintf("Bearer %s", token_service)))
  if (httr::http_status(res_1)$category == "Success" ){
    message("\t... Able to connect... \n\t...attempting to perform QA QC in ",env)
    message("\t... This may take a few minutes...")
    qaqc_url <- paste0(url[env],"qaqc")
    dt <- system.time(
      res_2 <-httr::GET(qaqc_url, config = httr::add_headers(Authorization = sprintf("Bearer %s", token_service)))
    )[3]
    if (httr::http_status(res_2)$category == "Success" ){
      message("\t... QAQC completed in ",format(dt)," sec...")
    } else{
      warning(httr::http_status(res_2)$category,"\t",httr::content(res_2))
    }
    return(httr::content(res_2))
  }
  invisible()
}

get_details <- function(service_account_key_path){
  details = jsonlite::fromJSON(service_account_key_path)
  if (details$type != "service_account") stop("bad key file")
  message("Using service account: ",details$client_email)

  env = stringr::str_match(details$project_id,"nih-nci-dceg-connect-(\\w+)-")[2]

  if (!env %in% c("stg","prod")) {
    print(details)
    warning("project: ",details$project_id)
    warning(env)
    stop("the key is for the wrong project")
  }

  url=c(stg="https://qaqc-api-s5gaat2zja-uc.a.run.app/",prod="https://qaqc-api-xbxy4luxeq-uc.a.run.app/")
  jwt <- googleCloudRunner::cr_jwt_create(url[env],service_account_key_path)
  token_service <- googleCloudRunner::cr_jwt_token(jwt,url)
  return( list(env=env,token=token_service,url=url[env]))
}

cloud_debug<-function(service_account_key_path){
  stopifnot(file.exists(service_account_key_path))

  sa = get_details(service_account_key_path)
  httr::GET(paste0(sa$url,"debug"), config = httr::add_headers(Authorization = sprintf("Bearer %s", sa$token)))
}
