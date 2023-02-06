preferences <- new.env(parent = emptyenv())

load_preference <-function(){
  configDir <- tools::R_user_dir("ConnectAnalystR", which="config")
  configFile <- file.path(configDir,"preferences.RData")

  if (!dir.exists(configDir)){
    dir.create(configDir, recursive=TRUE)
  }
  if (file.exists(configFile)){
    load(configFile,envir = preferences)
  }else{
    preferences$create_date = date()
    save_preferences()
  }
}

save_preferences <- function(){
  save(list=ls(preferences),envir = preferences,file = file.path(tools::R_user_dir("ConnectAnalystR","config"),"preferences.RData"))
}
load_preference()



#' set the preferences/default parameters
#'
#' @description
#' `set_env` sets the environment for the data. It can from from the development (dev),
#' staging (stage) or production (prod) Connect environments.
#'
#' `set project` sets the project that will get billed for the data processing and download.
#' @param project The GCP project getting billed
#' @param env prod, stage, or dev Connect data
#'
#' @export
set_project<-function(project){
  preferences$project <- project
  save_preferences()
}

#' @rdname set_project
#' @export
set_env<-function(env=c("dev","stage","prod")){
  env=rlang::arg_match(env)
  preferences$env <- env
  save_preferences()
}
