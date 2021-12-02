
#' saveTokenToFile
#'
#' @description
#' Gets a service account token and writes it to a plain text file.
#'
#' @param jsonFile Path to a service account key JSON file
#' @param saveTo Path of the output text file containing the service account
#' token
#'
#' @export
#'
saveTokenToFile <- function(jsonFile,saveTo="~/.access_token"){
  message("using keyfile: ",jsonFile)
  bigrquery::bq_auth(path = jsonFile)
  token <- bigrquery::bq_token()
  access_token = token$auth_token$credentials$access_token
  message("writing to file: ",saveTo)
  writeLines(access_token,saveTo)
}
