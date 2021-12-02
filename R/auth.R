
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
  scopes = c("https://www.googleapis.com/auth/bigquery",
             "https://www.googleapis.com/auth/cloud-platform")

  message("using keyfile: ",jsonFile)
  token <- gargle::credentials_service_account(scopes,jsonFile)
  access_token = token$credentials$access_token
  message("writing to file: ",saveTo)
  writeLines(access_token,saveTo)
}

#' Check the token saved by saveTokenToFile
#'
#' @description
#' Reads a token out of savedFile.  If there is a valid token, the email of the
#' service account is displayed.  Otherwise, there is an error, you should consider
#' recreating the token using [saveTokenToFile()]
#'
#' @seealso [ConnectAnalystR::saveTokenToFile()]
#'
#' @param savedFile The file read that contains the token.
#'
#' @return invisibly returns a list of information about the token
#' @export
#'
checkSavedAccessToken <- function(savedFile="~/.access_token"){
  access_token=readLines(savedFile)
  access_token=gsub("\"","",access_token)
  url = paste0("https://www.googleapis.com/oauth2/v1/tokeninfo?access_token=",
               access_token)
  res <- httr::GET(url)
  if (httr::http_status(res)$category == "Success"){
    res <- httr::content(res)
    message("email: ",res$email)
  } else {
    warning("there is a problem.  The token may be invalid, expired or there may be another issue.")
  }
  invisible(res)
}
