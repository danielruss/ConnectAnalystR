connect_bigquery_info <- list(
  dev = list(
    module1 = "`nih-nci-dceg-connect-dev.Connect.module1`"
      ),
  stage = list(
    module1 = "`nih-nci-dceg-connect-stg-5519.flat.flatmodule1_scheduledqueries`"
    ),
  prod =list(
    module1 = "`nih-nci-dceg-connect-prod-6d04.flat.module1_scheduledqueries`")
)

#' Get Occupation data from the Connect Data.
#'
#' Calls the BQ API.  The user will have to go through the OAuth dance.  To make sure you
#' have access to the data, you will want to run bq_auth().
#'
#' @param project - The project which is billed for the BQ processing.
#' @param connectEnv  - Either  "dev", "stage", or "prod"
#'
#' @return tibble with data.
#' @export
#'
getOccupationData <- function(project,connectEnv=c("dev","stage","prod")){
  rlang::check_required(project)
  connectEnv=rlang::arg_match(connectEnv)

  query = paste0("SELECT DISTINCT Connect_ID,d_627122657,d_761310265,d_118061122,d_279637054 FROM ",
                 connect_bigquery_info[[connectEnv]]$module1," where Connect_ID is not null AND (d_627122657 is Not null or d_761310265 is not null
or d_118061122 is not null or d_279637054 is not null) " )

  tb <- bigrquery::bq_project_query(project,query)
  data <- bigrquery::bq_table_download(tb,bigint = "integer64")
  colnames(data) <- c("Connect Id","CurrentJobTitle","CurrentSelection",
                      "LongestJobTitle","LongestSelection")
  attr(data,"date") <- format(Sys.time(),"%a %b %d %Y %I:%M %p")
  data
}


#' Pivot the Occupational data
#'
#' @param data Occupational data from getOccupationData
#' @param ...  alternatively, pass in the variables for getOccupationalData and this function
#'  will call getOccupationalData and pivot in one step
#'
#' @return a tibble with Connect Id, longest/Current job, JobTitle, Selection, and rank (1-4,5+)
#' @export
#'
pivotOccupationData<-function(data,...){
  if (missing("data")){
    data <- getOccupationData(...)
  }

  levels =c("1","2","3","4","5+")
  data <- data %>% tidyr::pivot_longer(2:5, names_to = c("set",".value"), names_sep = 7) %>%
    dplyr::filter(!is.na(JobTitle)&!is.na(Selection)) %>%
    dplyr::filter(grepl("NONE_OF_THE_ABOVE|\\d{2}-\\d{4}-\\d",Selection))  %>%
    dplyr::mutate(rank = factor(dplyr::case_when(
      Selection == "NONE_OF_THE_ABOVE" ~ "5+",
      grepl("-0$",Selection) ~ "1",
      grepl("-1$",Selection) ~ "2",
      grepl("-2$",Selection) ~ "3",
      grepl("-3$",Selection) ~ "4",
    ),levels=levels,ordered = TRUE))
  attr(data,"date") <- format(Sys.time(),"%a %b %d %Y %I:%M %p")
  return(data)
}


#' @rdname  makeOccupationBarChart
#' @export
makeDetailedOccupationBarChart<-function(pivottedData,...){
  if (missing(pivottedData)){
    pivottedData <- pivotOccupationData(...)
  }

  pivottedData %>% ggplot2::ggplot(ggplot2::aes(x = rank)) + ggplot2::geom_bar() +
    ggplot2::geom_text(stat = "count",
                       ggplot2::aes(label = paste0(..count..," (", round(..count.. /sum(..count..) * 100, 1), "%)")),
                       vjust = -1) +
    ggplot2::scale_y_continuous(expand = ggplot2::expansion(mult = c(0,0.1))) +
    ggplot2::labs(title = "Connect Participants Selecting from the Top 4\nSOCcer choices",
                  subtitle = "Current and Longest Job",
                  caption = paste0("Analysis run on data available at ",attr(pivottedData, "date")), x = "Participant Selected One of the Top 4 SOCcer choices")
}


#' Makes a bar chart for the Occupational Data Downloaded from the Connect Data
#'
#' @description
#' Makes a bar chart that from the Occupational Data in Connect.
#'
#' makeOccupationBarChart() returns a bar chart displaying whether or not a job was
#' coded to the top 4.
#'
#' makeDetailedOccupationBarChart() returns a bar chart showing the count of the rank for
#' all the Connect data.  The count is slight lower in this plot because the first 324
#' jobs did not have the rank.
#'
#' @param occData The occupational data returned from  \code{\link{getOccupationData}}.
#' @param pivottedData Occupational data returned from \code{\link{pivotOccupationData}}.
#' @param ... Parameters passed to getOccupationData(), must include both and project,connectEnv
#'
#'
#' @return
#' @export
#'
makeOccupationBarChart <- function(occData){
  data <- occData %>% tidyr::pivot_longer( 2:5,names_to=c("set",".value"),names_sep = 7 ) %>%
    dplyr::filter(!is.na(JobTitle) &!is.na(Selection))
  data %>% dplyr::mutate(InTop4=Selection!="NONE_OF_THE_ABOVE") %>% ggplot2::ggplot(ggplot2::aes(x=InTop4)) + ggplot2::geom_bar()+
    ggplot2::geom_text(stat='count', ggplot2::aes(label=paste0(..count..," (",round( ..count../sum(..count..)*100,1),"%)")),
              vjust=-1) +
    ggplot2::scale_y_continuous(expand = ggplot2::expansion(mult=c(0,0.1))) +
    ggplot2::labs(title = "Connect Participants Selecting from the Top 4\nSOCcer choices",
                  subtitle = "Current and Longest Job",
                  caption= paste0("Analysis run on data available at ",attr(occData,"date")),
                  x="Participant Selected One of the Top 4 SOCcer choices"
                  )
}

prod_env <- new.env(parent = emptyenv())
prod_env$module1 <- "`nih-nci-dceg-connect-prod-6d04.flat.module1_scheduledqueries`"



