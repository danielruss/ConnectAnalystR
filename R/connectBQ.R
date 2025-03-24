
connect_bigquery_info <- list(
  dev = list(
    module1_v2  = "`nih-nci-dceg-connect-dev.Connect.module1_v2`",
    participant = "`nih-nci-dceg-connect-stg-5519.Connect.participants`"
      ),
  stage = list(
    module1_v1  = "`nih-nci-dceg-connect-stg-5519.Connect.module1_v1`",
    module1_v2  = "`nih-nci-dceg-connect-stg-5519.Connect.module1_v2`",
    participant = "`nih-nci-dceg-connect-stg-5519.Connect.participants`"
    ),
  prod =list(
    module1_v1  = "`nih-nci-dceg-connect-prod-6d04.Connect.module1_v1`",
    module1_v2  = "`nih-nci-dceg-connect-prod-6d04.Connect.module1_v2`",
    participant = "`nih-nci-dceg-connect-prod-6d04.Connect.participants`"
    )
)

# make a small data dictionary that maps to concept ids needed by this function
cid <- list(
  "Connect_ID"="Connect_ID",
  "CurrentJobTitle"="d_627122657",
  "CurrentSelection"="d_761310265",
  "CurrentJobTask"="d_796828094",
  "LongestJobTitle"="d_118061122",
  "LongestSelection"="d_279637054",
  "LongestJobTask"="d_518387017",
  "CurrentIndustry"="d_700374192.d_700374192",
  "CurrentIndustryOther"="d_700374192.d_923333992",
  "LongestIndustry"="d_530742915.d_530742915",
  "LongestIndustryOther"="d_530742915.D_739237614",
  "641572847"="Manufacturing",
  "592592455"="Retail or wholesale sales",
  "149230791"="Transportation, warehousing, and utilities (e.g., water, sanitation, electric)",
  "439857718"="Professional and business services (e.g., real estate, technical and scientific services, finance, insurance)",
  "133362151"="Construction or equipment repair",
  "372993567"="Mining, quarrying, and oil and gas extraction",
  "927863729"="Farming, fishing, or forestry",
  "709307391"="Accommodation and food services (e.g., hotels and restaurants)",
  "766533183"="Healthcare or social assistance",
  "698814077"="Government",
  "942545134"="Military, police, firefighting, security services",
  "924908599"="Education",
  "868510850"="Arts, entertainment, and recreation",
  "807835037"="Some other type of business",
  "711138281"="Wholesale or distributor",
  "596792238"="Service Provider",
  "620577362"="Farming",
  "751475124"="Military",
  "178420302"="Don't Know",
  ## This is in the participants table
  "module1_submit_time"="d_517311251"
)



#' Get Occupation data from the Connect Data.
#'
#' Calls the BQ API.  The user will have to go through the OAuth dance.  To make sure you
#' have access to the data, you will want to run bq_auth().
#'
#' @param project - The project which is billed for the BQ processing.
#' @param env  - Either  "dev", "stage", or "prod"
#'
#' @return tibble with data.
#' @export
#'
getOccupationData <- function(project=preferences$project,env=preferences$env){
  if (is.null(project)) stop('project must be defined')
  if (is.null(env) || !env %in% c("dev","stage","prod")) stop('env must one of "dev", "stage", or "prod"')

  v1_data <- get_v1_with_date(project,env)
  v2_data <- get_v2_with_date(project,env)
  data <- dplyr::bind_rows(
    v1_data,
    v2_data
  )

  attr(data,"date") <- format(Sys.time(),"%a %b %d %Y %I:%M %p")
  data
}

get_v1_with_date <- function(project,env){
  ## the dev data from module 1 is empty
  ## so just return an empty tibble if you are looking
  ## at the dev data.
  if (env=="dev"){
    v1_data <- tibble::tibble(
      Connect_ID = bit64::integer64(),
      CurrentJobTitle = character(),
      CurrentJobTask = character(),
      CurrentSelection = character(),
      LongestJobTitle = character(),
      LongestJobTask = character(),
      LongestSelection = character(),
      version = character(),
    )
    return(v1_data)
  }

  tbl_v1 <- connect_bigquery_info[[env]]$module1_v1
  v1_query <- glue::glue_data(cid,"select module1.{Connect_ID},",
                              "module1.{CurrentJobTitle} as CurrentJobTitle, ",
                              "module1.{CurrentSelection} as CurrentSelection,",
                              "module1.{LongestJobTitle} as LongestJobTitle,",
                              "module1.{LongestSelection} as LongestSelection,",
                              "participant.{module1_submit_time} as submit_time,",
                              "from {tbl_v1} as module1 ",
                              "LEFT JOIN {tbl_participant} AS participant ",
                              "ON module1.{Connect_ID} = participant.{Connect_ID} ",
                              "WHERE module1.{Connect_ID} is not null AND not ",
                              "(module1.{CurrentJobTitle} is null and module1.{CurrentSelection} is null and ",
                              "module1.{LongestJobTitle} is null and module1.{LongestSelection} is null)")
  v1_tbl <- bigrquery::bq_project_query(project,v1_query)
  v1_data <- bigrquery::bq_table_download(v1_tbl,bigint = "integer64") |>
    dplyr::mutate(CurrentJobTask = NA_character_,
                  LongestJobTask = NA_character_)
  v1_data$version="v1";

  v1_data


}

get_v2_with_date <- function(project,env){
  tbl_v2 <- connect_bigquery_info[[env]]$module1_v2
  tbl_participant <- connect_bigquery_info[[env]]$participant
  v2_query <-  glue::glue_data(
    cid,
    "SELECT ",
    "module1.{Connect_ID}, ",
    "module1.{CurrentJobTitle} as CurrentJobTitle, ",
    "module1.{CurrentJobTask} as CurrentJobTask, ",
    "module1.{CurrentSelection} as CurrentSelection, ",
    "module1.{LongestJobTitle} as LongestJobTitle, ",
    "module1.{LongestJobTask} as LongestJobTask, ",
    "module1.{LongestSelection} as LongestSelection, ",
    "participant.{module1_submit_time} as submit_time ",
    "from {tbl_v2} as module1 ",
    "LEFT JOIN {tbl_participant} AS participant ",
    "ON module1.{Connect_ID} = participant.{Connect_ID} ",
    "WHERE module1.{Connect_ID} is not null AND not ",
    "(module1.{CurrentJobTitle} is null and module1.{CurrentSelection} is null and ",
    "module1.{LongestJobTitle} is null and module1.{LongestSelection} is null and ",
    "module1.{CurrentJobTask} is null and module1.{LongestJobTask} is null)"
  )
  #v2_query
  v2_tbl <- bigrquery::bq_project_query(project,as.character(v2_query) )
  v2_data <- bigrquery::bq_table_download(v2_tbl,bigint = "integer64")
  v2_data$version="v2";

  v2_data
}


#' Pivot the Occupational data
#'
#' @param data Occupational data from getOccupationData
#' @param ...  alternatively, pass in the variables for getOccupationalData and this function
#'  will call getOccupationalData and pivot in one step (project and env)
#'
#' @return a tibble with Connect Id, longest/Current job, JobTitle, Selection, and rank (1-4,5+)
#' @export
#'
#' @importFrom rlang .data
#'
pivotOccupationData<-function(data,...){
  if (missing("data")){
    data <- getOccupationData(...)
  }

  levels =c("1","2","3","4","5+")
  data <- data %>% tidyr::pivot_longer(c(dplyr::starts_with("Current"),dplyr::starts_with("Longest")),
                                       names_to = c("set",".value"),
                                       names_pattern = "(Current|Longest)(.*)") %>%
    dplyr::filter(!is.na(.data$JobTitle)&!is.na(.data$Selection)) %>%
    dplyr::filter(grepl("NONE_OF_THE_ABOVE|\\d{2}-\\d{4}-\\d",.data$Selection))  %>%
    dplyr::mutate(rank = factor(dplyr::case_when(
      .data$Selection == "NONE_OF_THE_ABOVE" ~ "5+",
      grepl("-0$",.data$Selection) ~ "1",
      grepl("-1$",.data$Selection) ~ "2",
      grepl("-2$",.data$Selection) ~ "3",
      grepl("-3$",.data$Selection) ~ "4",
    ),levels=levels,ordered = TRUE))
  attr(data,"date") <- format(Sys.time(),"%a %b %d %Y %I:%M %p")
  return(data)
}


#' @rdname  makeOccupationBarChart
#' @export
makeDetailedOccupationBarChart<-function(pivottedData,...){
  # no data passed...
  if (missing(pivottedData)){
    pivottedData <- pivotOccupationData(...)
  }

  # passed unpivotted data.
  if ("CurrentJobTitle" %in% colnames(pivottedData)){
    pivottedData <- pivotOccupationData(pivottedData)
  }

  # str2lang is needed to prevent check() from creaming about global variables.
  pivottedData %>% ggplot2::ggplot(ggplot2::aes(x = rank)) + ggplot2::geom_bar() +
    ggplot2::geom_text(stat = "count",
                       ggplot2::aes(label = ggplot2::after_stat( paste0(!!str2lang("count")," (", round(!!str2lang("count") /sum(!!str2lang("count")) * 100, 1), "%)"))),
                       vjust = -1,hjust=.3) +
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
#' @param ... Parameters passed to getOccupationData(), must include both and project,env
#'
#'
#' @return ggplot object of barplot ready for displaying or saving with ggsave.
#'
#' @export
#'
#' @importFrom rlang .data
makeOccupationBarChart <- function(occData){
  data <- occData %>% tidyr::pivot_longer( 2:5,names_to=c("set",".value"),names_sep = 7 ) %>%
    dplyr::filter(!is.na(.data$JobTitle) &!is.na(.data$Selection))
  data %>% dplyr::mutate(InTop4=.data$Selection!="NONE_OF_THE_ABOVE") %>% ggplot2::ggplot(ggplot2::aes(x=.data$InTop4)) + ggplot2::geom_bar()+
    ggplot2::geom_text(stat='count', ggplot2::aes(label=ggplot2::after_stat(paste0(!!str2lang("count")," (",round( !!str2lang("count")/sum(!!str2lang("count"))*100,1),"%)"))),
              vjust=-1) +
    ggplot2::scale_y_continuous(expand = ggplot2::expansion(mult=c(0,0.1))) +
    ggplot2::labs(title = "Connect Participants Selecting from the Top 4\nSOCcer choices",
                  subtitle = "Current and Longest Job",
                  caption= paste0("Analysis run on data available at ",attr(occData,"date")),
                  x="Participant Selected One of the Top 4 SOCcer choices"
                  )
}

#' getIndustryData
#'
#' @param project - The project which is billed for the BQ processing.
#' @param env  - Either  "dev", "stage", or "prod"
#'
#' @return - tibble with industry data:
#'  The Connect_ID, a column of integer64's representingthe Connect_ID;
#'  CurrentIndustryCID, a list column containing 1 or more ConceptIDs;
#'  CurrentIndustry, a list column containing 1 or more strings decyphered from the concept id;
#'  CurrentIndustryOther, a column of characters that represents what a participant put in the
#'  other field of the the question.
#'  LongestIndustryCID,LongestIndustry,LongestIndustryOther are the same as the CurrentXX except
#'  they are for the Longest Job question instead of the current job question.
#' @export
#'
#' @importFrom rlang .data
#'
getIndustryData <- function(project=preferences$project,env=preferences$env){
  if (is.null(project)) stop('project must be defined')
  if (is.null(env) || !env %in% c("dev","stage","prod")) stop('env must one of "dev", "stage", or "prod"')
  tbl_v1 <- connect_bigquery_info[[env]]$module1_v1
  v1_query <- glue::glue_data(cid,"select {Connect_ID},",
                              "{CurrentIndustry} as CurrentIndustryCID, ",
                              "{CurrentIndustryOther} as CurrentIndustryOther,",
                              "{LongestIndustry} as LongestIndustryCID,",
                              "{LongestIndustryOther} as LongestIndustryOther from {tbl_v1} ",
                              "where Connect_ID is not null AND ",
                              "ARRAY_LENGTH({CurrentIndustry}) + ARRAY_LENGTH({LongestIndustry}) > 0")
  v1_tbl <- bigrquery::bq_project_query(project,v1_query)
  v1_data <- bigrquery::bq_table_download(v1_tbl,bigint = "integer64")

  tbl_v2 <- connect_bigquery_info[[env]]$module1_v2
  v2_query <- glue::glue_data(cid,"select {Connect_ID},",
                              "{CurrentIndustry} as CurrentIndustryCID, ",
                              "{CurrentIndustryOther} as CurrentIndustryOther,",
                              "{LongestIndustry} as LongestIndustryCID,",
                              "{LongestIndustryOther} as LongestIndustryOther from {tbl_v2} ",
                              "where Connect_ID is not null AND ",
                              "ARRAY_LENGTH({CurrentIndustry}) + ARRAY_LENGTH({LongestIndustry}) > 0")
  v2_tbl <- bigrquery::bq_project_query(project,v2_query)
  v2_data <- bigrquery::bq_table_download(v2_tbl,bigint = "integer64")


  dplyr::bind_rows(v1_data,v2_data) |>
    ## convert the industry CID to the definitions...
    dplyr::mutate(CurrentIndustry=purrr::map(.data$CurrentIndustryCID,\(x) unname(unlist(cid[x]))),
                  LongestIndustry=purrr::map(.data$LongestIndustryCID,\(x) unname(unlist(cid[x]))) ) |>
    ## reorder the columns
    dplyr::select(.data$Connect_ID,
                  .data$CurrentIndustryCID,.data$CurrentIndustry,.data$CurrentIndustryOther,
                  .data$LongestIndustryCID,.data$LongestIndustry,.data$LongestIndustryOther)
}


