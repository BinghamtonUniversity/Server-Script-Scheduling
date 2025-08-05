# ==== Libraries ====
library(RODBC)
library(dplyr)
library(readxl)
library(writexl)
library(janitor)
library(blastula)
library(glue)
library(knitr)
library(kableExtra)
library(gt)
library(DBI)
library(tidyr)
library(tidygeocoder)


tryCatch({
  # ==== Setup Paths ====
  readRenviron("//bushare.binghamton.edu/assess/Shiny Apps/.Renviron.R")
  today <- format(Sys.Date(), "%Y-%m-%d")
  output_path <- paste0("//bushare.binghamton.edu/assess/Shared SAASI/Matthew/TSS/Shiny Apps/TSS Students/Temp Data/tss_cleaned_", today, ".csv")
  
  # ==== Connect to ODS ====
  con <- odbcConnect("ODSPROD", uid = Sys.getenv("ods_userid"), pwd = Sys.getenv("ods_pwd"))
  conn <- dbConnect(odbc::odbc(), dsn = "ODSPROD", UID = Sys.getenv("ods_userid"), PWD = Sys.getenv("ods_pwd"))
  
  # ==== 1. Load Fall 2024 PDE Snapshot ====
  # 1. point at your “Dumps” folder
  dump_dir <- "//bushare.binghamton.edu/assess/Shared SAASI/Banner Info/Periodic Data Exports/PDE - R Scripts/Dumps"
  
  # 2. list all .xlsx files
  xlsx_files <- list.files(dump_dir, pattern = "\\.xlsx$", full.names = TRUE)
  
  # 3. pick the one with the most recent mtime
  latest_xlsx <- xlsx_files[ which.max(file.info(xlsx_files)$mtime) ]
  
  # 4. read it in and clean names
  pde <- read_excel(latest_xlsx) %>%
    clean_names() %>%
    rename_all(toupper)
  
  stopifnot("ID_NUMBER" %in% names(pde))
  
  # ==== 2. Get PERSON_UID Mapping ====
  id_map <- sqlQuery(con, "
    SELECT DISTINCT ID_NUMBER, PERSON_UID
    FROM ODSMGR.STUDENT_BU
    WHERE ACADEMIC_PERIOD = 202490
      AND PRIMARY_PROGRAM_IND = 'Y'
  ")
  
  pde <- pde %>% left_join(id_map, by = "ID_NUMBER")
  
  # ==== 3. Flag Previous Transfers ====
  previous_transfers <- sqlQuery(con, "
SELECT
  s.PERSON_UID,
  s.ID_NUMBER,
  s.FIRST_NAME,
  s.LAST_NAME,
  s.STUDENT_POPULATION_DESC,
  s.ACADEMIC_PERIOD
FROM
  ODSMGR.STUDENT_BU s
WHERE
  s.STUDENT_POPULATION_DESC = 'UG Transfer'
  AND s.OFFICIALLY_ENROLLED = 'Y'
  AND PRIMARY_PROGRAM_IND = 'Y'
  AND REGEXP_LIKE (ACADEMIC_PERIOD, '^(....90|....20)')
  AND COLLEGE != 'UL'
") 
  previous_transfers = previous_transfers %>%  group_by(ID_NUMBER) %>%
  filter(ACADEMIC_PERIOD == min(ACADEMIC_PERIOD)) %>% rename(ACADEMIC_PERIOD_TRANSFER = ACADEMIC_PERIOD) %>% ungroup()
  
  pde <- pde %>%
    mutate(TRANSFER = if_else(ID_NUMBER %in% previous_transfers$ID_NUMBER, 1, 0))
  
  # ==== 4. Previous Institution Info ====
  prev_ed <- sqlQuery(con, "
  SELECT PERSON_UID, ID, ACADEMIC_YEAR, ACADEMIC_PERIOD, TRANSFER_COURSE_INSTITUTION, TRANSFER_COURSE_INST_DESC,  TRANS_INST_ATTENDANCE_SEQ
  FROM
    ODSMGR.STUDENT_TRANSFERRED_COURSE
    WHERE CREDITS_PASSED > 0
    AND TRANSFER_COURSE_INSTITUTION != 'TAADVN'
    AND TRANSFER_COURSE_INSTITUTION != 'TAIB'
")
  
  prev_ed_filtered <- prev_ed %>%
    left_join(previous_transfers %>% select(PERSON_UID, ACADEMIC_PERIOD_TRANSFER), by = "PERSON_UID") %>%
    # Filter to only institutions before the student's transfer entry
    filter(as.numeric(ACADEMIC_PERIOD) < as.numeric(ACADEMIC_PERIOD_TRANSFER)) %>% 
    group_by(PERSON_UID) %>%
    arrange(desc(ACADEMIC_PERIOD)) %>%
    slice(1) %>%
    ungroup()

  
  
  pde <- pde %>% left_join(prev_ed_filtered %>% select(PERSON_UID, TRANSFER_COURSE_INST_DESC ), by = "PERSON_UID")
  
  # ==== 5. Original Address (Earliest) ====
  address_data <- sqlQuery(con, "
  SELECT 
    a.ENTITY_UID AS PERSON_UID,
    a.ADDRESS_TYPE,
    a.ADDRESS_TYPE_DESC,
    a.STREET_LINE1,
    a.STREET_LINE2,
    a.STREET_LINE3,
    a.CITY,
    a.STATE_PROVINCE,
    a.STATE_PROVINCE_DESC,
    a.POSTAL_CODE,
    a.ADDRESS_START_DATE
  FROM ODSMGR.ADDRESS a
  JOIN (
    SELECT DISTINCT PERSON_UID
    FROM ODSMGR.STUDENT_BU
    WHERE ACADEMIC_PERIOD = 202490
      AND PRIMARY_PROGRAM_IND = 'Y'
  ) s ON a.ENTITY_UID = s.PERSON_UID
  WHERE a.ADDRESS_TYPE = 'PM'
")
  
  earliest_address <- address_data %>%
    group_by(PERSON_UID) %>%
    arrange(ADDRESS_START_DATE) %>%
    slice(1) %>%
    ungroup()
  
  pde <- pde %>% left_join(earliest_address %>% select(PERSON_UID, STREET_LINE1, CITY, STATE_PROVINCE, POSTAL_CODE), by = "PERSON_UID")
  
  pde <- pde %>%
    unite(
      col = "fullAddress",
      c(STREET_LINE1, CITY, STATE_PROVINCE, POSTAL_CODE),
      sep = ", ",
      remove = FALSE,
      na.rm = TRUE
    )
  
  locations_to_geocode <- pde %>%
    filter(!is.na(fullAddress)) %>%
    distinct(fullAddress, .keep_all = TRUE)
  
  # Split into batches of 9,000
  address_batches <- split(locations_to_geocode, 
                           ceiling(seq_along(1:nrow(locations_to_geocode)) / 9500))
  
  # Geocode each batch and bind together
  geocoded_results <- lapply(address_batches, function(batch) {
    batch %>%
      geocode(address = fullAddress, method = "census", lat = latitude, long = longitude)
  }) %>%
    bind_rows()
  
  # Join geocoded data back
  pde <- geocoded_results
  
  
  # ==== 6. Write Output ====
  write.csv(pde, output_path, row.names = FALSE)
  
  # ==== 7. Email on Success ====
  summary_stats <- pde %>% count(PREVIOUS_TRANSFER) %>%
    rename(`Previous Transfer` = PREVIOUS_TRANSFER, Count = n)
  
  summary_table <- summary_stats %>%
    kable("html", caption = "Previous Transfer Status") %>%
    kable_styling("striped")
  
  email <- compose_email(
    body = html(paste0(
      "<p>✅ TSS Cleaning script completed on ", Sys.Date(), ".</p>",
      "<p><strong>Saved to:</strong> ", output_path, "</p>",
      summary_table
    )),
    footer = "— Automated TSS Cleaning Script"
  )
  
  smtp_send(
    email,
    from = "mjacob28@binghamton.edu",
    to = c("mjacob28@binghamton.edu"),
    subject = paste("✅ TSS Data Prep Script Success", Sys.Date()),
    credentials = creds_envvar(
      host = Sys.getenv("SMTP_SERVER"),   # ✅ This gets the actual hostname
      user = Sys.getenv("SMTP_USER"),
      pass_envvar = "SMTP_PASS",          # ✅ This stays quoted — it's the name of the env var
      port = 465,
      use_ssl = TRUE
    )
  )
  
  close(con)
  
}, error = function(e) {
  
  email <- compose_email(
    body = md(paste0(
      "❌ *TSS Cleaning script failed on ", Sys.Date(), "*\n\n",
      "**Error message:**\n\n",
      "```
", e$message, "\n```"
    )),
    footer = "— Automated TSS Cleaning Script"
  )
  
  smtp_send(
    email,
    from = "mjacob28@binghamton.edu",
    to = c("mjacob28@binghamton.edu"),
    subject = paste("❌ TSS Data Prep Script Failed:", Sys.Date()),
    credentials = creds_envvar(
      host = Sys.getenv("SMTP_SERVER"),   # ✅ This gets the actual hostname
      user = Sys.getenv("SMTP_USER"),
      pass_envvar = "SMTP_PASS",          # ✅ This stays quoted — it's the name of the env var
      port = 465,
      use_ssl = TRUE
    )
  )
  
})

