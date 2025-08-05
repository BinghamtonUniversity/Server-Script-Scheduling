library(httr)
library(xml2)
library(tidyverse)
library(lubridate)
library(blastula)
library(kableExtra)
library(dplyr)
library(writexl)
library(knitr)
library(gt)

readRenviron("//bushare.binghamton.edu/assess/Shiny Apps/.Renviron.R")

tryCatch({
  readRenviron("//bushare.binghamton.edu/assess/Shiny Apps/.Renviron.R")
  # --- Config ---
  today <- format(Sys.Date(), "%Y-%m-%d")
  events_output_path <- paste0("//bushare.binghamton.edu/assess/Shared SAASI/Data Hub Development/B-Engaged Migration/B-Engaged Dumps/Daily Groups Dump/BEngaged Groups Pull_", today, ".csv")
  attendance_output_path <- paste0("//bushare.binghamton.edu/assess/Shared SAASI/Data Hub Development/B-Engaged Migration/B-Engaged Dumps/Daily Attendees Dump/BEngaged Attendance Pull_", today, ".csv")
  token <- Sys.getenv("cg_token")  # Must be stored securely in .Renviron
  event_start_cutoff <- "2025-07-01"
  event_end_cutoff <- "2024-12-15"
  
  # --- Step 1: Pull Events After July 1 ---
  url <- paste0(
    "https://bengaged.binghamton.edu/rss_events",
    "?time_range=past",  # <-- Required to get past events
    "&event_ends_after=", event_start_cutoff
  )
  
  response <- GET(url = url, add_headers("X-CG-API-Secret" = token))
  if (status_code(response) != 200) stop("Event pull failed: ", status_code(response))
  
  items <- xml_find_all(read_xml(response), "//item")
  
  event_data_list <- map(items, function(item) {
    data <- map(xml_children(item), xml_text)
    names(data) <- map_chr(xml_children(item), xml_name)
    data
  })
  
  events_df <- bind_rows(event_data_list) %>%
    as_tibble() %>%
    mutate(eventDate = mdy(eventDate))
  
  # --- Step 2: Pull Attendees for Each Event ---
  event_ids <- events_df$eventId
  attendees_list <- list()
  
  for (event_id in event_ids) {
    attendee_url <- paste0(
      "https://bengaged.binghamton.edu/rss_event_attendees?include_deactivated_users=1&event_id=",
      event_id
    )
    response <- GET(url = attendee_url, add_headers("X-CG-API-Secret" = token))
    if (status_code(response) != 200) next
    
    items <- xml_find_all(read_xml(response), "//item")
    data <- map(items, function(item) {
      entry <- map(xml_children(item), xml_text)
      names(entry) <- map_chr(xml_children(item), xml_name)
      entry$event_id <- event_id
      entry
    })
    attendees_list <- append(attendees_list, data)
  }
  
  attendees_df <- bind_rows(attendees_list) %>% as_tibble()
  
  # --- Step 3: Merge Attendee + Event Info ---
  final_df <- attendees_df %>%
    left_join(events_df %>% select(eventId, title, eventDate, fullDescription, eventLink, groupId, group, coHostedGroupIds), 
              by = c("event_id" = "eventId")) %>%
    relocate(title, eventDate, fullDescription, eventLink)
  
  # --- Step 4: Write CSV Output ---
  write_csv(final_df, attendance_output_path)
  write_csv(events_df, events_output_path)
  
  # --- Step 5: Build Summary Table for Email ---
  summary_table <- final_df %>%
    filter(checkedIn == "1") %>%  # Filter only checked-in attendees
    distinct(netId, eventId, .keep_all = TRUE) %>%  # Deduplicate by user + event
    count(title, name = "Attendee_Count") %>%
    arrange(desc(Attendee_Count))
  
  summary_html <- summary_table %>%
    rename(`Event` = title) %>%
    kable("html", escape = FALSE, align = "lr", caption = "Event Attendance Summary") %>%
    kable_styling("striped", full_width = FALSE)
  
  
  # --- Step 6: Success Email ---
  email <- compose_email(
    body = html(paste0(
      "<p>✅ B-Engaged attendee export completed successfully on ", Sys.Date(), ".</p>",
      "<p><strong>Output file:</strong><br>", attendance_output_path, "</p>",
      summary_html
    )),
    footer = "— Automated B-Engaged Script"
  )
  
  smtp_send(
    email,
    from = "mjacob28@binghamton.edu",
    to = c("mjacob28@binghamton.edu","ewalsh@binghamton.edu"),
    subject = paste("✅ B-Engaged Export Success:", Sys.Date()),
    credentials = creds_envvar(
      host = Sys.getenv("SMTP_SERVER"),   # ✅ This gets the actual hostname
      user = Sys.getenv("SMTP_USER"),
      pass_envvar = "SMTP_PASS",          # ✅ This stays quoted — it's the name of the env var
      port = 465,
      use_ssl = TRUE
    )
  )
  
}, error = function(e) {
  
  # --- Failure Email ---
  error_email <- compose_email(
    body = md(paste0(
      "❌ *B-Engaged attendee export failed on ", Sys.Date(), "*.\n\n",
      "**Expected output file:**  \n",
      attendance_output_path, "\n\n",
      "**Error message:**\n\n",
      "```\n", e$message, "\n```"
    )),
    footer = "— Automated B-Engaged Script"
  )
  
  smtp_send(
    error_email,
    from = "mjacob28@binghamton.edu",
    to = c("mjacob28@binghamton.edu","ewalsh@binghamton.edu"),
    subject = paste("❌ B-Engaged Export Failed:", Sys.Date()),
    credentials = creds_envvar(
      host = Sys.getenv("SMTP_SERVER"),   # ✅ This gets the actual hostname
      user = Sys.getenv("SMTP_USER"),
      pass_envvar = "SMTP_PASS",          # ✅ This stays quoted — it's the name of the env var
      port = 465,
      use_ssl = TRUE
    )
  )
})
