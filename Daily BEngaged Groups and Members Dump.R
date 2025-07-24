library(httr)
library(xml2)
library(tidyverse)
library(writexl)
library(blastula)
library(kableExtra)

tryCatch({
  
  # --- CONFIG ---
  token <- Sys.getenv("cg_token")
  timestamp <- format(Sys.Date(), "%Y-%m-%d")
  
  out_path_groups <- paste0("Z:/Shared SAASI/Data Hub Development/B-Engaged Migration/B-Engaged Dumps/Daily Groups Dump/Groups_dump_", timestamp, ".csv")
  out_path_members_full <- paste0("Z:/Shared SAASI/Data Hub Development/B-Engaged Migration/B-Engaged Dumps/Daily Members Dump/Members_dump", timestamp, ".csv")
  
  # --- GET GROUPS ---
  group_url <- "https://bengaged.binghamton.edu/rss_groups?include_deleted=1"
  group_response <- GET(url = group_url, add_headers("X-CG-API-Secret" = token))
  stop_for_status(group_response)
  
  group_items <- xml_find_all(read_xml(group_response), "//item")
  
  groups_df <- map(group_items, function(item) {
    node <- map(xml_children(item), xml_text)
    names(node) <- map_chr(xml_children(item), xml_name)
    node
  }) %>% bind_rows() %>% as_tibble()
  
  # --- GET MEMBERS BY GROUP ---
  group_ids <- groups_df$groupId
  members_list <- list()
  
  for (groupid in group_ids) {
    member_url <- paste0("https://bengaged.binghamton.edu/rss_members?group_ids=", groupid)
    response <- GET(member_url, add_headers("X-CG-API-Secret" = token))
    if (status_code(response) != 200) next
    
    items <- xml_find_all(read_xml(response), "//item")
    data <- map(items, function(item) {
      entry <- map(xml_children(item), xml_text)
      names(entry) <- map_chr(xml_children(item), xml_name)
      entry$group_id <- groupid
      entry
    })
    members_list <- append(members_list, data)
  }
  
  members_df <- bind_rows(members_list) %>% as_tibble()
  
  # --- JOIN GROUP TYPE ---
  group_type_df <- groups_df %>% select(groupId, groupType)
  members_joined <- members_df %>%
    left_join(group_type_df, by = c("group_id" = "groupId")) %>%
    relocate(groupName, groupType)
  
  # --- EXPORT FILES ---
  write_csv(groups_df, out_path_groups)
  write_csv(members_joined, out_path_members_full)
  
  # --- SUMMARY TABLE FOR EMAIL ---
  summary_table <- members_joined %>%
    count(groupType, name = "Members") %>%
    arrange(desc(Members))
  
  summary_html <- summary_table %>%
    rename(`Group Type` = groupType) %>%
    kable("html", escape = FALSE, align = "lr", caption = "Member Counts by Group Type") %>%
    kableExtra::kable_styling("striped", full_width = FALSE)
  
  # --- SUCCESS EMAIL ---
  email <- compose_email(
    body = html(paste0(
      "<p>✅ Group/member export completed successfully on ", Sys.Date(), ".</p>",
      "<ul>",
      "<li><strong>Groups:</strong> ", nrow(groups_df), "</li>",
      "<li><strong>Members:</strong> ", nrow(members_df), "</li>",
      "</ul>",
      "<p>Files written to:</p><ul>",
      "<li>", out_path_groups, "</li>",
      "<li>", out_path_members, "</li>",
      "<li>", out_path_members_full, "</li>",
      "</ul>",
      summary_html
    )),
    footer = "— Automated Group Member Export"
  )
  
  smtp_send(
    email,
    from = "mjacob28@binghamton.edu",
    to = c("mjacob28@binghamton.edu"),
    subject = paste("✅ Group Member Export Success:", Sys.Date()),
    credentials = creds_file("Z:/Shared SAASI/Banner Info/Periodic Data Exports/PDE - R Scripts/gmail_creds")
  )
  
}, error = function(e) {
  
  # --- FAILURE EMAIL ---
  error_email <- compose_email(
    body = md(paste0(
      "❌ *Group/member export failed on ", Sys.Date(), "*\n\n",
      "**Error message:**\n\n```\n", e$message, "\n```"
    )),
    footer = "— Automated Group Member Export"
  )
  
  smtp_send(
    error_email,
    from = "mjacob28@binghamton.edu",
    to = c("mjacob28@binghamton.edu"),
    subject = paste("❌ Group Member Export Failed:", Sys.Date()),
    credentials = creds_file("Z:/Shared SAASI/Banner Info/Periodic Data Exports/PDE - R Scripts/gmail_creds")
  )
})
