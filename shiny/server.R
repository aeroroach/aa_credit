#
# This is the server logic of a Shiny web application. You can run the 
# application by clicking 'Run App' above.
#
# Find out more about building applications with Shiny here:
# 
#    http://shiny.rstudio.com/
#

shinyServer(function(input, output) {

# Data reactive manipulation ----------------------------------------------

  user_input <- reactive({
    dt %>% 
      filter(prof_date == input$prof_month) %>% 
      mutate(take_adj = cum_use_prop/max(cum_use_prop)*input$takeup/100,
             cum_fee = cum_no_subs*take_adj*cum_avg_trx*input$fee*(1-cum_bad_prop),
             cum_loss = cum_no_subs*cum_bad_prop*take_adj*input$loan,
             p_l = cum_fee - cum_loss)
  })

# Value Box ---------------------------------------------------------------

  top_pl <- reactive({
    user_input() %>% 
    filter(p_l == max(p_l))
  })
  
  output$decile <- renderValueBox({
    
    tmp <- top_pl()
    decile <- tmp$decile
    
    valueBox(
      decile, "Decile",color = "green", icon = icon("bookmark")
    )
  })

  output$subs <- renderValueBox({
    
    tmp <- top_pl()
    cum_sub <- tmp$cum_no_subs
    
    valueBox(
      paste(round(cum_sub/1000000, digits = 2), "M"), "#Subs",
      color = "green", icon = icon("users")
    )
  })

  output$risky <- renderValueBox({
    
    tmp <- top_pl()
    risky <- tmp$cum_bad_prop
    
    valueBox(
      paste0(round(risky*100, digits = 2),"%"), "Risky subs",
      color = "red", icon = icon("bug")
    )
  })

  output$profit <- renderValueBox({
    
    tmp <- top_pl()
    prof <- tmp$p_l
    
    valueBox(
      paste (round(prof/1000000, digits = 0), "M"), "P / L",
      color = "green", icon = icon("thumbs-up")
    )
  })

# Chart -------------------------------------------------------------------

  output$cum_usage <- renderPlot({
    
    fac_decile <- user_input()
    fac_decile <- factor(fac_decile$decile)
    
    user_input() %>%
      ggplot(aes(x = factor(decile), y = lift_rate)) + 
      geom_bar(stat = "identity", alpha = 0.8, fill = "seagreen4") +
      xlab("Decile") + ylab("lift rate") + 
      scale_x_discrete(limits = rev(levels(fac_decile))) +
      coord_flip() + 
      theme_minimal() + theme(text = element_text(size=20))
  })

  output$cum_risky <- renderPlot({
    
    fac_decile <- user_input()
    fac_decile <- factor(fac_decile$decile)
    
    user_input() %>%
      ggplot(aes(x = factor(decile), y = cum_bad_prop*100)) + 
      geom_bar(stat = "identity", alpha = 0.8, fill = "red4") +
      xlab("Decile") + ylab("Cumulative risky sub (%)") +
      scale_x_discrete(limits = rev(levels(fac_decile))) +
      coord_flip() +
      theme_minimal() + theme(text = element_text(size=20))
  })
  
  output$cum_profit <- renderPlot({
    
    fac_decile <- user_input()
    fac_decile <- factor(fac_decile$decile)
    
    user_input() %>%
      ggplot(aes(x = factor(decile), y = p_l/1000000)) + 
      geom_bar(stat = "identity", alpha = 0.8, fill = "seagreen4") +
      xlab("Decile") + ylab("Cumulative profit (M THB)") +
      scale_x_discrete(limits = rev(levels(fac_decile))) +
      coord_flip() +
      theme_minimal() + theme(text = element_text(size=20))
  })
  
# DT output ---------------------------------------------------------------

  output$lift_tbl <- DT::renderDataTable({
    user_input() %>% 
      select(decile, cum_no_subs, cum_use_prop, cum_avg_trx, bad_prop, cum_bad_prop, lift_rate, 
             take_adj, cum_fee, cum_loss, p_l) %>% 
      mutate(cum_use_prop = round(cum_use_prop*100, digits = 2),
             cum_avg_trx = round(cum_avg_trx, digits = 2),
             bad_prop = round(bad_prop*100, digits = 2), 
             cum_bad_prop = round(cum_bad_prop*100, digits = 2),
             lift_rate = round(lift_rate, digits = 0),
             take_adj = round(take_adj*100, digits = 0), 
             cum_fee = round(cum_fee, digits = 0), 
             cum_loss = round(cum_loss*-1, digits = 0),
             p_l = round(p_l, digits = 0)) %>% 
      DT::datatable(rownames = F, options = list(dom = "t"), 
                    colnames = c("Decile", "Cum #Subs", "Cum usage(%)", "Cum avg. usage", "Risky(%)", 
                                 "Cum Risky(%)", "Lift rate",
                                 "Tak up adj(%)", "Cum fee", "Cum loss", "P/L")) %>% 
      formatCurrency(c("cum_no_subs","cum_fee", "cum_loss", "p_l"), 
                     currency = "", interval = 3, mark = ",", before = FALSE, digits = 0) %>% 
      formatStyle("cum_fee", color = "green") %>% 
      formatStyle("cum_loss", color = "red") %>% 
      formatStyle("p_l", color = "green", fontWeight = "bold")
    })
})


# Test script -------------------------------------------------------------
