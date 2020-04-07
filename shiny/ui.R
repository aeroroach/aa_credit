#
# This is the user-interface definition of a Shiny web application. You can
# run the application by clicking 'Run App' above.
#
# Find out more about building applications with Shiny here:
# 
#    http://shiny.rstudio.com/
#

shinyUI(
  dashboardPage(skin = "green",
    
    dashboardHeader(title = "Advanced Airtime"),
    
    dashboardSidebar(
      selectInput(inputId = "prof_month", label = "Profile Month",
                  choices = as.list(sort(unique(dt$prof_date)))),
      sliderInput(inputId = "takeup", label = "Take up rate (%)", 
                  min = 0, max = 100, step = 5, value = 20),
      numericInput(inputId = "fee", label = "Fee (THB)", 
                   value = 2),
      numericInput(inputId = "loan", label = "Loan (THB)", 
                   value = 20)
      
    ),
    
    dashboardBody(
      
      # Optimum values outputs
      h3("Maximum Profit Values"),
      frowopti <- fluidRow(
        valueBoxOutput(outputId = "decile", width = 3), 
        valueBoxOutput(outputId = "subs", width = 3),
        valueBoxOutput(outputId = "risky", width = 3),
        valueBoxOutput(outputId = "profit", width = 3)
      ),
      
      navbarPage(title = "Visualisation type", id ="tabset",
        tabPanel("Chart", 
                 fluidRow(
                   box(
                         title = "Model lift rate", width = 4,
                         status = "success",
                         solidHeader = T,
                         plotOutput("cum_usage", height = 300)
                       ),
                       box(
                         title = "Cumulative Risky sub rate (%)", width = 4,
                         status = "danger",
                         solidHeader = T,
                         plotOutput("cum_risky", height = 300)
                       ),
                       box(
                         title = "Cumulative profit (THB)", width = 4,
                         status = "success",
                         solidHeader = T,
                         plotOutput("cum_profit", height = 300)
                       )
                 )
        ),
        tabPanel("Table",
                 DT::dataTableOutput("lift_tbl")
        )
      
      )
    )
  )
)