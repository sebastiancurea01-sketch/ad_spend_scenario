```dax
Brand Revenue % = 
DIVIDE(
    CALCULATE([Total Revenue], website_sessions[utm_campaign] = "brand"),
    [Total Revenue],
    0
)
