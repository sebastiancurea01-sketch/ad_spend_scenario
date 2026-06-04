```dax
Revenue YoY% = 
VAR _Current = [Total Revenue]
VAR _Previous = CALCULATE([Total Revenue], SAMEPERIODLASTYEAR('calendar'[Date]))

RETURN
DIVIDE(_Current - _Previous, _Previous, 0)
