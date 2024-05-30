corn values


PH_PL_chartink.py
PH_PL_ values
schedule:
- cron: 31 10-16 * * mon-fri  #1hr
- cron: 0 16 * * mon-fri      # 1d
- cron: 0 16 * * 3,5          # 1wk
- cron: 0 16 15,30 * *        # 1 mo 
- cron: 0 16 1 * *            # 3 mo


chartink_screener.py, pdf_report_generator.py
breakout_stocks_to_watch
schedule:
- cron: 31 10-16 * * mon-fri  #1hr
- cron: 0 16 * * mon-fri      # 1d
- cron: 0 16 * * 3,5          # 1wk
- cron: 0 16 15,30 * *.       # 1 mo 
- cron: 0 16 1 * *            # 3 mo


python two_line_pattern_detect_class.py "60m"
hour
  schedule:
    - cron: 0 19 * * 2,5   #hour


python two_line_pattern_detect_class.py "1d"
day
schedule:
- cron: 0 19 * * 5  #day


python two_line_pattern_detect_class.py "1wk"
week
schedule:
- cron: 0 0 * * SAT  #week


python two_line_pattern_detect_class.py "1mo"
month
schedule:
- cron: 0 2 1 * *  # month


python two_line_pattern_detect_class.py "3mo"
quarter
schedule:
- cron: 0 4 1 JAN,APR,JUL,OCT *