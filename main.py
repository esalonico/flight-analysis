from datetime import date

from flight_analysis.airport import Airport
from flight_analysis.flight import Flight
from flight_analysis.scraper import DirectOneWayScraper

from flight_analysis.sheets.download_sheets import download_all_sheets

if __name__ == "__main__":
    download_all_sheets()
    
    a1 = Airport("JFK")
    a2 = Airport("LAX")

    datetime_dep = date(2024, 3, 25) # 25th March 2024
    f = Flight(a1, a2, datetime_dep)

    print(a1.__repr__())
    print(a2.__repr__())
    print(f)
    
    s = DirectOneWayScraper(f)
    print(s)
    s.scrape()
    
    s.driver.quit()
    
