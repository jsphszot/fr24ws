# fr24ws
Older version of FlightRadar24-to-BigQuery repository. Rudimentary proof of concept and less interesting.

-----------------------------------------------

flightradar24 web scrapers using python

<h6>pastflights-scraper-fr24.py</h6>
Input is a list of Airline codes which are used to scrape all registration numbers per airline on the page, which are then used to scrape flight info for each airplane. 
Outputs are: 
  <ol>
  <li>a data frame with arrival/departure time and airport, as well as flight status.</li>
  <li>former data frame but with airplane routing from and to MIA airport on single row.</li> 
  </ol>

<h6>postedflights-scraper-fr24.py</h6>
Input is a list of Airlines and airports, output is a data frame with planned departure/arrival times for different routes by plane and flight number. 
Process includes slack messaging for process tracking, as well as loading df to GBQ.
