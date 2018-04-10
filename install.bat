docker build -t name_scraper .
docker run -v %cd%:/usr/local/bin --name name_scraper name_scraper
pause
