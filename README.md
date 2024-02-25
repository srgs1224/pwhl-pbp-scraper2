# 🏒 PWHL Play-by-Play Data Scraper
### Overview
This is a Python program that scrapes play-by-play data from the PWHL API and transforms it into a well-structured Pandas Data Frame so analysis can be done on the data. At the time of this scrapers creation, the PWHL was in it's inaugural season, and with lots of buzz around the league, I wanted to make sure there was a Python too available for scraping the play-by-play data so anyone can do analysis on the league. 

### IMPORTANT
Since the PWHL is a new league, the data (from what I've seen so far) can change quickly during the season. This means that the scraper will have to change as new data becomes available in the API. I will be closely keeping an eye on any data changes, but if you notice any, please reach out so I can make any necessary adjustments to the scraper! Because of this, I will not be uploading this to pip just yet. For now, a simple clone of this repo will be the way to use it. If I notice the data stabilize after a long period of time, I will certainly upload it to pip for the easiest use.

### Installation
In your terminal of choice, run the following commands:
```
git clone https://github.com/ztandrews/pwhl-pbp-scraper.git
```

### Quick Start
To run the program, you have a few options. What I would suggest is creating a new .py file in the root directory of this project, and adding in something like this:
```
from pwhl_pbp_scraper import scrape_game
game_id = 5
game = scrape_game(game_id)
print(game.head())
```

### Contributing
Contributions to this scraper are welcome! If you have suggestions for improvements or new features, feel free to fork the repository, make your changes, and submit a pull request.

### Support and Feedback
If you encounter any issues or have suggestions, please open an issue on GitHub or contact the author via Twitter: @StatsByZach.
