# DE_FINAL_PROJECT

DE_FINAL_PROJECT is an Airflow Image for scraping data from [/r/cryptocurrency](https://www.reddit.com/r/CryptoCurrency/) and performance of the top 100 coins on [Coingecko](https://www.coingecko.com/). The image contains a DAG which scrapes the data, and performs a word count on how many times any symbol or coin name has been mentioned in the post text, or title. 



## Installation

First, install [Docker](https://docs.docker.com/get-docker/) on your respective OS. Additional steps may be required for windows such as installing [WSL](https://docs.microsoft.com/en-us/windows/wsl/install). 

## Usage
Once the following have been installed. Here are the following steps to get [Airflow](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html) running

1.  cd to directory
```bash
cd path/to/directory
```
2. run this command to initialize database migrations and create the first user account.
```bash
docker-compose up airflow-init
```
3. Afterwards  all services can be started by running
```
docker compose up
```  
Unless configured otherwise the Airflow GUI can be accessed at: http://localhost:8080/home

![Airflow GUi](https://cdn.discordapp.com/attachments/824701298716573807/990222241675477042/unknown.png)

The DAG is called "Crypto_Scraper_Wordcount" and since the docker compose installs the dependencies, it's almost ready to run. However, some additional keys are necessary to run the DAG, and these can be accessed in the variables tab of airflow (under admin).  
Reddit Variables [(Guide here)](https://github.com/reddit-archive/reddit/wiki/OAuth2-Quick-Start-Example#first-steps)
```
client_id=Variable.get('client_id'),      
client_secret=Variable.get('client_secret'),      
user_agent=Variable.get('user_agent'),        
username=Variable.get('username'),        
password=Variable.get('password'))     
```  
GCP Variables:
```
aws_access_key_id=Variable.get("SERVICE_ACCESS_KEY"),
aws_secret_access_key=Variable.get("SERVICE_SECRET")
```
## Folder Overview
* Keys - Can be used to import variables.
* Dags - Used to store dags 
* Logs - Used to review dag runs and reasons for why a task may have failed, etc.
## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

