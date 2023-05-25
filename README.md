# openiimis-lightning_dkr
Dockerized instance of Lightning with demo workflow for openIMIS Individual ETL

## OpenFN/Lightning Dockerized setup for openIMIS development 
This repository is using dockerized instance of [Lightning](https://github.com/OpenFn/Lightning)
and providing initial setup for the openIMIS implementation. 

It creates new example project that: 
- Uses psql adaptor 
- Has provided openIMIS database credentials 
- Implement's workflow that expects certain input and do simple input of data from IndividualDataSource to Individual table


### Setup 

1. Copy .env.exapmle to .env 
2. Adjust .env configuration according to your imis setup 
3. Run imis migrations `docker compose run --rm web ./imisSetup.sh`
4. 
