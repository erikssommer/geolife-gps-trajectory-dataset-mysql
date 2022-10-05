# geolife-gps-trajectory-dataset-mysql
TDT4225 - Very Large, Distributed Data Volumes

Setup
```bash
pip3 install -r requirements.txt
```

Run
```bash
cd docker
docker-compose up --build -V
```

In seperate shell

Init database with data and run queries
```bash
python3 readingFiles.py -i
```

Run only queries, requires database to be initialized
```bash
python3 readingFiles.py
```

If running docker-compose build command multiple times remember to prune volumes
```bash
docker volume prune
```