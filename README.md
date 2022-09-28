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
```bash
python3 readingFiles.py
```

Remember to prune volumes regularly
```bash
docker volume prune
```