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

Remember to prune volumes regularly
```bash
docker volume prune
```
