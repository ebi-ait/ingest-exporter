[![Docker Repository on Quay](https://quay.io/repository/ebi-ait/ingest-exporter/status "Docker Repository on Quay")](https://quay.io/repository/ebi-ait/ingest-exporter)

# ingest-exporter

Component that handles the exporting of HCA metadata and data files to DCP
 
This component listens for messages from the Ingest Core using RabbitMQ. When a submission is valid and complete (i.e. all data files have been uploaded to the upload area), Ingest Core will notify this component and this will transfer the files to Terra. 

```
pip install -r requirements.txt
```

```
python exporter.py
```

# testing
```
pip install -r requirements-dev.txt
```

```
nosetests
```