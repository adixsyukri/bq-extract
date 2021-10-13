# bq-extract
Bigquery Data Extractor on Python

Extract data from Bigquery into txt or parquet file and compress the folder into tar.gz

# Install
```bash
conda create bqenv
conda activate bqenv
conda install -c conda-forge google-cloud-bigquery google-cloud-bigquery-storage[pandas,pyarrow] pyyaml
pip install 'apache-beam[gcp]'
conda install -c conda-forge google-cloud-storage
```

# Run
```bash
python scripts/generator.py --path source.yaml --table table_name --rundate 2021-10-13T12:26:00.000000
python scripts/compressor.py --path source.yaml --rundate 2021-10-13T12:26:00.000000
```
