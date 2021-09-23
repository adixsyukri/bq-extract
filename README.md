# bq-extract
Bigquery Data Extractor on Python

Extract data from Bigquery into txt file and compress the folder into tar.gz

# Install
```bash
conda create bqenv
conda activate bqenv
conda install -c conda-forge google-cloud-bigquery google-cloud-bigquery-storage[pandas,pyarrow] pyyaml
```

# Run
```bash
python scripts/generator.py --path source.yaml --workdir $PWD
```
