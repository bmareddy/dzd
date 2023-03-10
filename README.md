# Phenotype Data Processing
This project contains the following components

## Exploration Notebook
The notebook named `0_exploration_notebook.ipynb` is a Jupyter notebook that inspects the source data and seeks to validate, understand the data in those files

## Data Model
The markdown file `datamodel.md` layout the proposed data model for structuring the input data

## ETL
The file `etl.py` is the code that processes the data. It utilizes the `extract.py` and `transforms.py` modules. It expects that source data is located in `source_data\` folder and dumps the output back to `output_data\` folder